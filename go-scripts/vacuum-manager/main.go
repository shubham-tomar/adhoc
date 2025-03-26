package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// Database configuration
const (
	defaultHost     = "XXXXXXXX"
	defaultPort     = 5432
	defaultUser     = "postgres"
	defaultDBName   = "XXXXX"
	
	// Environment variable names
	envDBPassword   = "DB_PASSWORD"
	
	// Parallel processing configuration
	defaultWorkers  = 5
	
	// Fallback password if environment variable is not set
	// Only used if DB_PASSWORD environment variable is not set
	fallbackPassword = "XXXXXXXX"
)

// Configuration options
type Config struct {
	Host        string
	Port        int
	User        string
	Password    string
	DBName      string
	ParentTable string
	VacuumFlag  bool
	Schema      string
	Timeout     time.Duration
	Verbose     bool
	Workers     int
}

func main() {
	// Parse command line arguments
	config := parseFlags()

	// Connect to the database
	log.Printf("Connecting to database: %s@%s:%d/%s", 
		config.User, config.Host, config.Port, config.DBName)
	
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=require connect_timeout=10",
		config.Host, config.Port, config.User, config.Password, config.DBName,
	)
	
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Printf("Failed to connect to database: %v", err)
		return
	}
	defer db.Close()
	
	// Configure connection pool
	db.SetMaxOpenConns(config.Workers * 2)
	db.SetMaxIdleConns(config.Workers)
	db.SetConnMaxLifetime(config.Timeout)
	
	// Test the connection
	log.Println("Testing database connection...")
	if err := db.Ping(); err != nil {
		log.Printf("Database connection failed: %v", err)
		return
	}
	log.Println("Database connection successful!")

	// Set up context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	// Get all partitions for the table
	partitions, err := getPartitions(ctx, db, config.Schema, config.ParentTable)
	if err != nil {
		log.Printf("Failed to get partitions: %v", err)
		return
	}
	
	log.Printf("Found %d partitions for %s.%s", len(partitions), config.Schema, config.ParentTable)
	
	if config.Verbose {
		for i, partition := range partitions {
			log.Printf("%d. %s (parent: %s)", i+1, partition.Name, partition.Parent)
		}
	}

	// Set vacuum flag for all partitions in parallel
	results := setVacuumFlagParallel(ctx, db, partitions, config.VacuumFlag, config.Workers)
	
	// Count successes and failures
	successCount, failCount := 0, 0
	for _, success := range results {
		if success {
			successCount++
		} else {
			failCount++
		}
	}

	action := "enabled"
	if !config.VacuumFlag {
		action = "disabled"
	}
	log.Printf("Operation completed: %s vacuum for %d/%d partitions (succeeded/failed: %d/%d)", 
		action, successCount, len(partitions), successCount, failCount)
}

// Partition represents a database partition
type Partition struct {
	Name   string
	Parent string
}

// parseFlags parses command-line flags and returns a Config
func parseFlags() Config {
	host := flag.String("host", defaultHost, "database host")
	port := flag.Int("port", defaultPort, "database port")
	user := flag.String("user", defaultUser, "database user")
	// Get password from environment variable first, fallback to command line arg
	envPassword := os.Getenv(envDBPassword)
	password := flag.String("password", "", "database password (overrides environment variable)")
	dbname := flag.String("dbname", defaultDBName, "database name")
	parentTable := flag.String("parent", "asset_combination", "parent table name")
	schema := flag.String("schema", "check2", "schema name")
	vacuumFlag := flag.Bool("enable", true, "enable vacuum if true, disable if false")
	timeout := flag.Duration("timeout", 5*time.Minute, "timeout for operations")
	verbose := flag.Bool("verbose", false, "enable verbose output")
	workers := flag.Int("workers", defaultWorkers, "number of parallel workers")

	flag.Parse()

	// Determine which password to use in order of precedence:
	// 1. Command line argument (if provided)
	// 2. Environment variable
	// 3. Fallback hardcoded password
	finalPassword := fallbackPassword
	if envPassword != "" {
		finalPassword = envPassword
	}
	if *password != "" {
		finalPassword = *password
	}
	
	return Config{
		Host:        *host,
		Port:        *port,
		User:        *user,
		Password:    finalPassword,
		DBName:      *dbname,
		ParentTable: *parentTable,
		VacuumFlag:  *vacuumFlag,
		Schema:      *schema,
		Timeout:     *timeout,
		Verbose:     *verbose,
		Workers:     *workers,
	}
}

// getPartitions retrieves all partitions for a given parent table
func getPartitions(ctx context.Context, db *sql.DB, schema, parentTable string) ([]Partition, error) {
	fullTableName := fmt.Sprintf("%s.%s", schema, parentTable)
	
	query := `
WITH RECURSIVE all_partitions AS (
  -- Get direct child partitions
  SELECT
    inhrelid::regclass AS partition_name,
    inhparent::regclass AS parent_name
  FROM pg_inherits
  WHERE inhparent = $1::regclass

  UNION ALL

  -- Recursively find sub-partitions
  SELECT
    p.inhrelid::regclass,
    p.inhparent::regclass
  FROM pg_inherits p
  JOIN all_partitions ap ON p.inhparent = ap.partition_name
)
SELECT partition_name::text, parent_name::text FROM all_partitions;
`

	rows, err := db.QueryContext(ctx, query, fullTableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query partitions: %w", err)
	}
	defer rows.Close()

	var partitions []Partition
	for rows.Next() {
		var p Partition
		if err := rows.Scan(&p.Name, &p.Parent); err != nil {
			return nil, fmt.Errorf("failed to scan partition: %w", err)
		}
		partitions = append(partitions, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over partitions: %w", err)
	}

	return partitions, nil
}

// setVacuumFlagParallel enables or disables vacuum for all partitions in parallel
func setVacuumFlagParallel(ctx context.Context, db *sql.DB, partitions []Partition, enableVacuum bool, numWorkers int) []bool {
	// Generate SQL for setting the reloptions
	var reloption string
	if enableVacuum {
		reloption = "RESET (autovacuum_enabled)"
	} else {
		reloption = "SET (autovacuum_enabled = false)"
	}

	log.Printf("Processing %d partitions with %d workers", len(partitions), numWorkers)
	
	// Create channels for work distribution
	jobs := make(chan Partition, len(partitions))
	results := make(chan struct{ index int; success bool }, len(partitions))
	
	// Create a wait group to track worker completion
	var wg sync.WaitGroup
	
	// Launch workers
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for partition := range jobs {
				// Build the ALTER TABLE statement
				sql := fmt.Sprintf("ALTER TABLE %s %s;", partition.Name, reloption)
				log.Printf("[Worker %d] Processing partition: %s", workerID, partition.Name)
				
				// Use a separate context for each query
				queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				
				// Execute the statement
				_, err := db.ExecContext(queryCtx, sql)
				cancel() // Always cancel context to prevent leaks
				
				// Generate index from partition name for results tracking
				index := 0
				for i, p := range partitions {
					if p.Name == partition.Name {
						index = i
						break
					}
				}
				
				if err != nil {
					log.Printf("[Worker %d] Failed to set vacuum flag for %s: %v", workerID, partition.Name, err)
					results <- struct{ index int; success bool }{index, false}
				} else {
					results <- struct{ index int; success bool }{index, true}
				}
			}
		}(w)
	}
	
	// Send all partitions to the workers
	go func() {
		for _, partition := range partitions {
			jobs <- partition
		}
		close(jobs)
	}()
	
	// Close results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()
	
	// Collect results
	successList := make([]bool, len(partitions))
	for result := range results {
		successList[result.index] = result.success
	}
	
	return successList
}

// setVacuumFlag enables or disables vacuum for a single partition
func setVacuumFlag(ctx context.Context, db *sql.DB, partition Partition, enableVacuum bool) error {
	// Generate SQL for setting the reloptions
	var reloption string
	if enableVacuum {
		reloption = "RESET (autovacuum_enabled)"
	} else {
		reloption = "SET (autovacuum_enabled = false)"
	}
	
	// Build and execute the ALTER TABLE statement
	sql := fmt.Sprintf("ALTER TABLE %s %s;", partition.Name, reloption)
	
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to set vacuum flag for %s: %w", partition.Name, err)
	}
	
	return nil
}
