package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// Configuration for the script
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SQLFile  string
	Workers  int
}

// Result of executing a query
type QueryResult struct {
	Query   string
	Success bool
	Error   error
	Time    time.Duration
}

func main() {
	// Parse command line arguments
	// config := parseFlags()
	
	// Read and parse SQL file
	// queries, err := parseQueriesFromFile("/Users/shubhamtomar/Downloads/migration/delete_partition_queries.sql")
	queries, err := parseQueriesFromFile("/Users/shubhamtomar/Downloads/migration/metric_raw_value_partition_queries.sql")
	if err != nil {
		log.Fatalf("Error parsing SQL file: %v", err)
	}
	
	fmt.Printf("Found %d queries to execute\n", len(queries))
	
	// Create connection string
	// connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
	// 	config.Host, config.Port, config.User, config.Password, config.DBName)
	
	// Update connection string with more options
connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require connect_timeout=10",
		"XXXXXXXX", 5432, "postgres", "XXXXXXXX", "XXXXX")
	
	// Create a database connection pool
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer db.Close()
	
	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	fmt.Println("Successfully connected to database")
	
	// Create wait group and channels for results
	var wg sync.WaitGroup
	resultsChan := make(chan QueryResult, len(queries))
	
	// Start worker pool
	queryQueue := make(chan string, len(queries))
	
	// Start the workers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go worker(db, queryQueue, resultsChan, &wg, i)
	}
	
	// Queue the queries
	for _, query := range queries {
		queryQueue <- query
	}
	close(queryQueue)
	
	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultsChan)
	}()
	
	// Process results
	processResults(resultsChan)
}

func parseFlags() Config {
	host := flag.String("host", "localhost", "Database host")
	port := flag.Int("port", 5432, "Database port")
	user := flag.String("user", "postgres", "Database user")
	password := flag.String("password", "", "Database password")
	dbname := flag.String("dbname", "postgres", "Database name")
	sqlFile := flag.String("file", "partition_queries.sql", "SQL file with queries")
	workers := flag.Int("workers", 10, "Number of concurrent workers")
	
	flag.Parse()
	
	return Config{
		Host:     *host,
		Port:     *port,
		User:     *user,
		Password: *password,
		DBName:   *dbname,
		SQLFile:  *sqlFile,
		Workers:  *workers,
	}
}

func parseQueriesFromFile(filename string) ([]string, error) {
	// Read file content
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	
	// Convert to string
	sqlContent := string(content)
	
	// Use regex to find all CREATE TABLE statements
	re := regexp.MustCompile(`(?is)(CREATE\s+TABLE\s+[^;]+;)`)
	// re := regexp.MustCompile(`(?is)(DROP\s+TABLE\s+[^;]+;)`)
	matches := re.FindAllString(sqlContent, -1)
	
	// Filter out empty matches
	var queries []string
	for _, match := range matches {
		query := strings.TrimSpace(match)
		if query != "" {
			queries = append(queries, query)
		}
	}
	
	fmt.Printf("Found %d queries using regex approach\n", len(queries))
	
	// If no queries found, try splitting by semicolon as fallback
	if len(queries) == 0 {
		parts := strings.Split(sqlContent, ";")
		for _, part := range parts {
			query := strings.TrimSpace(part)
			if query != "" && strings.Contains(strings.ToUpper(query), "CREATE TABLE") {
				queries = append(queries, query+";")
			}
		}
		fmt.Printf("Found %d queries using fallback split approach\n", len(queries))
	}
	
	return queries, nil
}

func worker(db *sql.DB, queryQueue <-chan string, resultsChan chan<- QueryResult, wg *sync.WaitGroup, workerID int) {
	defer wg.Done()
	
	for query := range queryQueue {
		// Use a self-executing function to properly scope the defer
		func(q string) {
			// Execute the query
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel() // Now this will execute when the anonymous function ends
			
			start := time.Now()
			_, err := db.ExecContext(ctx, q)
			duration := time.Since(start)
			
			// Format short query for logging (first line or first 50 chars)
			shortQuery := q
			if idx := strings.Index(q, "\n"); idx > 0 {
				shortQuery = q[:idx] + "..."
			} else if len(q) > 50 {
				shortQuery = q[:47] + "..."
			}
			
			// Send result
			result := QueryResult{
				Query:   shortQuery,
				Success: err == nil,
				Error:   err,
				Time:    duration,
			}
			
			resultsChan <- result
			
			// Log progress
			if err != nil {
				fmt.Printf("[Worker %d] FAILED: %s (%v)\n", workerID, shortQuery, err)
			} else {
				fmt.Printf("[Worker %d] SUCCESS: %s (%v)\n", workerID, shortQuery, duration)
			}
		}(query) // Pass the query to the anonymous function
	}
}

func processResults(resultsChan <-chan QueryResult) {
	// Count successes and failures
	var success, failure int
	
	// Record start time for total execution
	startTime := time.Now()
	
	// Track all results
	var results []QueryResult
	
	// Process results as they come in
	for result := range resultsChan {
		results = append(results, result)
		if result.Success {
			success++
		} else {
			failure++
		}
	}
	
	// Calculate total execution time
	totalTime := time.Since(startTime)
	
	// Print summary
	fmt.Println("\n--- Execution Summary ---")
	fmt.Printf("Total queries: %d\n", len(results))
	fmt.Printf("Successful: %d\n", success)
	fmt.Printf("Failed: %d\n", failure)
	fmt.Printf("Total execution time: %v\n", totalTime)
	
	// If there are failures, write them to a file
	if failure > 0 {
		// Create failed queries file
		file, err := os.Create("failed_queries.sql")
		if err != nil {
			fmt.Printf("Error creating failed queries file: %v\n", err)
			return
		}
		defer file.Close()
		
		// Write each failed query
		for _, result := range results {
			if !result.Success {
				file.WriteString(result.Query)
				file.WriteString("\n-- Error: ")
				file.WriteString(result.Error.Error())
				file.WriteString("\n\n")
			}
		}
		
		fmt.Println("Failed queries have been written to failed_queries.sql")
	}
}
