package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// Database configuration
const (
	host     = "dataplatform-dev-rds.c7kssuaisap5.us-east-1.rds.amazonaws.com"
	port     = 5432
	user     = "postgres"
	dbname   = "optimization_dev_pse"
	
	// Environment variable names
	envDBPassword = "DB_PASSWORD"
	
	// Worker configuration
	numWorkers = 5         // Number of parallel workers
	queueSize  = 100       // Size of the work queue
	
	// Fallback password if environment variable is not set
	fallbackPassword = "XXXXXXXX"  // Only used if DB_PASSWORD environment variable is not set
)

func main() {
	// Get password from environment variable or use fallback
	password := fallbackPassword
	envPassword := os.Getenv(envDBPassword)
	if envPassword != "" {
		password = envPassword
	}
	
	// Print startup information
	log.Println("Starting batch insert operation")
	log.Printf("Using database: %s@%s:%d/%s", user, host, port, dbname)
	log.Printf("Worker configuration: %d parallel workers, %d max queue size", 
		numWorkers, queueSize)
	
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require connect_timeout=10",
		host, port, user, password, dbname)

	// Connect to database
	log.Println("Establishing database connection...")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()
	
	// Configure the main connection - we'll create separate connections for each worker
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)

	// Generate date ranges (weekly batches)
	dateRanges := generateDateRanges("2024-01-01", "2024-02-01", "1 week")

	// Generate INSERT statements
	insertStatements := generateInsertStatements(dateRanges)

	// Execute the statements
	executeInsertStatements(db, insertStatements, password)
}

// DateRange represents a start and end date
type DateRange struct {
	StartDate string
	EndDate   string
}

// generateDateRanges creates date ranges with the specified interval
func generateDateRanges(startDate, endDate, interval string) []DateRange {
	var ranges []DateRange

	startTime, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		log.Fatalf("Error parsing start date: %v", err)
	}

	endTime, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		log.Fatalf("Error parsing end date: %v", err)
	}

	currentTime := startTime

	for currentTime.Before(endTime) {
		// Calculate next date based on interval
		var nextTime time.Time

		// Calculate next date based on interval type
		switch interval {
		case "1 month":
			year, month, _ := currentTime.Date()
			nextMonth := month + 1
			nextYear := year

			if nextMonth > 12 {
				nextMonth = 1
				nextYear++
			}

			nextTime = time.Date(nextYear, nextMonth, 1, 0, 0, 0, 0, currentTime.Location())
		case "1 week":
			nextTime = currentTime.AddDate(0, 0, 7)
		default:
			log.Fatalf("Unsupported interval: %s", interval)
		}
		
		// Make sure we don't exceed the specified end date
		if nextTime.After(endTime) {
			nextTime = endTime
		}

		ranges = append(ranges, DateRange{
			StartDate: currentTime.Format("2006-01-02"),
			EndDate:   nextTime.Format("2006-01-02"),
		})

		// Stop if we've reached the end
		if nextTime.Equal(endTime) {
			break
		}

		currentTime = nextTime
	}

	return ranges
}

// generateInsertStatements creates INSERT statements for each date range
func generateInsertStatements(dateRanges []DateRange) []string {
	var statements []string

	for _, dr := range dateRanges {
		statement := fmt.Sprintf(`
INSERT INTO check2.metric_raw_value
SELECT * FROM raw_input.metric_raw_value
WHERE date >= '%s' AND date < '%s' ON CONFLICT DO NOTHING;`, dr.StartDate, dr.EndDate)

		statements = append(statements, statement)
	}

	return statements
}

// Result represents the result of a SQL execution
type Result struct {
	Index      int
	Statement  string
	Error      error
	DateRange  string
	ExecutionTime time.Duration
	WorkerID   int
}

// executeInsertStatements runs all INSERT statements in parallel with progress tracking
func executeInsertStatements(db *sql.DB, statements []string, dbPassword string) {
	totalStatements := len(statements)
	log.Printf("Executing %d INSERT statements in parallel with %d workers (each with dedicated DB connection)\n", 
		totalStatements, numWorkers)
	
	// Test primary database connection first
	log.Println("Testing database connection...")
	err := db.Ping()
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	log.Println("Primary database connection successful!")

	// Create worker pools and channels
	jobs := make(chan int, queueSize)
	results := make(chan Result, totalStatements)
	
	// Create a context for all operations
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a WaitGroup to track when all goroutines are done
	var wg sync.WaitGroup

	// Create connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require connect_timeout=10",
		host, port, user, dbPassword, dbname)

	// Start workers with individual DB connections
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		
		// Create a new database connection for this worker
		workerDB, err := sql.Open("postgres", connStr)
		if err != nil {
			log.Fatalf("Failed to create DB connection for worker %d: %v", w, err)
		}
		
		// Configure this connection
		workerDB.SetMaxOpenConns(1)  // Only need one connection per worker
		workerDB.SetMaxIdleConns(1)
		workerDB.SetConnMaxLifetime(30 * time.Minute)
		
		// Start the worker with its own connection
		go func(workerID int, db *sql.DB) {
			defer db.Close() // Close this connection when worker is done
			worker(ctx, workerID, db, statements, jobs, results, &wg)
		}(w, workerDB)
	}

	// Send jobs to workers
	go func() {
		for i := range statements {
			select {
			case <-ctx.Done():
				return
			case jobs <- i:
				// Job sent successfully
			}
		}
		close(jobs) // No more jobs
	}()

	// Collect and process results
	var completed, failed int
	processedIndexes := make(map[int]bool)

	// Start a goroutine to close results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Process results as they come in
	for result := range results {
		processedIndexes[result.Index] = true
		
		if result.Error != nil {
			failed++
			log.Printf("[Worker %d][%d/%d] ERROR for range starting %s: %v", 
				result.WorkerID, result.Index+1, totalStatements, result.DateRange, result.Error)
			log.Printf("Statement: %s", result.Statement)
			
			// Ask if user wants to continue
			if !askToContinue() {
				log.Println("Operation aborted by user")
				// Cancel context to stop all workers
				cancel()
				break
			}
		} else {
			completed++
			log.Printf("[Worker %d][%d/%d] SUCCESS for range starting %s (%.2f seconds)", 
				result.WorkerID, result.Index+1, totalStatements, result.DateRange, result.ExecutionTime.Seconds())
		}
	}

	// Final summary
	log.Printf("Execution complete: %d succeeded, %d failed out of %d statements", 
		completed, failed, totalStatements)
}

// worker is a goroutine that processes jobs from the jobs channel
func worker(ctx context.Context, id int, db *sql.DB, statements []string, jobs <-chan int, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	
	for {
		select {
		case <-ctx.Done():
			return // Context cancelled, stop worker
		case i, ok := <-jobs:
			if !ok {
				return // Channel closed, no more jobs
			}
			
			stmt := statements[i]
			dateRange := getDateFromStatement(stmt)
			
			// Print query details before execution
			log.Printf("[Worker %d] Starting query %d for range %s", id, i+1, dateRange)
			log.Printf("Query %d: %s", i+1, stmt)
			
			// Use the parent context without additional timeout
			opCtx := ctx
			
			// Track execution time
			startTime := time.Now()
			
			// Set up a ticker for periodic status updates
			statusTicker := time.NewTicker(30 * time.Second)
			go func() {
				defer statusTicker.Stop()
				for {
					select {
					case <-opCtx.Done():
						return // Query finished or timed out
					case t := <-statusTicker.C:
						elapsed := t.Sub(startTime)
						log.Printf("[Worker %d] Query %d still running after %.1f seconds", 
							id, i+1, elapsed.Seconds())
					}
				}
			}()
			
			// Execute the statement
			_, err := db.ExecContext(opCtx, stmt)
			execTime := time.Since(startTime)
			
			// Send result back via results channel
			result := Result{
				Index:      i,
				Statement:  stmt,
				Error:      err,
				DateRange:  dateRange,
				ExecutionTime: execTime,
				WorkerID:   id,
			}
			
			select {
			case <-ctx.Done():
				return // Context cancelled
			case results <- result:
				// Result sent successfully
			}
		}
	}
}

// getDateFromStatement extracts the start date from an INSERT statement for logging
func getDateFromStatement(stmt string) string {
	// Simple string parsing to extract the start date for logging
	parts := strings.Split(stmt, ">=")
	if len(parts) < 2 {
		return "unknown"
	}
	
	datePart := strings.Split(parts[1], "AND")[0]
	return strings.Trim(datePart, " '")
}

// askToContinue prompts the user whether to continue after an error
func askToContinue() bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("An error occurred. Do you want to continue with the next statement? (y/n): ")
	response, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	
	response = strings.ToLower(strings.TrimSpace(response))
	return response == "y" || response == "yes"
}
