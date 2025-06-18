package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// Config holds database connection parameters
type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

// MetadataResponse represents the structure of the custom API response
type MetadataResponse struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
	Data    struct {
		Info TableInfo `json:"info"`
	} `json:"data"`
}

type TableInfo struct {
	Name       string      `json:"name"`
	Location   string      `json:"location"`
	Format     string      `json:"format"`
	Columns    []Column    `json:"columns"`
	Partitions []Partition `json:"partitions"`
	Properties map[string]string `json:"properties"`
}

type Column struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

// Partition represents partition information from the API
type Partition struct {
	SourceID  int    `json:"source_id"`
	FieldID   int    `json:"field_id"`
	Name      string `json:"name"`
	Transform string `json:"transform"`
}

// PartitionDef represents individual partition definitions
type PartitionDef struct {
	Name   string `json:"name"`
	Values string `json:"values"` // e.g., "LESS THAN ('2024-01-01')" or "IN ('A', 'B')"
}

// DorisClient handles database operations
type DorisClient struct {
	db *sql.DB
}

// NewDorisClient creates a new Doris database connection
func NewDorisClient(cfg Config) (*DorisClient, error) {
	// Doris uses MySQL protocol, so we use mysql driver
	// Format: username:password@tcp(host:port)/database
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	
	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	
	return &DorisClient{db: db}, nil
}

// Close closes the database connection
func (c *DorisClient) Close() error {
	return c.db.Close()
}

// GetTables retrieves all table names from the current database
func (c *DorisClient) GetTables() ([]string, error) {
	resp, err := c.db.Exec("SWITCH nessieCatalogDev;")
	if err != nil {
		return nil, fmt.Errorf("failed to switch catalog: %w", err)
	} else {
		fmt.Println("Switched to nessieCatalogDev: ", resp)
	}
	resp, err = c.db.Exec("USE dev;")
	if err != nil {
		return nil, fmt.Errorf("failed to use database: %w", err)
	} else {
		fmt.Println("Used dev database: ", resp)
	}
	rows, err := c.db.Query("SHOW TABLES;")
	if err != nil {
		return nil, fmt.Errorf("failed to show tables: %w", err)
	}
	defer rows.Close()
	
	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	
	return tables, nil
}

// GetCreateTableDDL retrieves the CREATE TABLE statement for a given table
func (c *DorisClient) GetCreateTableDDL(tableName string) (string, error) {
	var createTableStmt string
	var tableNameResult string

	fmt.Println("Fetching DDL for table: ", tableName)
	
	query := fmt.Sprintf("SHOW CREATE TABLE %s", tableName)
	err := c.db.QueryRow(query).Scan(&tableNameResult, &createTableStmt)
	if err != nil {
		return "", fmt.Errorf("failed to get create table DDL for %s: %w", tableName, err)
	}

	fmt.Println("createTableStmt: ", createTableStmt)
	
	return createTableStmt, nil
}

// FetchPartitionMetadata fetches partition information from custom API
func FetchPartitionMetadata(tableName string) (*MetadataResponse, error) {
	// Construct the API URL with table name as parameter
	
	url := fmt.Sprintf("http://icebridge-dev.pixis.ai/namespace/dev/table/%s/info", tableName)
	fmt.Println("Fetching partition metadata for table: ", tableName)
	fmt.Println("API URL: ", url)
	
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}
	
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	
	var metadata MetadataResponse
	if err := json.Unmarshal(body, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	fmt.Println("Metadata: ", metadata.Data.Info.Partitions)
	
	return &metadata, nil
}

// BuildPartitionClause constructs the PARTITION BY clause from metadata
func BuildPartitionClause(partitions []Partition) string {
	if len(partitions) == 0 {
		return ""
	}
	
	var partitionClause strings.Builder
	
	// For Doris, we'll use RANGE partitioning for identity transforms
	// You can modify this based on your specific requirements
	partitionClause.WriteString("\nPARTITION BY (")
	
	// Add partition columns
	columns := make([]string, 0, len(partitions))
	for _, p := range partitions {
		columns = append(columns, p.Name)
	}
	partitionClause.WriteString(strings.Join(columns, ", "))
	partitionClause.WriteString(");")
	return partitionClause.String()
}

// RemoveExistingPartitionClause removes any existing PARTITION BY clause from DDL
func RemoveExistingPartitionClause(ddl string) string {
	// Find the position of LOCATION
	locationIndex := strings.Index(strings.ToUpper(ddl), "LOCATION")
	if locationIndex == -1 {
		// No LOCATION clause found, return as is
		return ddl
	}
	
	// Find the closing parenthesis before LOCATION
	// We need to keep the table definition intact
	beforeLocation := ddl[:locationIndex]
	lastParenIndex := strings.LastIndex(beforeLocation, ")")
	
	if lastParenIndex == -1 {
		// Shouldn't happen with valid DDL, but return original if no closing paren found
		return ddl
	}
	
	// Return everything up to and including the closing parenthesis
	return strings.TrimSpace(ddl[:lastParenIndex+1])
}

func main() {
	// Configuration
	config := Config{
		Host:     "10.31.50.240",
		Port:     "9030",  // Default Doris FE port
		User:     "admin",
		Password: "A4nK6YQFATTu8zk",
		Database: "",
	}
	
	// Connect to Doris
	client, err := NewDorisClient(config)
	if err != nil {
		log.Fatalf("Failed to connect to Doris: %v", err)
	}
	defer client.Close()
	
	// Get all tables
	tables, err := client.GetTables()
	if err != nil {
		log.Fatalf("Failed to get tables: %v", err)
	}
	
	fmt.Printf("Found %d tables\n\n", len(tables))
	
	// Process each table
	for _, tableName := range tables {
		fmt.Printf("Processing table: %s\n", tableName)
		fmt.Println(strings.Repeat("-", 50))
		
		// Get original DDL
		ddl, err := client.GetCreateTableDDL(tableName)
		if err != nil {
			log.Printf("Error getting DDL for table %s: %v", tableName, err)
			continue
		}
		
		// Fetch partition metadata from API
		metadata, err := FetchPartitionMetadata(tableName)
		if err != nil {
			log.Printf("Error fetching metadata for table %s: %v", tableName, err)
			// Print original DDL if metadata fetch fails
			fmt.Printf("Original DDL:\n%s\n\n", ddl)
			continue
		}
		
		// Remove any existing partition clause
		ddlWithoutPartition := RemoveExistingPartitionClause(ddl)
		
		// Build new partition clause
		partitionClause := BuildPartitionClause(metadata.Data.Info.Partitions)
		
		// Concatenate DDL with new partition clause
		completeDDL := ddlWithoutPartition
		if partitionClause != "" {
			completeDDL += partitionClause
		}
		
		// Print the complete query
		fmt.Printf("Complete DDL with Partition:\n%s\n\n", completeDDL)
		fmt.Println(strings.Repeat("=", 80))
		fmt.Println()
	}
}