"""
DBT ADAPTER CODE-PAIR INTERVIEW - REFERENCE SOLUTION
====================================================

This is a reference solution. Try to complete the exercise yourself first!
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from contextlib import contextmanager
from enum import Enum
import time
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimpleDBError(Exception):
    """Base exception for SimpleDB errors"""
    pass


class SimpleDBConnectionError(SimpleDBError):
    """Raised when connection to SimpleDB fails"""
    pass


class SimpleDBQueryError(SimpleDBError):
    """Raised when query execution fails"""
    pass


class SQLType(Enum):
    INTEGER = "INTEGER"
    VARCHAR = "VARCHAR"
    DECIMAL = "DECIMAL"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"


@dataclass
class ColumnDefinition:
    """Represents a column in a table"""
    name: str
    sql_type: str
    nullable: bool = True


@dataclass
class TableDefinition:
    """Represents a table schema"""
    schema: str
    name: str
    columns: List[ColumnDefinition]


# ============================================================================
# PHASE 1: Type Mapping Solution
# ============================================================================

class TypeMapper:
    """Maps standard SQL types to SimpleDB types"""
    
    TYPE_MAPPING = {
        "INTEGER": "SIMPLEINT",
        "VARCHAR": "SIMPLETEXT",
        "DECIMAL": "SIMPLEDEC",
        "BOOLEAN": "SIMPLEBOOL",
        "TIMESTAMP": "SIMPLETIME",
    }
    
    def map_type(self, sql_type: str) -> str:
        """
        Convert a SQL type string to SimpleDB type.
        
        Approach:
        1. Parse the base type and parameters
        2. Map base type to SimpleDB equivalent
        3. Preserve parameters if present
        """
        sql_type = sql_type.strip().upper()
        
        # Pattern to match TYPE or TYPE(params)
        match = re.match(r'(\w+)(?:\(([^)]+)\))?', sql_type)
        
        if not match:
            raise ValueError(f"Invalid SQL type format: {sql_type}")
        
        base_type = match.group(1)
        params = match.group(2)
        
        if base_type not in self.TYPE_MAPPING:
            raise ValueError(f"Unsupported SQL type: {base_type}")
        
        simple_type = self.TYPE_MAPPING[base_type]
        
        # Add parameters if present
        if params:
            return f"{simple_type}({params})"
        
        return simple_type


# ============================================================================
# PHASE 2: SQL Generation Solution
# ============================================================================

class SQLGenerator:
    """Generates SimpleDB-specific SQL statements"""
    
    def __init__(self, type_mapper: TypeMapper):
        self.type_mapper = type_mapper
    
    def generate_create_table(self, table_def: TableDefinition) -> str:
        """
        Generate a CREATE SIMPLE_TABLE statement.
        
        Approach:
        1. Build column definitions with type mapping
        2. Handle nullability (REQUIRED vs NOT_REQUIRED)
        3. Format with proper indentation
        """
        if not table_def.columns:
            raise ValueError("Table must have at least one column")
        
        # Build column definitions
        column_defs = []
        for col in table_def.columns:
            simple_type = self.type_mapper.map_type(col.sql_type)
            nullable_spec = "NOT_REQUIRED" if col.nullable else "REQUIRED"
            column_defs.append(f"    {col.name} {simple_type} {nullable_spec}")
        
        # Assemble the SQL statement
        sql = f"CREATE SIMPLE_TABLE {table_def.schema}.{table_def.name} (\n"
        sql += ",\n".join(column_defs)
        sql += "\n) WITH SIMPLE_OPTIONS;"
        
        return sql


# ============================================================================
# PHASE 3: Connection & Execution Solution
# ============================================================================

class SimpleDBConnection:
    """Manages connection to SimpleDB"""
    
    def __init__(self, host: str, port: int, database: str, max_retries: int = 3):
        self.host = host
        self.port = port
        self.database = database
        self.max_retries = max_retries
        self._connected = False
        self._connection_attempts = 0
    
    def connect(self) -> None:
        """
        Establish connection to SimpleDB with retry logic.
        
        Simulates connection failures for first 2 attempts to test retry logic.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Connection attempt {attempt}/{self.max_retries} to {self.host}:{self.port}/{self.database}")
                
                # Simulate connection delay
                time.sleep(0.1)
                
                # Simulate failures for first 2 attempts (for testing)
                if attempt < 3:
                    raise SimpleDBConnectionError(f"Simulated connection failure (attempt {attempt})")
                
                # Success on 3rd attempt
                self._connected = True
                self._connection_attempts = attempt
                logger.info(f"Successfully connected after {attempt} attempts")
                return
                
            except SimpleDBConnectionError as e:
                if attempt == self.max_retries:
                    logger.error(f"Failed to connect after {self.max_retries} attempts")
                    raise
                logger.warning(f"Connection failed: {e}. Retrying...")
                time.sleep(0.5 * attempt)  # Exponential backoff
    
    def execute(self, query: str) -> Dict[str, Any]:
        """
        Execute a query on SimpleDB.
        
        Validates connection and simulates query execution.
        """
        if not self._connected:
            raise SimpleDBConnectionError("Not connected to database. Call connect() first.")
        
        logger.info(f"Executing query: {query[:100]}...")
        
        # Simulate query execution
        time.sleep(0.05)
        
        # Simulate occasional query errors (10% chance)
        import random
        if random.random() < 0.1:
            raise SimpleDBQueryError("Simulated query execution error")
        
        # Return simulated result
        return {
            "success": True,
            "rows_affected": 1,
            "execution_time_ms": 50,
            "query": query
        }
    
    def close(self) -> None:
        """Close the connection"""
        if self._connected:
            logger.info("Closing connection")
            self._connected = False
    
    @contextmanager
    def cursor(self):
        """
        Context manager for query execution.
        
        Ensures connection is established and properly closed.
        """
        try:
            if not self._connected:
                self.connect()
            yield self
        finally:
            self.close()


# ============================================================================
# PHASE 4: Full Adapter Solution
# ============================================================================

class SimpleDBAdapter:
    """Main adapter class that orchestrates all components"""
    
    def __init__(self, host: str, port: int, database: str):
        self.type_mapper = TypeMapper()
        self.sql_generator = SQLGenerator(self.type_mapper)
        self.connection = SimpleDBConnection(host, port, database)
    
    def create_table(self, table_def: TableDefinition) -> bool:
        """
        Create a table in SimpleDB.
        
        Orchestrates SQL generation and execution with proper error handling.
        """
        try:
            # Generate SQL
            logger.info(f"Generating CREATE TABLE for {table_def.schema}.{table_def.name}")
            sql = self.sql_generator.generate_create_table(table_def)
            logger.debug(f"Generated SQL:\n{sql}")
            
            # Execute with connection management
            with self.connection.cursor() as conn:
                result = conn.execute(sql)
                logger.info(f"Table created successfully: {result}")
                return True
                
        except SimpleDBError as e:
            logger.error(f"Failed to create table: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return False
    
    def validate_table_definition(self, table_def: TableDefinition) -> List[str]:
        """
        Validate table definition for common issues.
        
        Returns list of validation errors, empty if valid.
        """
        errors = []
        
        if not table_def.schema:
            errors.append("Schema name is required")
        
        if not table_def.name:
            errors.append("Table name is required")
        
        if not table_def.columns:
            errors.append("Table must have at least one column")
        
        # Check for duplicate column names
        column_names = [col.name for col in table_def.columns]
        if len(column_names) != len(set(column_names)):
            errors.append("Duplicate column names found")
        
        # Validate column types
        for col in table_def.columns:
            try:
                self.type_mapper.map_type(col.sql_type)
            except ValueError as e:
                errors.append(f"Invalid type for column '{col.name}': {e}")
        
        return errors


# ============================================================================
# TEST CASES
# ============================================================================

def test_type_mapper():
    """Test type mapping functionality"""
    mapper = TypeMapper()
    
    test_cases = [
        ("INTEGER", "SIMPLEINT"),
        ("VARCHAR(255)", "SIMPLETEXT(255)"),
        ("DECIMAL(10,2)", "SIMPLEDEC(10,2)"),
        ("BOOLEAN", "SIMPLEBOOL"),
        ("TIMESTAMP", "SIMPLETIME"),
    ]
    
    print("Testing Type Mapper:")
    for sql_type, expected in test_cases:
        try:
            result = mapper.map_type(sql_type)
            status = "✓" if result == expected else "✗"
            print(f"  {status} {sql_type} → {result} (expected: {expected})")
        except Exception as e:
            print(f"  ✗ {sql_type} raised {type(e).__name__}: {e}")
    
    # Test error handling
    print("\nTesting error handling:")
    try:
        mapper.map_type("UNSUPPORTED_TYPE")
        print("  ✗ Should have raised ValueError for unsupported type")
    except ValueError as e:
        print(f"  ✓ Correctly raised ValueError: {e}")


def test_sql_generator():
    """Test SQL generation"""
    mapper = TypeMapper()
    generator = SQLGenerator(mapper)
    
    table_def = TableDefinition(
        schema="myschema",
        name="users",
        columns=[
            ColumnDefinition("id", "INTEGER", nullable=False),
            ColumnDefinition("name", "VARCHAR(255)", nullable=True),
            ColumnDefinition("balance", "DECIMAL(10,2)", nullable=True),
        ]
    )
    
    print("\nTesting SQL Generator:")
    try:
        sql = generator.generate_create_table(table_def)
        print(f"Generated SQL:\n{sql}\n")
        
        # Validate output format
        assert "CREATE SIMPLE_TABLE" in sql
        assert "myschema.users" in sql
        assert "SIMPLEINT REQUIRED" in sql
        assert "SIMPLETEXT(255) NOT_REQUIRED" in sql
        assert "WITH SIMPLE_OPTIONS" in sql
        print("  ✓ SQL format validated")
        
    except Exception as e:
        print(f"  ✗ Failed: {type(e).__name__}: {e}")


def test_connection():
    """Test connection and execution"""
    print("\nTesting Connection:")
    try:
        conn = SimpleDBConnection("localhost", 5432, "testdb")
        
        # Test connection with retry
        conn.connect()
        print(f"  ✓ Connected after {conn._connection_attempts} attempts")
        
        # Test query execution
        result = conn.execute("CREATE SIMPLE_TABLE test.table (id SIMPLEINT);")
        print(f"  ✓ Query executed: {result['success']}")
        
        # Test context manager
        conn.close()
        with conn.cursor() as cursor:
            result = cursor.execute("SELECT * FROM test.table")
            print(f"  ✓ Context manager works: {result['success']}")
        
    except Exception as e:
        print(f"  ✗ Failed: {type(e).__name__}: {e}")


def test_adapter():
    """Test full adapter functionality"""
    print("\nTesting Full Adapter:")
    
    adapter = SimpleDBAdapter("localhost", 5432, "testdb")
    
    table_def = TableDefinition(
        schema="analytics",
        name="events",
        columns=[
            ColumnDefinition("event_id", "INTEGER", nullable=False),
            ColumnDefinition("user_id", "INTEGER", nullable=False),
            ColumnDefinition("event_type", "VARCHAR(100)", nullable=False),
            ColumnDefinition("created_at", "TIMESTAMP", nullable=False),
        ]
    )
    
    # Test validation
    errors = adapter.validate_table_definition(table_def)
    if errors:
        print(f"  ✗ Validation failed: {errors}")
    else:
        print("  ✓ Table definition validated")
    
    # Test table creation
    try:
        success = adapter.create_table(table_def)
        print(f"  {'✓' if success else '✗'} Table creation: {success}")
    except Exception as e:
        print(f"  ✗ Failed: {type(e).__name__}: {e}")


def test_edge_cases():
    """Test edge cases and error scenarios"""
    print("\nTesting Edge Cases:")
    
    adapter = SimpleDBAdapter("localhost", 5432, "testdb")
    
    # Test 1: Empty columns
    empty_table = TableDefinition("test", "empty", [])
    errors = adapter.validate_table_definition(empty_table)
    assert len(errors) > 0
    print(f"  ✓ Caught empty columns: {errors[0]}")
    
    # Test 2: Duplicate column names
    dup_table = TableDefinition(
        "test", "duplicates",
        [
            ColumnDefinition("id", "INTEGER"),
            ColumnDefinition("id", "VARCHAR(100)")
        ]
    )
    errors = adapter.validate_table_definition(dup_table)
    assert any("Duplicate" in err for err in errors)
    print("  ✓ Caught duplicate column names")
    
    # Test 3: Invalid type
    invalid_table = TableDefinition(
        "test", "invalid",
        [ColumnDefinition("col", "INVALID_TYPE")]
    )
    errors = adapter.validate_table_definition(invalid_table)
    assert len(errors) > 0
    print("  ✓ Caught invalid type")


if __name__ == "__main__":
    print("=" * 80)
    print("DBT ADAPTER CODE-PAIR EXERCISE - REFERENCE SOLUTION")
    print("=" * 80)
    print("\nRunning tests...\n")
    
    test_type_mapper()
    test_sql_generator()
    test_connection()
    test_adapter()
    test_edge_cases()
    
    print("\n" + "=" * 80)
    print("KEY CONCEPTS DEMONSTRATED:")
    print("=" * 80)
    print("""
1. TYPE MAPPING:
   - Regex pattern matching for type parsing
   - Preserving type parameters
   - Error handling for unsupported types
   
2. SQL GENERATION:
   - String formatting and indentation
   - Conditional logic (nullable vs required)
   - Validation before generation
   
3. CONNECTION MANAGEMENT:
   - Retry logic with exponential backoff
   - Connection state tracking
   - Context manager implementation
   - Proper resource cleanup
   
4. ERROR HANDLING:
   - Custom exception hierarchy
   - Graceful error recovery
   - Comprehensive logging
   
5. VALIDATION:
   - Pre-flight checks
   - Descriptive error messages
   - Multiple validation rules
   
6. TESTING:
   - Unit tests for each component
   - Integration tests for full flow
   - Edge case coverage
   - Assertion-based validation
    """)
    
    print("\n" + "=" * 80)
    print("DISCUSSION TOPICS:")
    print("=" * 80)
    print("""
1. CONCURRENCY:
   - Connection pooling (e.g., using queue.Queue)
   - Thread-safe operations (locks, thread-local storage)
   - Async/await for I/O operations
   
2. CACHING:
   - Cache parsed type mappings (functools.lru_cache)
   - Cache generated SQL for common patterns
   - Metadata caching (table schemas)
   
3. PRODUCTION CONSIDERATIONS:
   - Comprehensive logging and metrics (Prometheus)
   - Circuit breaker pattern for failing connections
   - Health checks and monitoring
   - Configuration management (env vars, config files)
   - Rate limiting and backpressure
   
4. SCHEMA EVOLUTION:
   - ALTER TABLE support
   - Column addition/removal
   - Type migration strategies
   - Backwards compatibility
   
5. TESTING STRATEGIES:
   - Unit tests with mocks
   - Integration tests with test database
   - Property-based testing (hypothesis)
   - Performance benchmarking
   - Chaos engineering (random failures)
   
6. VERSIONING:
   - API versioning strategy
   - Feature flags for new capabilities
   - Deprecation warnings
   - Migration guides
    """)
