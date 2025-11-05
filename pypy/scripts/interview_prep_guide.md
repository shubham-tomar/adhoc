# DBT Adapter Interview Preparation Guide

## About Your Interviewer (Colin Rogers)

**Focus Areas:**
- dbt Core maintenance and development
- dbt-adapters (integration layer between dbt and data platforms)
- Open-source contributions and community engagement
- Python-based data engineering infrastructure

**Recent Work Patterns (from GitHub):**
- Adapter protocol improvements
- Connection management and error handling
- SQL generation and query planning
- Cross-platform compatibility
- Testing infrastructure for adapters

---

## Interview Strategy

### 1. **Initial Approach (First 5 minutes)**

When given the problem:
- ✅ **Read the entire problem first** - Don't start coding immediately
- ✅ **Ask clarifying questions:**
  - "Are there any specific SimpleDB quirks I should know about?"
  - "Should I prioritize correctness or handle edge cases?"
  - "Do you want me to write tests as I go or after?"
- ✅ **State your approach** - "I'll start with type mapping since that's foundational..."
- ✅ **Discuss trade-offs** - "I could use regex or string splitting for parsing..."

### 2. **During Coding (30 minutes)**

**Think out loud:**
```python
# Good: "I'm using regex here because VARCHAR(255) has parameters
# that I need to preserve..."

# Good: "I'll add a try-except here since connection failures are expected..."

# Good: "Let me add a docstring to clarify what this returns..."
```

**Incremental Development:**
- Write skeleton first, then fill in implementation
- Test each component before moving to the next
- Refactor only when something works

**Show Good Practices:**
- Type hints on all functions
- Descriptive variable names
- Error handling with specific exceptions
- Logging for debugging
- Validation before operations

### 3. **Common Pitfalls to Avoid**

❌ **Don't:**
- Start coding without understanding requirements
- Write everything in one giant function
- Ignore error handling
- Skip docstrings/comments
- Get stuck on perfect code (iterate instead)
- Be silent - they can't read your mind!

✅ **Do:**
- Break down into small functions
- Handle errors gracefully
- Write testable code
- Communicate your thought process
- Ask for hints if stuck (shows humility)

---

## Technical Deep Dives (Likely Follow-ups)

### 1. **Type System Design**

**Q: "How would you handle custom types or user-defined types?"**

```python
# Show extensibility:
class TypeMapper:
    def __init__(self, custom_mappings: Optional[Dict[str, str]] = None):
        self.base_mappings = {...}
        self.custom_mappings = custom_mappings or {}
    
    def register_type(self, sql_type: str, simple_type: str):
        """Allow runtime type registration"""
        self.custom_mappings[sql_type] = simple_type
    
    def map_type(self, sql_type: str) -> str:
        # Check custom first, then base
        ...
```

**Discussion Points:**
- Type system versioning
- Backwards compatibility
- Type inference vs explicit declaration
- Complex types (arrays, structs, nested types)

### 2. **Connection Management**

**Q: "How would you implement connection pooling?"**

```python
import queue
from threading import Lock

class ConnectionPool:
    def __init__(self, max_connections: int = 5):
        self.pool = queue.Queue(maxsize=max_connections)
        self.lock = Lock()
        self._total_connections = 0
        
    def get_connection(self) -> SimpleDBConnection:
        try:
            # Try to get existing connection (non-blocking)
            return self.pool.get_nowait()
        except queue.Empty:
            # Create new if under limit
            with self.lock:
                if self._total_connections < self.pool.maxsize:
                    conn = SimpleDBConnection(...)
                    self._total_connections += 1
                    return conn
            # Wait for available connection
            return self.pool.get()
    
    def return_connection(self, conn: SimpleDBConnection):
        if conn.is_healthy():
            self.pool.put(conn)
        else:
            # Replace unhealthy connection
            self._total_connections -= 1
```

**Discussion Points:**
- Pool exhaustion strategies
- Connection health checks
- Timeouts and dead connections
- Thread safety vs process safety

### 3. **Error Handling & Retries**

**Q: "When should you retry vs fail fast?"**

**Retry:**
- Transient network errors
- Connection timeouts
- Rate limiting (with backoff)
- Lock conflicts in concurrent operations

**Fail Fast:**
- Authentication failures
- SQL syntax errors
- Schema validation errors
- Quota exceeded errors

```python
from enum import Enum

class ErrorSeverity(Enum):
    TRANSIENT = "transient"  # Retry
    PERMANENT = "permanent"  # Fail fast
    UNKNOWN = "unknown"      # Limited retry

def classify_error(error: Exception) -> ErrorSeverity:
    if isinstance(error, (ConnectionTimeout, NetworkError)):
        return ErrorSeverity.TRANSIENT
    if isinstance(error, (AuthError, SyntaxError)):
        return ErrorSeverity.PERMANENT
    return ErrorSeverity.UNKNOWN
```

### 4. **SQL Generation**

**Q: "How would you handle SQL injection prevention?"**

```python
class SQLGenerator:
    def sanitize_identifier(self, name: str) -> str:
        """Escape identifiers to prevent injection"""
        # Remove or escape dangerous characters
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(f"Invalid identifier: {name}")
        return f'"{name}"'  # Quote it
    
    def generate_create_table(self, table_def: TableDefinition) -> str:
        schema = self.sanitize_identifier(table_def.schema)
        name = self.sanitize_identifier(table_def.name)
        # Use parameterized queries where possible
        ...
```

**Discussion Points:**
- Parameterized queries vs string interpolation
- Identifier quoting strategies
- Case sensitivity handling
- Reserved keyword conflicts

### 5. **Testing Strategies**

**Q: "How would you test an adapter without access to the actual database?"**

```python
# Strategy 1: Mock the connection
from unittest.mock import Mock, patch

def test_adapter_with_mock():
    with patch('adapter.SimpleDBConnection') as mock_conn:
        mock_conn.return_value.execute.return_value = {"success": True}
        adapter = SimpleDBAdapter("localhost", 5432, "test")
        result = adapter.create_table(table_def)
        assert result == True

# Strategy 2: In-memory database simulation
class InMemorySimpleDB:
    def __init__(self):
        self.tables = {}
    
    def execute(self, query: str):
        # Parse and execute in-memory
        ...

# Strategy 3: Docker containers for integration tests
def test_with_docker():
    with DockerContainer("simpledb:latest") as db:
        adapter = SimpleDBAdapter(db.host, db.port, "test")
        # Run real queries
        ...
```

**Testing Levels:**
1. **Unit Tests**: Individual functions with mocks
2. **Integration Tests**: Real database, isolated transactions
3. **Contract Tests**: Adapter protocol compliance
4. **Performance Tests**: Benchmark critical operations
5. **Chaos Tests**: Random failures, network issues

---

## Advanced Topics (If Time Permits)

### 1. **Concurrency & Parallelism**

```python
import asyncio

class AsyncSimpleDBConnection:
    async def execute(self, query: str) -> Dict[str, Any]:
        # Non-blocking I/O
        async with aiohttp.ClientSession() as session:
            response = await session.post(...)
            return await response.json()

# Batch operations
async def create_tables_parallel(tables: List[TableDefinition]):
    tasks = [adapter.create_table(table) for table in tables]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results
```

### 2. **Observability**

```python
import time
from contextlib import contextmanager

@contextmanager
def measure_time(operation: str):
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        metrics.record(f"adapter.{operation}.duration", duration)

class InstrumentedAdapter:
    def create_table(self, table_def: TableDefinition):
        with measure_time("create_table"):
            metrics.increment("adapter.create_table.attempts")
            try:
                result = super().create_table(table_def)
                metrics.increment("adapter.create_table.success")
                return result
            except Exception as e:
                metrics.increment(f"adapter.create_table.error.{type(e).__name__}")
                raise
```

### 3. **Schema Evolution**

```python
class SchemaEvolution:
    def detect_changes(
        self, 
        old_schema: TableDefinition, 
        new_schema: TableDefinition
    ) -> List[SchemaChange]:
        """Detect differences between schemas"""
        changes = []
        
        # New columns
        old_cols = {c.name for c in old_schema.columns}
        new_cols = {c.name for c in new_schema.columns}
        
        for col_name in new_cols - old_cols:
            changes.append(AddColumn(col_name))
        
        for col_name in old_cols - new_cols:
            changes.append(DropColumn(col_name))
        
        # Type changes
        for col in new_schema.columns:
            old_col = self._find_column(old_schema, col.name)
            if old_col and old_col.sql_type != col.sql_type:
                changes.append(AlterColumnType(col.name, old_col.sql_type, col.sql_type))
        
        return changes
    
    def generate_migration(self, changes: List[SchemaChange]) -> str:
        """Generate SQL for schema migration"""
        sql_statements = []
        for change in changes:
            if isinstance(change, AddColumn):
                sql_statements.append(f"ALTER TABLE ... ADD COLUMN {change.column} ...")
            # ... handle other changes
        return ";\n".join(sql_statements)
```

---

## Questions to Ask Colin

### About the Role:
1. "What's the most challenging adapter integration you've worked on?"
2. "How do you balance new features vs maintaining existing adapters?"
3. "What's your testing philosophy for adapter code?"

### About dbt:
1. "How does the adapter protocol evolve? Is there a deprecation process?"
2. "What are common pitfalls when implementing a new adapter?"
3. "How do you handle platform-specific SQL dialects?"

### Technical:
1. "How do you approach cross-platform testing with so many data warehouses?"
2. "What's the strategy for handling breaking changes in data platforms?"
3. "Are there plans to support more async operations in adapters?"

---

## Time Management

```
0:00 - 0:05   |  Read problem, ask questions, discuss approach
0:05 - 0:15   |  Phase 1: Type mapping (core foundation)
0:15 - 0:30   |  Phase 2: SQL generation (main logic)
0:30 - 0:40   |  Phase 3: Connection management (error handling)
0:40 - 0:45   |  Run tests, discuss improvements, answer questions
```

**If running behind:**
- Skip fancy features, focus on core functionality
- Add TODOs for improvements: `# TODO: Add connection pooling`
- Discuss what you'd add with more time

**If ahead:**
- Add validation methods
- Improve error messages
- Add more comprehensive tests
- Discuss optimizations

---

## Code Quality Checklist

Before saying "I'm done":

- [ ] All functions have type hints
- [ ] All functions have docstrings
- [ ] Error cases are handled
- [ ] Tests pass (or run them if time)
- [ ] Code is reasonably organized
- [ ] No obvious bugs (off-by-one, None checks, etc.)
- [ ] Logging is present for debugging
- [ ] Variable names are descriptive

---

## Common Interview Mistakes (and how to avoid them)

### 1. **Silent Coding**
❌ Writes code in silence for 10 minutes  
✅ "I'm using a dictionary here because lookup is O(1)..."

### 2. **Perfect is the enemy of good**
❌ Spends 20 mins on perfect error handling for Phase 1  
✅ Gets basic version working, then iterates

### 3. **Not testing incrementally**
❌ Writes all code, then realizes type mapping is broken  
✅ Tests `map_type()` with a few examples before moving on

### 4. **Ignoring the interviewer**
❌ Dismisses hints or suggestions  
✅ "Oh good point! Let me adjust that..."

### 5. **Getting stuck without asking**
❌ Stares at screen for 5 minutes stuck on regex  
✅ "I'm trying to parse VARCHAR(255) - would you prefer regex or split()?"

---

## Final Tips

1. **Breathe**: It's okay to pause and think
2. **Be yourself**: They're evaluating culture fit too
3. **Show curiosity**: Ask questions about their work
4. **Handle ambiguity**: Real work is often underspecified
5. **Admit gaps**: "I haven't used that pattern, but I'd approach it by..."
6. **Show growth mindset**: "That's a great suggestion, I'll incorporate that"

---

## Good Luck! 🚀

Remember: They're not looking for perfection. They want to see:
- How you think through problems
- How you communicate technical ideas
- How you handle feedback
- Whether you'd be good to work with

Colin maintains open-source code used by thousands - he values clear thinking, good communication, and collaborative problem-solving over perfect code.
