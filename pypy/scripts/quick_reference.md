# DBT Adapter Interview - Quick Reference Card

## Time-boxed Plan (45 mins total)

```
┌─────────────────────────────────────────────────────────────┐
│ 0-5 min:   Read, clarify, plan approach                    │
│ 5-15 min:  Phase 1 - Type mapping (foundation)             │
│ 15-30 min: Phase 2 - SQL generation (core logic)           │
│ 30-40 min: Phase 3 - Connection + error handling           │
│ 40-45 min: Test, discuss, improvements                     │
└─────────────────────────────────────────────────────────────┘
```

---

## Must-Have Code Patterns

### 1. Type Mapping (Phase 1)
```python
def map_type(self, sql_type: str) -> str:
    # Parse: "VARCHAR(255)" -> base="VARCHAR", params="255"
    match = re.match(r'(\w+)(?:\(([^)]+)\))?', sql_type.upper())
    base_type = match.group(1)
    params = match.group(2)
    
    # Map base type
    if base_type not in TYPE_MAPPING:
        raise ValueError(f"Unsupported: {base_type}")
    
    simple_type = TYPE_MAPPING[base_type]
    
    # Preserve parameters
    return f"{simple_type}({params})" if params else simple_type
```

### 2. SQL Generation (Phase 2)
```python
def generate_create_table(self, table_def: TableDefinition) -> str:
    column_defs = []
    for col in table_def.columns:
        simple_type = self.type_mapper.map_type(col.sql_type)
        nullable = "NOT_REQUIRED" if col.nullable else "REQUIRED"
        column_defs.append(f"    {col.name} {simple_type} {nullable}")
    
    sql = f"CREATE SIMPLE_TABLE {table_def.schema}.{table_def.name} (\n"
    sql += ",\n".join(column_defs)
    sql += "\n) WITH SIMPLE_OPTIONS;"
    return sql
```

### 3. Connection with Retry (Phase 3)
```python
def connect(self) -> None:
    for attempt in range(1, self.max_retries + 1):
        try:
            logger.info(f"Attempt {attempt}/{self.max_retries}")
            # ... connection logic ...
            self._connected = True
            return
        except SimpleDBConnectionError as e:
            if attempt == self.max_retries:
                raise
            logger.warning(f"Retry after: {e}")
            time.sleep(0.5 * attempt)  # Exponential backoff
```

### 4. Context Manager (Phase 3)
```python
@contextmanager
def cursor(self):
    try:
        if not self._connected:
            self.connect()
        yield self
    finally:
        self.close()
```

---

## Essential Imports

```python
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from contextlib import contextmanager
from enum import Enum
import time
import re
import logging
```

---

## Common Patterns Reference

### Error Handling
```python
try:
    result = risky_operation()
except SpecificError as e:
    logger.error(f"Failed: {e}")
    raise CustomError(f"Context: {e}") from e
```

### Validation
```python
def validate(self, obj) -> List[str]:
    errors = []
    if not obj.required_field:
        errors.append("Missing required_field")
    return errors
```

### Logging
```python
logger.info("Starting operation")
logger.debug(f"Details: {data}")
logger.warning("Potential issue")
logger.error(f"Failed: {error}")
```

---

## Type Mapping Cheat Sheet

```
INTEGER   → SIMPLEINT
VARCHAR   → SIMPLETEXT
DECIMAL   → SIMPLEDEC
BOOLEAN   → SIMPLEBOOL
TIMESTAMP → SIMPLETIME
```

---

## SQL Template

```sql
CREATE SIMPLE_TABLE schema.table (
    col1 SIMPLEINT REQUIRED,
    col2 SIMPLETEXT(255) NOT_REQUIRED
) WITH SIMPLE_OPTIONS;
```

---

## Testing Quick Checks

```python
# Phase 1: Type mapping
assert mapper.map_type("INTEGER") == "SIMPLEINT"
assert mapper.map_type("VARCHAR(255)") == "SIMPLETEXT(255)"

# Phase 2: SQL generation
sql = generator.generate_create_table(table_def)
assert "CREATE SIMPLE_TABLE" in sql
assert "WITH SIMPLE_OPTIONS" in sql

# Phase 3: Connection
conn = SimpleDBConnection("localhost", 5432, "db")
conn.connect()
result = conn.execute("SELECT 1")
assert result["success"]
```

---

## Communication Templates

### Starting
> "Let me make sure I understand: I need to implement X, Y, and Z. Is that correct?"

### Thinking out loud
> "I'm using regex here because I need to extract both the type and parameters..."

### When stuck (after 2-3 mins)
> "I'm considering two approaches: A or B. Which would you prefer I focus on?"

### Asking for hint
> "I'm stuck on parsing the type parameters. Would you suggest regex or string splitting?"

### When done with phase
> "I've completed the type mapping. Should I add more edge cases or move to SQL generation?"

### Accepting feedback
> "Good point! Let me refactor that to handle the edge case..."

---

## Red Flags to Avoid

- ❌ Silent for >3 minutes
- ❌ No error handling
- ❌ Missing type hints
- ❌ No docstrings
- ❌ Giant functions (>30 lines)
- ❌ Hard-coded values
- ❌ No logging
- ❌ Ignoring test cases

---

## Green Flags to Show

- ✅ Think out loud
- ✅ Ask clarifying questions
- ✅ Handle errors gracefully
- ✅ Write incremental code
- ✅ Test as you go
- ✅ Use descriptive names
- ✅ Add docstrings
- ✅ Show trade-off thinking

---

## If Running Behind

**Priority Order:**
1. ✅ Type mapping working
2. ✅ SQL generation working
3. ✅ Basic connection working
4. ⏭️ Retry logic (discuss instead)
5. ⏭️ Context manager (discuss instead)

Say: "I'd implement retry logic next with exponential backoff..."

---

## If Running Ahead

**Add These:**
1. Validation methods
2. Better error messages
3. More test cases
4. Edge case handling
5. Performance considerations

---

## Regex Patterns (Quick Copy)

```python
# Parse TYPE(params)
r'(\w+)(?:\(([^)]+)\))?'

# Validate identifier
r'^[a-zA-Z_][a-zA-Z0-9_]*$'

# Parse DECIMAL(10,2)
r'DECIMAL\((\d+),(\d+)\)'
```

---

## Key Discussion Points

### When asked "How would you improve this?"
- Connection pooling
- Caching (LRU cache for type mappings)
- Async/await for I/O
- Metrics and observability
- Better validation
- Schema evolution support

### When asked "How would you test this?"
- Unit tests with mocks
- Integration tests with Docker
- Property-based testing
- Performance benchmarks
- Chaos testing (random failures)

### When asked "What about production?"
- Comprehensive logging
- Metrics (latency, errors, throughput)
- Health checks
- Rate limiting
- Connection pooling
- Circuit breakers
- Configuration management

---

## Emergency Debugging

### Code not working?
1. Print intermediate values
2. Check types (print(type(x)))
3. Verify test inputs
4. Check exception messages
5. Add try-except with print

### Regex not matching?
```python
# Test interactively
import re
pattern = r'(\w+)(?:\(([^)]+)\))?'
test = "VARCHAR(255)"
match = re.match(pattern, test)
print(match.groups())  # Should be ('VARCHAR', '255')
```

### Connection issues?
```python
# Check state
print(f"Connected: {self._connected}")
print(f"Attempts: {self._connection_attempts}")
```

---

## Last-Minute Checklist

Before saying "I'm done":

```
□ Does it run without errors?
□ Do test cases pass?
□ Are there type hints?
□ Are there docstrings?
□ Is error handling present?
□ Are variable names clear?
□ Did I explain my approach?
□ Can I answer "why" questions?
```

---

## Sample "Why" Answers

**Q: Why regex over string split?**
> "Regex handles edge cases like 'DECIMAL(10,2)' where there's a comma. Split would be simpler for 'VARCHAR(255)' but less robust."

**Q: Why custom exceptions?**
> "Different errors need different handling. Connection errors should retry, but syntax errors should fail fast. Custom exceptions make this explicit."

**Q: Why context manager?**
> "Ensures cleanup happens even if exceptions occur. Better than manual try-finally, and more Pythonic for resource management."

**Q: Why dataclasses?**
> "Reduces boilerplate, provides automatic __init__ and __repr__, and makes data structures self-documenting with type hints."

---

## Remember

- **Communication > Perfect Code**
- **Working Code > Optimized Code**
- **Incremental > All-at-once**
- **Questions > Assumptions**
- **Collaboration > Solo Work**

You've got this! 💪
