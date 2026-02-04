# Array Support & NUMERIC Handling - Implementation Plan

## Status: BOTH VIABLE ✅

Both features are straightforward additions to the existing architecture.

## 1. Array Support

### Complexity: Low-Medium
**Estimated Time:** 30-60 minutes

### What's Needed

PostgreSQL arrays can be converted to `z::dyn::Value::Array` (which maps to native arrays in all formats).

### Implementation Approach

```cpp
// Add to datum_to_dynamic() function

// Check if type is an array
if (type_is_array(typid)) {
    ArrayType* arr = DatumGetArrayTypeP(value);
    int ndim = ARR_NDIM(arr);

    // For simplicity, handle 1D arrays first
    if (ndim == 1) {
        Oid element_type = ARR_ELEMTYPE(arr);
        int nitems;
        Datum* elements;
        bool* nulls;

        // Deconstruct array into elements
        deconstruct_array(arr, element_type,
                         -1, true, 'i',  // Will be adjusted per type
                         &elements, &nulls, &nitems);

        // Build dyn::Value::Array
        z::dyn::Value::Array result_array;
        for (int i = 0; i < nitems; i++) {
            result_array.push_back(
                datum_to_dynamic(elements[i], element_type, nulls[i])
            );
        }

        pfree(elements);
        pfree(nulls);

        return z::dyn::Value::array(std::move(result_array));
    }
}
```

### PostgreSQL Array Types Supported

All existing types would automatically work in arrays:
- `INT[]` - Integer arrays
- `TEXT[]` - Text arrays
- `FLOAT[]` - Float arrays
- `BOOL[]` - Boolean arrays

### Example Usage

```sql
-- Integer array
SELECT row_to_msgpack(ROW(ARRAY[1,2,3,4,5]));

-- Text array
SELECT row_to_msgpack(ROW(ARRAY['a','b','c']));

-- Mixed record with array
SELECT row_to_msgpack(
    ROW('user', 25, ARRAY['tag1', 'tag2', 'tag3'])::record
);

-- Real table with arrays
CREATE TABLE posts (
    id int,
    title text,
    tags text[]
);

SELECT row_to_msgpack(posts.*) FROM posts;
```

### Multidimensional Arrays

For full support, we'd need to handle multidimensional arrays:
- PostgreSQL supports multi-dim: `int[][]`
- Could flatten to 1D, or
- Use nested arrays: `Array<Array<int>>`

Initial implementation: 1D arrays (covers 90% of use cases)
Later: Multi-dimensional support

### Performance Impact

Minimal - just iterating through array elements, which we already do for record fields.

---

## 2. NUMERIC Handling

### Complexity: Low
**Estimated Time:** 15-30 minutes

### Current Behavior

NUMERIC is currently converted to text string:
```sql
SELECT row_to_msgpack(ROW(123.45::numeric));
-- numeric becomes "123.45" (string)
```

### Improvement Options

#### Option A: Convert to Double (Simple, Some Precision Loss)

```cpp
case NUMERICOID:
{
    // Convert NUMERIC to float8 (double)
    Datum float_val = DirectFunctionCall1(numeric_float8, value);
    return z::dyn::Value(DatumGetFloat8(float_val));
}
```

**Pros:**
- Simple, fast
- Native number in binary formats
- Works for most use cases

**Cons:**
- Precision loss for very large/precise decimals
- Max ~15-17 significant digits

#### Option B: Smart Conversion (Best of Both Worlds)

```cpp
case NUMERICOID:
{
    // Try to convert to double
    Datum float_val = DirectFunctionCall1(numeric_float8, value);
    double d = DatumGetFloat8(float_val);

    // Check if conversion is exact
    Datum back = DirectFunctionCall1(float8_numeric, float_val);

    if (DirectFunctionCall2(numeric_eq, value, back)) {
        // Lossless conversion, use double
        return z::dyn::Value(d);
    } else {
        // Would lose precision, use string
        char* str = DatumGetCString(
            DirectFunctionCall1(numeric_out, value)
        );
        z::dyn::Value result(std::string(str));
        pfree(str);
        return result;
    }
}
```

**Pros:**
- Uses native number when possible
- Falls back to string for high precision
- Best of both worlds

**Cons:**
- Slightly more complex
- Extra conversion overhead

#### Option C: Always Use String (Current, Safe)

Keep current behavior - always convert to string.

**Pros:**
- No precision loss ever
- Simple

**Cons:**
- Larger output
- Not a native number in binary format

### Recommendation: Option A (Convert to Double)

For most use cases (prices, percentages, measurements), double precision is sufficient.

**Implementation:**

```cpp
case NUMERICOID:
{
    // Convert NUMERIC to double
    Datum float_val = DirectFunctionCall1(numeric_float8, value);
    return z::dyn::Value(DatumGetFloat8(float_val));
}
```

Add one line to the switch statement - that's it!

### Example Impact

```sql
-- Before (current):
SELECT row_to_msgpack(ROW(123.45::numeric));
-- Result: {"f1": "123.45"}  (string)
-- Size: ~20 bytes

-- After (with double):
SELECT row_to_msgpack(ROW(123.45::numeric));
-- Result: {"f1": 123.45}  (number)
-- Size: ~15 bytes
```

**Space savings:** ~5 bytes per numeric field

---

## Combined Implementation

Both features can be added in one session:

### Code Changes Required

**File:** `pg_zerialize.cpp`
**Location:** `datum_to_dynamic()` function
**Lines to add:** ~40-50 lines total

### Step-by-Step Plan

1. **Add NUMERIC support** (5 minutes)
   - Add one case to switch statement
   - Test with numeric values

2. **Add array support** (30-45 minutes)
   - Add array detection
   - Add array deconstruction
   - Add recursive conversion
   - Test with various array types

3. **Update documentation** (10 minutes)
   - Update README
   - Add examples
   - Note limitations if any

4. **Testing** (15 minutes)
   - Test numeric values
   - Test arrays (1D)
   - Test mixed records
   - Test all formats

### Total Estimated Time: 1-1.5 hours

---

## Benefits

### Array Support Adds:
- Support for PostgreSQL arrays
- Natural array representation in binary formats
- Common use case (tags, lists, etc.)

### NUMERIC Handling Adds:
- Smaller output (~5 bytes per numeric)
- Native number representation
- Better type preservation

### Combined Impact:
- More complete PostgreSQL type coverage
- Better compression (numeric as number, not string)
- More natural data representation

---

## Testing Plan

### Arrays

```sql
-- Test 1: Simple integer array
SELECT row_to_msgpack(ROW(ARRAY[1,2,3]));

-- Test 2: Text array
SELECT row_to_msgpack(ROW(ARRAY['a','b','c']));

-- Test 3: Mixed with arrays
SELECT row_to_msgpack(
    ROW('user', 25, ARRAY['tag1','tag2'])::record
);

-- Test 4: NULL in array
SELECT row_to_msgpack(ROW(ARRAY[1,NULL,3]));

-- Test 5: Empty array
SELECT row_to_msgpack(ROW(ARRAY[]::int[]));
```

### NUMERIC

```sql
-- Test 1: Simple decimal
SELECT row_to_msgpack(ROW(123.45::numeric));

-- Test 2: Large precision
SELECT row_to_msgpack(ROW(123.456789012345::numeric));

-- Test 3: Very large number
SELECT row_to_msgpack(ROW(999999999999999.99::numeric));

-- Test 4: Money type (based on numeric)
SELECT row_to_msgpack(ROW(123.45::money));
```

---

## Potential Issues & Solutions

### Arrays

**Issue:** Multidimensional arrays
**Solution:** Start with 1D, add multi-dim later if needed

**Issue:** Array of composite types
**Solution:** Recursively call datum_to_dynamic() - already works!

**Issue:** Very large arrays
**Solution:** Add size limit or streaming support later

### NUMERIC

**Issue:** Precision loss with double
**Solution:** Document limitation, or use Option B (smart conversion)

**Issue:** Very large decimals (>308 exponent)
**Solution:** Fall back to string for out-of-range values

**Issue:** Money type
**Solution:** Treat as NUMERIC (it's based on numeric internally)

---

## Recommendation

**Implement both!** They're:
- ✅ Straightforward
- ✅ High value
- ✅ Low risk
- ✅ ~1 hour total

The current template architecture makes this easy - just enhance the `datum_to_dynamic()` function.

Would you like me to implement these now?
