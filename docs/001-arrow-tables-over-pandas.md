# 001: Use Arrow Tables Over Pandas for Data Type Handling

**Date**: 2025-10-21
**Status**: Accepted

## Context

When loading AWS CUR CSV files with mixed-type columns (e.g., `product/ecu` can be both strings like "73" and numeric values), pandas DataFrame type inference fails with PyArrow conversion errors:

```
ArrowInvalid: Could not convert '73' with type str: tried to convert to double
Conversion failed for column product/ecu with type object
```

## Problem

We need to handle AWS CUR billing data that has:
- Mixed-type columns (strings and numbers in the same column)
- AWS-specific column naming (`category/name` format with slashes)
- Complex schema that changes monthly (AWS adds/removes columns)
- Large data volumes requiring efficient processing

Initial approach using `pandas.read_csv()` hit type inference issues that would require either:
- Converting everything to strings (loses type information, poor for analytics)
- Complex pandas dtype specifications (fragile, hard to maintain)

## Research Findings

### DLT Recommendations

From DLT documentation research:

1. **DLT recommends yielding Arrow tables over pandas DataFrames**
   - "We recommend yielding Arrow tables from your resources"
   - Arrow tables are schema-aware (store column names and types)
   - Bypasses normalization steps for Parquet-compatible destinations like BigQuery
   - No data conversion performed - types pass through directly

2. **Performance benefits**
   - Arrow route uses columnar format for better memory efficiency
   - Delegates computation to PyArrow's C++ backend vs Python row iteration
   - Direct Parquet file writing to BigQuery without intermediate processing

3. **Type handling**
   - "The data in the table must be compatible with the destination database as no data conversion is performed"
   - Schema inference not needed because Arrow tables are schema-aware
   - Eliminates DLT's need to infer types from data

### PyArrow CSV Reader

PyArrow's CSV reader supports explicit column type specification:

```python
import pyarrow.csv as csv

table = csv.read_csv(
    file_path,
    convert_options=csv.ConvertOptions(
        column_types={
            'column_name': pa.string(),  # Force specific type
            # Other columns use inference
        }
    )
)
```

Key capabilities:
- `column_types` parameter disables inference for specified columns
- Handles gzip compression automatically
- Can map all columns explicitly or just problematic ones
- Type mismatches fail fast with clear errors

## Decision

**Use PyArrow CSV reader with manifest-driven schema to produce Arrow tables.**

### Implementation Strategy

1. **Extract column types from AWS CUR manifest**
   - Manifest contains: `{"category": "product", "name": "ecu", "type": "String"}`
   - Map AWS types to PyArrow types

2. **Read CSV with explicit types**
   - Use `pyarrow.csv.read_csv()` instead of `pandas.read_csv()`
   - Pass `ConvertOptions(column_types=...)` with manifest-derived types

3. **Yield Arrow tables to DLT**
   - Return `pa.Table` objects instead of `pd.DataFrame`
   - DLT handles Arrow -> BigQuery Parquet conversion automatically

### Type Mapping

```python
AWS_TO_ARROW_TYPES = {
    "String": pa.string(),
    "BigDecimal": pa.float64(),  # Or pa.decimal128(38, 9) if precision needed
    "DateTime": pa.timestamp('s'),
    "OptionalString": pa.string(),
    "Interval": pa.string(),
}
```

## Benefits

1. **Proper type preservation**: Decimals as floats, strings as strings, no data loss
2. **No inference errors**: Explicit types prevent PyArrow guessing wrong
3. **Performance**: Arrow -> Parquet -> BigQuery is fastest path in DLT
4. **Schema evolution**: Manifest updates automatically include new column types
5. **DLT idiomatic**: Following DLT's recommended approach
6. **Maintainable**: Type mapping centralized, not scattered across code

## Alternatives Considered

### 1. Convert everything to strings
- **Rejected**: Loses type information, poor for analytics, harder queries
- User explicitly stated: "I don't wanna just hammer everything into strings because that sucks"

### 2. Use pandas with complex dtype specification
- **Rejected**: Pandas dtype specification is fragile and doesn't handle mixed types well
- Still requires conversion from pandas -> Arrow -> Parquet
- Extra step in processing pipeline

### 3. Let DLT infer types
- **Rejected**: DLT inference works on JSON-like data, not CSV with mixed types
- Would still hit the same PyArrow conversion errors we saw

## Implementation Notes

- Modify `_read_billing_file()` to return `pa.Table` instead of `pd.DataFrame`
- Add `_build_column_types_from_manifest()` helper function
- Update type hints in resource function
- No changes needed to DLT resource decorator or pipeline setup
- Column name normalization (slashes -> underscores) still handled by DLT automatically

## References

- [DLT Arrow/Pandas Documentation](https://dlthub.com/docs/dlt-ecosystem/verified-sources/arrow-pandas)
- [DLT: How we use Apache Arrow](https://dlthub.com/blog/how-dlt-uses-apache-arrow)
- [PyArrow CSV ConvertOptions](https://arrow.apache.org/docs/python/generated/pyarrow.csv.ConvertOptions.html)
- [PyArrow CSV Reading](https://arrow.apache.org/docs/python/csv.html)
