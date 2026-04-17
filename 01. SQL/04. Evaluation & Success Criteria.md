## 5. EVALUATION & SUCCESS CRITERIA

| Dimension | Weight | Evaluation Standard |
|---|---|---|
| Logical Correctness | 25% | Query returns accurate results against known benchmarks. Handles edge cases explicitly. |
| Performance & Cost | 20% | Execution plan shows pruning, caching utilization, or clustering efficiency. Credit burn documented. |
| Maintainability | 20% | CTEs are logically named. Comments explain business intent, not syntax. No duplicated logic. |
| Data Quality Awareness | 15% | Explicit handling of NULLs, duplicates, late arrivals, NDC supersession, formulary SCDs. |
| Production Readiness | 20% | Query tagged, idempotent where applicable, HIPAA masking noted, error handling documented. |

**Submission Requirements**:
- Each query must include rationale, assumptions, trade-offs.
- EXPLAIN output or plan summary required for Scenarios 5, 8, 12, 14, 15.
- All queries formatted for Snowflake syntax.
- No hard-coded dates unless explicitly required. Use dynamic ranges.
- Include at least one data validation check per scenario.
- HIPAA/PII handling explicitly noted where applicable.


# COMPLETE SOLUTIONS, QUERIES, ASSUMPTIONS, EXPLANATIONS
