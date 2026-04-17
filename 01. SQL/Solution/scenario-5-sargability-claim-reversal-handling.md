## SCENARIO 5: SARGABILITY & CLAIM REVERSAL HANDLING

**Assumptions**:
- Reversal identified via `reversal_claim_id` linking to original `claim_id`.
- December 2023 = 2023-12-01 to 2024-01-01 (exclusive).
- Net impact = original `gross_amount` - reversal `gross_amount`.
- Query must execute < 4 seconds on XS.

**Query**:
```sql
WITH original_claims AS (
    SELECT 
        claim_id,
        member_id,
        ndc,
        fill_date,
        gross_amount
    FROM FACT_PHARMACY_CLAIMS
    WHERE claim_status = 'adjudicated'
      AND fill_date >= '2023-12-01'
      AND fill_date < '2024-01-01'
),
reversal_claims AS (
    SELECT 
        reversal_claim_id AS original_claim_id,
        gross_amount AS reversal_amount
    FROM FACT_PHARMACY_CLAIMS
    WHERE claim_status = 'reversed'
      AND reversal_claim_id IS NOT NULL
),
net_impact AS (
    SELECT 
        o.member_id,
        o.ndc,
        o.fill_date,
        o.gross_amount - COALESCE(r.reversal_amount, 0) AS net_amount
    FROM original_claims o
    LEFT JOIN reversal_claims r ON o.claim_id = r.original_claim_id
)
SELECT 
    member_id,
    COUNT(*) AS claim_count,
    SUM(net_amount) AS total_net_impact
FROM net_impact
GROUP BY 1
ORDER BY total_net_impact DESC;
```

**Explanation**:
- Predicate on `fill_date` range is SARGable. Snowflake prunes micro-partitions.
- `LEFT JOIN` captures originals without reversals. `COALESCE` handles missing reversals.
- No `TO_DATE()` or `DATE_TRUNC()` in WHERE clause. Preserves pruning.
- Validation: Cross-check 5 reversed claims. Verify net impact matches original minus reversal.
- Edge case: Reversals arriving late. Handled by joining on stable `claim_id`.

**Cost/Performance Note**:
- Avoid `DATE_PART()` on `fill_date`. Breaks pruning.
- `EXPLAIN` shows `Partitions scanned: X of Y` with low ratio. `Predicates pushed down` confirmed.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario5_sargable';`

**Production Note**:
- Reversal logic must handle late-arriving reversals. Stream-based pipelines recommended for real-time.
