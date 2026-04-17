## SCENARIO 4: SLOWLY CHANGING DIMENSIONS & FORMULARY ALIGNMENT

**Assumptions**:
- `DIM_FORMULARY` uses Type 2 SCD with `effective_start_date`, `effective_end_date`, `is_current`.
- Match `fill_date` to tier active at that time.
- Q2 2023 = 2023-04-01 to 2023-06-30.
- Exclude tier = 'excluded'.

**Query**:
```sql
WITH q2_claims AS (
    SELECT 
        claim_id,
        ndc,
        fill_date,
        payer_paid_amount
    FROM FACT_PHARMACY_CLAIMS
    WHERE fill_date BETWEEN '2023-04-01' AND '2023-06-30'
      AND claim_status != 'reversed'
),
historical_mapping AS (
    SELECT 
        c.claim_id,
        c.ndc,
        c.fill_date,
        c.payer_paid_amount,
        f.tier
    FROM q2_claims c
    JOIN DIM_FORMULARY f 
      ON c.ndc = f.ndc
      AND c.fill_date >= f.effective_start_date
      AND c.fill_date < f.effective_end_date
      AND f.tier != 'excluded'
),
tier_aggregation AS (
    SELECT 
        tier,
        COUNT(DISTINCT claim_id) AS claim_count,
        SUM(payer_paid_amount) AS total_paid
    FROM historical_mapping
    GROUP BY 1
)
SELECT * FROM tier_aggregation ORDER BY total_paid DESC;
```

**Explanation**:
- Date overlap logic: `fill_date >= start AND fill_date < end`. Handles Type 2 SCD correctly.
- `q2_claims` CTE isolates relevant facts early.
- `COUNT(DISTINCT claim_id)` prevents inflation if multiple line items map to same tier.
- Validation: Spot-check NDC with known tier change in May 2023. Verify spend splits correctly across tiers.
- Data quality check: Add `WHERE f.effective_end_date > f.effective_start_date` to catch malformed SCDs.

**Cost/Performance Note**:
- SCD joins are range joins. Snowflake handles efficiently if `ndc` filtered first.
- If `DIM_FORMULARY` small, broadcast join occurs automatically.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario4_scd';`

**Production Note**:
- Formulary alignment is critical for contract reporting. Misalignment triggers payer disputes.
