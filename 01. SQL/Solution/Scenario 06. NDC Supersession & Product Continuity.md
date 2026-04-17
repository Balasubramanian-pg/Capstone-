## SCENARIO 6: NDC SUPERSESSION & PRODUCT CONTINUITY

**Assumptions**:
- Supersession may chain: A->B, B->C. Resolve to final active NDC.
- Use recursive CTE or iterative mapping. Snowflake supports recursive CTEs.
- Exclude discontinued drugs with no path.
- 2023 claims mapped to current GPI.

**Query**:
```sql
WITH supersession_chain AS (
    SELECT ndc_old, ndc_new, supersession_date
    FROM DIM_NDC_SUPERSESSION
    WHERE supersession_reason != 'discontinued'
),
resolved_ndc AS (
    SELECT ndc AS current_ndc, ndc AS root_ndc
    FROM DIM_DRUG_MASTER
    UNION ALL
    SELECT sc.ndc_new, rc.root_ndc
    FROM supersession_chain sc
    JOIN resolved_ndc rc ON sc.ndc_old = rc.current_ndc
),
final_mapping AS (
    SELECT root_ndc AS original_ndc, MAX(current_ndc) AS active_ndc
    FROM resolved_ndc
    GROUP BY 1
),
claims_mapped AS (
    SELECT 
        c.claim_id,
        c.quantity_dispensed,
        COALESCE(fm.active_ndc, c.ndc) AS mapped_ndc,
        d.gpi
    FROM FACT_PHARMACY_CLAIMS c
    LEFT JOIN final_mapping fm ON c.ndc = fm.original_ndc
    LEFT JOIN DIM_DRUG_MASTER d ON COALESCE(fm.active_ndc, c.ndc) = d.ndc
    WHERE c.fill_date BETWEEN '2023-01-01' AND '2023-12-31'
      AND c.claim_status = 'adjudicated'
)
SELECT 
    gpi,
    COUNT(DISTINCT claim_id) AS claim_count,
    SUM(quantity_dispensed) AS total_quantity
FROM claims_mapped
GROUP BY 1
ORDER BY total_quantity DESC;
```

**Explanation**:
- Recursive CTE resolves chains. `MAX(current_ndc)` captures final active NDC.
- `COALESCE` handles drugs without supersession path.
- Join to `DIM_DRUG_MASTER` uses resolved NDC for GPI lookup.
- Validation: Spot-check chained NDCs. Verify GPI grouping matches manufacturer catalog.
- Edge case: Circular mappings prevented by `supersession_reason != 'discontinued'`.

**Cost/Performance Note**:
- Recursive CTEs can be heavy. Limit depth with `MAXRECURSION` if needed.
- If supersession table small, broadcast join efficient.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario6_ndc_mapping';`

**Production Note**:
- NDC mapping must align with FDA Orange Book updates. Schedule quarterly refresh.
