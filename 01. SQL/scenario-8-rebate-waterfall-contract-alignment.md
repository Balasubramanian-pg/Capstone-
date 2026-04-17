## SCENARIO 8: REBATE WATERFALL & CONTRACT ALIGNMENT

**Assumptions**:
- Fixed rebate: `quantity * fixed_per_unit`.
- Percentage: `quantity * unit_acquisition_cost * percentage`. Simplified to `gross_amount * rebate_value`.
- Tiered: Apply `rebate_value` only if `total_quantity >= tier_threshold`.
- Q4 2023 = 2023-10-01 to 2023-12-31.
- Missing contracts = 0.

**Query**:
```sql
WITH q4_claims AS (
    SELECT 
        ndc,
        manufacturer,
        SUM(quantity_dispensed) AS total_quantity,
        SUM(gross_amount) AS total_spend
    FROM FACT_PHARMACY_CLAIMS c
    JOIN DIM_DRUG_MASTER d ON c.ndc = d.ndc
    WHERE c.fill_date BETWEEN '2023-10-01' AND '2023-12-31'
      AND c.claim_status = 'adjudicated'
    GROUP BY 1, 2
),
contract_applied AS (
    SELECT 
        c.ndc,
        c.manufacturer,
        c.total_quantity,
        c.total_spend,
        CASE 
            WHEN rc.rebate_type = 'fixed_per_unit' THEN c.total_quantity * rc.rebate_value
            WHEN rc.rebate_type = 'percentage_of_acq_cost' THEN c.total_spend * rc.rebate_value
            WHEN rc.rebate_type = 'tiered_volume' AND c.total_quantity >= rc.tier_threshold_quantity THEN c.total_quantity * rc.rebate_value
            ELSE 0
        END AS eligible_rebate
    FROM q4_claims c
    LEFT JOIN FACT_REBATE_CONTRACTS rc 
      ON c.ndc = rc.ndc
      AND c.fill_date BETWEEN rc.contract_start_date AND rc.contract_end_date
)
SELECT 
    manufacturer,
    SUM(eligible_rebate) AS total_rebate
FROM contract_applied
GROUP BY 1
ORDER BY total_rebate DESC;
```

**Explanation**:
- Pre-aggregate claims to NDC/manufacturer level. Reduces contract join size.
- `CASE` logic applies contract type correctly. Tiered threshold enforced.
- `LEFT JOIN` ensures NDCs without contracts return 0, not excluded.
- Validation: Cross-check rebate totals against manufacturer statements. Verify tier thresholds applied correctly.
- Edge case: Multiple contracts per NDC. Use `MAX` or explicit priority. Add `ROW_NUMBER()` if needed.

**Cost/Performance Note**:
- Pre-aggregation critical. Avoid joining 12M claims to contracts row-by-row.
- If contracts small, broadcast join efficient.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario8_rebates';`

**Production Note**:
- Rebate waterfall must align with PBM contract terms. Audit log required for compliance.
