## SCENARIO 7: DIR FEE LAG & RETROACTIVE ADJUSTMENTS

**Assumptions**:
- DIR fees apply to `service_month`, not `received_date`.
- Q1 2024 = 2024-01-01 to 2024-03-31.
- Net revenue = gross payer paid - DIR fees.
- Handle missing DIR records as 0.

**Query**:
```sql
WITH q1_claims AS (
    SELECT 
        DATE_TRUNC('MONTH', fill_date) AS service_month,
        pharmacy_npi,
        SUM(payer_paid_amount) AS gross_paid
    FROM FACT_PHARMACY_CLAIMS
    WHERE fill_date BETWEEN '2024-01-01' AND '2024-03-31'
      AND claim_status = 'adjudicated'
    GROUP BY 1, 2
),
dir_adjustments AS (
    SELECT 
        service_month,
        pharmacy_npi,
        SUM(dir_fee_amount) AS total_dir
    FROM FACT_DIR_FEES
    WHERE service_month BETWEEN '2024-01-01' AND '2024-03-31'
    GROUP BY 1, 2
),
net_revenue AS (
    SELECT 
        c.service_month,
        c.pharmacy_npi,
        c.gross_paid,
        COALESCE(d.total_dir, 0) AS dir_adjustment,
        c.gross_paid - COALESCE(d.total_dir, 0) AS net_revenue
    FROM q1_claims c
    LEFT JOIN dir_adjustments d ON c.service_month = d.service_month AND c.pharmacy_npi = d.pharmacy_npi
)
SELECT * FROM net_revenue ORDER BY service_month, net_revenue DESC;
```

**Explanation**:
- Separate aggregations prevent cartesian explosion.
- Join on `service_month` aligns DIR fees to correct period, ignoring `received_date` lag.
- `COALESCE` handles pharmacies with no DIR fees.
- Validation: Cross-check DIR totals against payer remittance files. Verify net revenue matches accounting.
- Edge case: DIR fees arriving in Q2 for Q1 service. Handled by `service_month` filter.

**Cost/Performance Note**:
- Two independent aggregations reduce join size. Snowflake hash join efficient.
- If `FACT_DIR_FEES` large, cluster on `service_month, pharmacy_npi`.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario7_dir_lag';`

**Production Note**:
- DIR fee reconciliation requires audit trail. Log `received_date` vs `service_month` delta.
