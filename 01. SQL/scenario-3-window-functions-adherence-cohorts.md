## SCENARIO 3: WINDOW FUNCTIONS & ADHERENCE COHORTS

**Assumptions**:
- Maintenance drug classes: statins, ACE_inhibitors, ARBs, diabetes_oral.
- Consecutive months = fills in 6 consecutive calendar months.
- PDC = min(days_supply_sum, 180) / 180. Cap at 1.0.
- Overlapping fills: cap days at 180, do not double count.
- Exclude specialty drugs.

**Query**:
```sql
WITH maintenance_claims AS (
    SELECT 
        c.member_id,
        c.fill_date,
        c.days_supply,
        DATE_TRUNC('MONTH', c.fill_date) AS fill_month,
        d.drug_class
    FROM FACT_PHARMACY_CLAIMS c
    JOIN DIM_DRUG_MASTER d ON c.ndc = d.ndc
    WHERE c.claim_status = 'adjudicated'
      AND d.drug_class IN ('statins', 'ACE_inhibitors', 'ARBs', 'diabetes_oral')
      AND d.formulation NOT IN ('injection', 'infusion')
),
monthly_coverage AS (
    SELECT 
        member_id,
        drug_class,
        fill_month,
        SUM(days_supply) AS month_days
    FROM maintenance_claims
    GROUP BY 1, 2, 3
),
consecutive_months AS (
    SELECT 
        member_id,
        drug_class,
        fill_month,
        LAG(fill_month, 5) OVER (PARTITION BY member_id, drug_class ORDER BY fill_month) AS month_6_prior
    FROM monthly_coverage
),
qualifying_members AS (
    SELECT 
        member_id,
        drug_class,
        COUNT(fill_month) AS covered_months
    FROM consecutive_months
    WHERE month_6_prior IS NOT NULL
      AND MONTHS_BETWEEN(fill_month, month_6_prior) = 5
    GROUP BY 1, 2
),
pdc_calc AS (
    SELECT 
        mc.member_id,
        mc.drug_class,
        LEAST(SUM(mc.month_days), 180) / 180.0 AS pdc
    FROM monthly_coverage mc
    JOIN qualifying_members qm ON mc.member_id = qm.member_id AND mc.drug_class = qm.drug_class
    GROUP BY 1, 2
)
SELECT 
    member_id,
    drug_class,
    ROUND(pdc, 4) AS pdc_score,
    CASE WHEN pdc >= 0.80 THEN 'adherent' ELSE 'non_adherent' END AS adherence_status
FROM pdc_calc
ORDER BY pdc DESC;
```

**Explanation**:
- `LAG(fill_month, 5)` checks 5 months prior. `MONTHS_BETWEEN = 5` ensures consecutive months.
- `LEAST(SUM(month_days), 180)` caps overlapping fills at 180 days.
- PDC calculation restricted to qualifying members only.
- Validation: Manual check 3 members. Verify fill months are consecutive. Confirm PDC matches days covered / 180.
- Edge case: Gaps > 30 days break consecutive logic. Handled by month equality check.

**Cost/Performance Note**:
- Window functions scan partitioned set once. Efficient.
- If `FACT_PHARMACY_CLAIMS` lacks clustering on `member_id, fill_date`, consider clustering for adherence queries.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario3_adherence';`

**Production Note**:
- PDC is standard for CMS Star Ratings. Ensure capping at 1.0 aligns with regulatory definitions.
