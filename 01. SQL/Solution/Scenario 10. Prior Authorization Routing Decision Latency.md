## SCENARIO 10: PRIOR AUTHORIZATION ROUTING & DECISION LATENCY

**Assumptions**:
- Latency = `decision_date - request_date`. Exclude NULL decision dates.
- Median calculated via `PERCENTILE_CONT(0.5)`.
- Group by `drug_class` and `prescriber_specialty`.
- Exclude expired requests.

**Query**:
```sql
WITH pa_valid AS (
    SELECT 
        p.pa_request_id,
        p.request_date,
        p.decision_date,
        p.ndc,
        p.prescriber_npi,
        DATEDIFF(DAY, p.request_date, p.decision_date) AS latency_days
    FROM FACT_PRIOR_AUTHORIZATIONS p
    WHERE p.status IN ('approved', 'denied')
      AND p.decision_date IS NOT NULL
      AND p.request_date BETWEEN '2023-07-01' AND '2023-09-30'
),
joined_data AS (
    SELECT 
        pv.latency_days,
        d.drug_class,
        pr.specialty
    FROM pa_valid pv
    JOIN DIM_DRUG_MASTER d ON pv.ndc = d.ndc
    JOIN DIM_PROVIDERS pr ON pv.prescriber_npi = pr.npi
)
SELECT 
    drug_class,
    COALESCE(specialty, 'unknown') AS prescriber_specialty,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_days) AS median_latency_days,
    COUNT(pa_request_id) AS decision_count
FROM joined_data
GROUP BY 1, 2
ORDER BY median_latency_days DESC;
```

**Explanation**:
- `PERCENTILE_CONT` calculates true median, unaffected by outliers.
- Filter `status IN ('approved', 'denied')` excludes pending/expired.
- Join to drug class and specialty for grouping.
- Validation: Cross-check median against PBM turnaround SLAs. Verify specialty alignment.
- Edge case: Negative latency if dates reversed. Add `WHERE latency_days >= 0`.

**Cost/Performance Note**:
- Window aggregate efficient. Hash grouping optimized.
- If `FACT_PRIOR_AUTHORIZATIONS` large, cluster on `request_date`.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario10_pa_latency';`

**Production Note**:
- PA latency impacts CMS Star Ratings and member satisfaction. Monitor weekly.
