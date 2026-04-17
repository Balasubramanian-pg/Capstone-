## SCENARIO 9: 340B COMPLIANCE & ELIGIBILITY VALIDATION

**Assumptions**:
- 340B eligibility is time-bound. Match `fill_date` to eligibility window.
- Flag fills where covered entity is active but member not eligible.
- Exclude non-340B pharmacies.

**Query**:
```sql
WITH eligible_fills AS (
    SELECT 
        c.claim_id,
        c.pharmacy_npi,
        c.member_id,
        c.fill_date,
        e.covered_entity_id
    FROM FACT_PHARMACY_CLAIMS c
    JOIN FACT_340B_ELIGIBILITY e 
      ON c.pharmacy_npi = e.covered_entity_id
      AND c.fill_date BETWEEN e.eligibility_start_date AND e.eligibility_end_date
    WHERE c.fill_date >= DATEADD(MONTH, -12, CURRENT_DATE)
      AND c.claim_status = 'adjudicated'
      AND e.is_active = TRUE
),
non_compliant AS (
    SELECT 
        ef.claim_id,
        ef.pharmacy_npi,
        ef.member_id,
        ef.fill_date,
        ef.covered_entity_id
    FROM eligible_fills ef
    LEFT JOIN DIM_MEMBERS m ON ef.member_id = m.member_id
    WHERE m.risk_score IS NULL OR m.eligibility_status != 'active'
)
SELECT 
    covered_entity_id,
    COUNT(DISTINCT claim_id) AS flagged_fills,
    COUNT(DISTINCT member_id) AS flagged_members
FROM non_compliant
GROUP BY 1
ORDER BY flagged_fills DESC;
```

**Explanation**:
- Date overlap logic ensures historical eligibility alignment.
- `LEFT JOIN` to `DIM_MEMBERS` identifies non-eligible members.
- Flagging logic explicit. Returns covered entity level for audit.
- Validation: Cross-check flagged fills against 340B compliance reports. Verify eligibility dates.
- Edge case: Member eligibility changes mid-month. Handled by fill date alignment.

**Cost/Performance Note**:
- Join on `pharmacy_npi` and date range. Snowflake prunes efficiently if clustered.
- If `DIM_MEMBERS` large, filter early. Avoid full scan.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario9_340b';`

**Production Note**:
- 340B compliance triggers HRSA audits. False positives costly. Validate eligibility logic quarterly.
