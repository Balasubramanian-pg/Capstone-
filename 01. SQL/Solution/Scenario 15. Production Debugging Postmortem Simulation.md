## SCENARIO 15: PRODUCTION DEBUGGING & POSTMORTEM SIMULATION

**Assumptions**:
- Inherited query uses `WHERE received_date >= CURRENT_DATE - 3`.
- Upstream DIR feeds delayed 19 days. Late fees reference `service_month`, not `received_date`.
- Fix: Filter on `service_month` with ingestion window buffer.

**Diagnosis**:
- Root cause: Date filter relies on `received_date`, which does not capture ingestion delay. Late-arriving fees fall outside window.
- Impact: Dashboard shows $0 DIR adjustments for December 2023. P&L misaligned. Stakeholders escalate.

**Fixed Query**:
```sql
WITH dir_current AS (
    SELECT *
    FROM FACT_DIR_FEES
    WHERE service_month >= DATEADD(MONTH, -3, CURRENT_DATE) - INTERVAL '24 HOUR'
      AND received_date >= CURRENT_DATE - INTERVAL '30 DAY'
)
SELECT 
    service_month,
    pharmacy_npi,
    SUM(dir_fee_amount) AS total_dir,
    COUNT(dir_record_id) AS record_count
FROM dir_current
GROUP BY 1, 2
ORDER BY service_month DESC;
```

**Postmortem Summary**:
- **What Happened**: Date filter assumed `received_date` aligns with ingestion reality. It does not.
- **Why It Happened**: No explicit delay buffer in query logic. No upstream SLA monitoring tied to dashboard refresh.
- **How We Fixed It**: Switched to `service_month` with 24-hour ingestion buffer. Added `received_date` fallback for late files.
- **Prevention Steps**:
  1. Standardize service-month filtering across all financial dashboards.
  2. Add `QUERY_HISTORY` monitoring for row count drops > 90%.
  3. Implement task dependency checks before BI refresh.
  4. Document data latency expectations per source.
- **Validation**: Run query with simulated delayed load. Confirm DIR fees populate. Cross-check with payer remittance.

**Cost/Performance Note**:
- `service_month` filter is SARGable. Pruning works.
- Buffer adds negligible scan overhead. Prevents false empty states.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario15_debug';`

**Production Note**:
- Financial dashboards require resilient date logic. Never rely on single timestamp without ingestion buffer.
