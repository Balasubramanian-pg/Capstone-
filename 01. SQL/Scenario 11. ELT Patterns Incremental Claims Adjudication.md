## SCENARIO 11: ELT PATTERNS & INCREMENTAL CLAIMS ADJUDICATION

**Assumptions**:
- Stream captures `INSERT` and `UPDATE` on `FACT_PHARMACY_CLAIMS`.
- Task runs hourly.
- Target: `FACT_DAILY_PLAN_SUMMARY` with `(fill_date, plan_id, net_receipts, claim_count)`.
- Idempotent: `MERGE` handles updates and inserts.

**Query (Stream & Task Setup)**:
```sql
-- 1. Create Stream
CREATE OR REPLACE STREAM claim_stream ON TABLE FACT_PHARMACY_CLAIMS 
  APPEND_ONLY = FALSE 
  SHOW_INITIAL_ROWS = TRUE;

-- 2. Create Target Table
CREATE OR REPLACE TABLE FACT_DAILY_PLAN_SUMMARY (
    fill_date DATE,
    plan_id VARCHAR,
    net_receipts DECIMAL(12,2),
    claim_count INTEGER,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Task Definition
CREATE OR REPLACE TASK daily_plan_agg
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'
WHEN
  SYSTEM$STREAM_HAS_DATA('claim_stream')
AS
  MERGE INTO FACT_DAILY_PLAN_SUMMARY tgt
  USING (
    SELECT 
      fill_date,
      plan_id,
      SUM(payer_paid_amount) AS net_receipts,
      COUNT(claim_id) AS claim_count
    FROM claim_stream
    WHERE METADATA$ACTION = 'INSERT'
      AND claim_status = 'adjudicated'
    GROUP BY 1, 2
  ) src
  ON tgt.fill_date = src.fill_date AND tgt.plan_id = src.plan_id
  WHEN MATCHED THEN UPDATE SET 
    net_receipts = tgt.net_receipts + src.net_receipts,
    claim_count = tgt.claim_count + src.claim_count,
    last_updated = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (fill_date, plan_id, net_receipts, claim_count, last_updated)
    VALUES (src.fill_date, src.plan_id, src.net_receipts, src.claim_count, CURRENT_TIMESTAMP());

-- 4. Resume Task
ALTER TASK daily_plan_agg RESUME;
```

**Explanation**:
- `APPEND_ONLY = FALSE` captures updates. `SHOW_INITIAL_ROWS` loads existing data.
- `WHEN SYSTEM$STREAM_HAS_DATA()` prevents empty runs.
- `MERGE` handles idempotency. Late arrivals update existing rows.
- Validation: Check stream offset. Verify `last_updated` increments. Test with manual `INSERT`.
- Error handling: Add task error notification via `NOTIFICATION_INTEGRATION`. Monitor `TASK_HISTORY()`.

**Cost/Performance Note**:
- Stream scans only changed rows. Efficient.
- Task runs on XS warehouse. Auto-suspend after completion.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario11_elt';`

**Production Note**:
- Stream offset tracking critical. Monitor for drift. Reinitialize if corrupted.
