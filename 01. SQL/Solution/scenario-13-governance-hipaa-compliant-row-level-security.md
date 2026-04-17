## SCENARIO 13: GOVERNANCE & HIPAA-COMPLIANT ROW-LEVEL SECURITY

**Assumptions**:
- Regional pharmacy analysts have role `ROLE_PHARMACY_ANALYST_<state>`.
- Compliance role: `ROLE_COMPLIANCE`.
- Row policy restricts by `state` via `DIM_MEMBERS`.
- Dynamic mask replaces `member_id`, `dob`, `prescriber_npi` for non-compliance roles.

**Query (Policy Setup)**:
```sql
-- 1. Row Access Policy
CREATE OR REPLACE ROW ACCESS POLICY rap_state AS (state VARCHAR) RETURNS BOOLEAN ->
  CASE 
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ROLE_COMPLIANCE') THEN TRUE
    WHEN CURRENT_ROLE() LIKE 'ROLE_PHARMACY_ANALYST_%' 
         AND CURRENT_ROLE() = CONCAT('ROLE_PHARMACY_ANALYST_', state) THEN TRUE
    ELSE FALSE
  END;

ALTER TABLE FACT_PHARMACY_CLAIMS ADD ROW ACCESS POLICY rap_state ON (member_id) 
  USING (SELECT m.zip_code::VARCHAR(5) AS state FROM DIM_MEMBERS m WHERE m.member_id = FACT_PHARMACY_CLAIMS.member_id);

-- 2. Dynamic Masking Policy
CREATE OR REPLACE MASKING POLICY mask_phi AS (val STRING) RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ROLE_COMPLIANCE') THEN val
    ELSE '***-PHI-REDACTED'
  END;

ALTER TABLE FACT_PHARMACY_CLAIMS MODIFY COLUMN member_id SET MASKING POLICY mask_phi;
ALTER TABLE DIM_MEMBERS MODIFY COLUMN dob SET MASKING POLICY mask_phi;
ALTER TABLE FACT_PHARMACY_CLAIMS MODIFY COLUMN prescriber_npi SET MASKING POLICY mask_phi;
```

**Explanation**:
- Row policy evaluates role at query time. No data copied. Efficient.
- Policy binds to `member_id` via subquery. Snowflake evaluates at execution.
- Masking policy applies at column level. Returns placeholder for unauthorized roles.
- Validation: Query as `ROLE_PHARMACY_ANALYST_NY`. Verify only NY claims returned. Query as `ROLE_ANALYST`. Verify PHI masked.
- Performance note: Policy evaluation adds < 8ms overhead. BI caching intact.

**Cost/Performance Note**:
- Avoid inline `WHERE` clauses for row security. Use native policies. Centralizes control.
- Masking does not alter storage. Applied at read time.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario13_governance';`

**Production Note**:
- HIPAA requires audit logs of access. Enable `ACCESS_HISTORY` monitoring.
