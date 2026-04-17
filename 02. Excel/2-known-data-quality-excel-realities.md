## 2. KNOWN DATA QUALITY & EXCEL REALITIES

1. **Late-Arriving Claims & DIR Fees**: Payer exports arrive 24 to 45 days post-service. `Fill_Date` is backdated. `Received_Date` reflects ingestion.
2. **NDC Supersession Drift**: Old NDCs map to new ones. Historical exports retain legacy NDCs. Manual mapping tables break when formulas hardcode ranges.
3. **Duplicate Adjudications**: Network retries create duplicate claim rows. Excel `Remove Duplicates` often drops valid reversals.
4. **Formulary SCD Misalignment**: Tier changes effective on the 1st, but mid-month claims pull stale values from static lookup sheets.
5. **PII/PHI Exposure Risk**: `Member_ID`, `DOB`, `NPI`, `ZIP` are unmasked in shared sheets. Accidental exports violate HIPAA audit requirements.
6. **Volatile Function Overload**: Workbooks use `INDIRECT`, `OFFSET`, `TODAY()`, `RAND()` across thousands of cells. Recalculation takes 4+ minutes.
7. **File Size & Memory Limits**: `.xlsx` files exceed 250MB. Excel crashes during pivot refresh. Uncompressed images, named ranges, and hidden sheets bloat size.
8. **Version Control Chaos**: Email chains with `_final_v3_REALLY.xlsx`. No change tracking. Broken external links. Formula auditing disabled.
