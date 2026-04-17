## SCENARIO 12: PRODUCTION DEBUGGING & POSTMORTEM SIMULATION

**Assumptions**:
- Broken dashboard shows $0 DIR, `#REF!` errors, crashes on refresh.
- Root causes: broken external link, circular ref, stale PQ cache, unmasked PHI.

**Fix Steps**:
1. `Formulas > Error Checking > Circular Reference`. Resolve by removing self-reference.
2. `Data > Edit Links > Change Source` or `Break Link`.
3. `Power Query > Refresh Preview > Clear Cache`. Reapply.
4. Apply masking: `=IFERROR(IF(ROLE()<>"Compliance","***-REDACTED",[@Member_ID]), "")`
5. Add error logging: `=IF(ISERROR([@Calc]), "Check Source", [@Calc])`
6. Postmortem: Document root cause, fix, prevention (validation rules, refresh logging, masked defaults).

**Explanation**:
- Circular refs break calc chain. Broken links cause `#REF!`. PQ cache stores stale transforms. Masking prevents PHI leaks.
- Validation: Refresh dashboard. Verify DIR populates. Check PHI redaction. Review audit log.

**Excel Performance Note**:
- Never share workbooks with live PHI without masking. Use `Protect Sheet` + `Hide/Unhide` for sensitive ranges. Implement refresh SLA monitoring.

This capstone trains analysts to treat Excel not as a scratchpad, but as an engineering platform. Each scenario builds a muscle: data hygiene, modeling precision, automation resilience, compliance awareness, and production debugging.
