## 3. CAPSTONE SCENARIOS (12, INCREMENTAL DIFFICULTY)

Each scenario builds on the previous. Success requires correctness, performance, maintainability, compliance awareness, and business alignment.

### SCENARIO 1: POWER QUERY INGESTION & CLEANUP
**Objective**: Import raw pharmacy claims CSV, normalize NDC to 11-digit, parse dates, remove duplicate adjudications, and load as a structured table.
**Constraints**: Use Power Query only. No VBA. Handle missing `Copay` and `Coinsurance` as 0. Preserve `Reversal_Claim_ID` linkage.
**Success Criteria**: Clean table, correct data types, duplicate logic preserves valid reversals, refreshable connection.
**Known Pitfall**: Using Excel `Remove Duplicates` on entire row drops valid reversals with different timestamps.

### SCENARIO 2: FORMULARY TIER MAPPING & LOOKUP ROBUSTNESS
**Objective**: Map cleaned claims to active formulary tiers using NDC. Handle superseded NDCs via mapping table. Return "Not on Formulary" for missing matches.
**Constraints**: Use modern Excel functions (`XLOOKUP`, `FILTER`, `LET`). Avoid `VLOOKUP`. Handle case sensitivity and trailing spaces.
**Success Criteria**: Accurate tier assignment, fallback handling, dynamic range expansion, zero `#N/A` errors in production view.
**Known Pitfall**: Hardcoded lookup ranges break when formulary sheet expands. `VLOOKUP` fails on left-side NDC mapping.

### SCENARIO 3: DYNAMIC ARRAY FILTERING & MULTI-CRITERIA AGGREGATION
**Objective**: Extract Q3 2023 specialty drug claims for Medicare Advantage plans, group by drug class, and calculate net payer spend.
**Constraints**: Use `FILTER`, `UNIQUE`, `SORT`, `SUMIFS`, `LET`. Exclude reversed claims. Output must auto-expand.
**Success Criteria**: Dynamic spill range, correct aggregation boundaries, automatic refresh on source change, no helper columns.
**Known Pitfall**: `FILTER` returns `#CALC!` when array dimensions mismatch. `SUMIFS` on unstructured ranges ignores dynamic spill.

### SCENARIO 4: POWER PIVOT DATA MODELING & RELATIONSHIPS
**Objective**: Build a star schema linking Claims, Members, Plans, Formulary, and DIR Fees. Create DAX measures for gross spend, member OOP, and net revenue after DIR.
**Constraints**: Use Power Pivot. No flat table joins. Define proper relationships. Mark date table. Handle many-to-many via bridge if needed.
**Success Criteria**: Validated relationships, working measures, correct cardinality, pivot table displays accurate net revenue.
**Known Pitfall**: Joining fact tables directly causes cartesian explosion. Missing date table breaks time intelligence.

### SCENARIO 5: ADVANCED DAX & LATE-ADJUSTMENT HANDLING
**Objective**: Calculate rolling 3-month Proportion of Days Covered (PDC) and adjust for late-arriving DIR fees using `LASTNONBLANK` and `DATESINPERIOD`.
**Constraints**: DAX only. PDC capped at 1.0. DIR fees applied to `Service_Month`, not `Received_Date`. Handle gaps in data.
**Success Criteria**: Accurate PDC trend, correct DIR lag handling, smooth rolling calculation, no blank period distortions.
**Known Pitfall**: `AVERAGE` skews PDC outliers. `DATESBETWEEN` ignores late DIR adjustments. Missing date continuity breaks rolling logic.

### SCENARIO 6: WHAT-IF ANALYSIS & FORMULARY IMPACT SIMULATION
**Objective**: Model financial impact of moving 3 high-spend NDCs from Tier 3 to Tier 2. Simulate copay shifts ($10, $15, $20) and adherence rate changes.
**Constraints**: Use Data Tables, Goal Seek, and Scenario Manager. Link to input sheet. No manual cell edits in output.
**Success Criteria**: Dynamic scenario matrix, break-even adherence threshold identified, clear input/output separation, reproducible results.
**Known Pitfall**: Hardcoding assumptions in pivot formulas breaks simulation. Goal Seek overwrites input cells without backup.

### SCENARIO 7: LAMBDA & CUSTOM REUSABLE FUNCTIONS
**Objective**: Create a custom Excel function `=PDCStatus(days_covered, observation_days, threshold)` that returns "Adherent", "Partially Adherent", or "Non-Adherent" based on regulatory PDC bands.
**Constraints**: Use `LAMBDA` and `LET`. Register via Name Manager. Document parameters. Return consistent text output.
**Success Criteria**: Reusable across sheets, handles NULL/0 gracefully, threshold adjustable, zero macro dependency.
**Known Pitfall**: `LAMBDA` scope limited to workbook. Complex nested logic breaks evaluation order. Missing `LET` optimization causes recalc lag.

### SCENARIO 8: VBA AUTOMATION & ERROR-RESILIENT REFRESH
**Objective**: Build a macro that refreshes all Power Query connections, updates pivots, exports summary to PDF, and logs refresh time/errors to an audit sheet.
**Constraints**: VBA only. Include `On Error GoTo`, `Application.ScreenUpdating = False`, file path validation, and user prompt for overwrite.
**Success Criteria**: Unattended refresh works, broken connections handled gracefully, PDF exports correctly, audit log updates.
**Known Pitfall**: Macro fails silently on broken external link. `DoEvents` missing causes Excel freeze. Hardcoded paths break on shared drive.

### SCENARIO 9: DATA VALIDATION & INPUT CONTROL GOVERNANCE
**Objective**: Build a controlled input sheet for formulary tier changes. Restrict entries to valid NDCs, date ranges, and tier values. Lock formula ranges.
**Constraints**: Use Data Validation, conditional formatting, sheet protection, and custom formulas. Allow only unlocked cells to be edited.
**Success Criteria**: Dropdowns enforce valid inputs, out-of-range entries blocked, protected ranges uneditable, clear error alerts.
**Known Pitfall**: Validation allows paste-over of invalid data. Sheet protection breaks pivot refresh if not configured correctly.

### SCENARIO 10: VERSION CONTROL, AUDIT TRAIL & DEPENDENCY MAPPING
**Objective**: Implement change tracking, formula documentation, and dependency mapping in a shared workbook. Prevent accidental overwrites.
**Constraints**: Use `FORMULATEXT`, `Trace Dependents`, structured audit log, named ranges, and workbook protection. No third-party add-ins.
**Success Criteria**: Clear audit trail, formula changes logged, dependency graph visible, version history maintained, locked structure.
**Known Pitfall**: `FORMULATEXT` breaks on array formulas. Unprotected structure allows sheet deletion. Audit log overwrites on next save.

### SCENARIO 11: PERFORMANCE OPTIMIZATION & FILE SIZE REDUCTION
**Objective**: Reduce a 320MB claims workbook to under 80MB without losing functionality. Eliminate volatile functions, optimize calculation chain, and compress structure.
**Constraints**: Replace `INDIRECT`/`OFFSET`/`TODAY()` with static dates or `LET`. Convert ranges to tables. Disable auto-calc during refresh. Save as `.xlsb`.
**Success Criteria**: Measurable size reduction, recalc time under 15 seconds, no broken references, stable pivot refresh.
**Known Pitfall**: Removing volatile functions breaks dynamic date logic. `.xlsb` conversion loses macro signatures if not re-signed.

### SCENARIO 12: PRODUCTION DEBUGGING & POSTMORTEM SIMULATION
**Objective**: Inherit a broken DIR reconciliation dashboard. It shows $0 adjustments, throws `#REF!` errors, and crashes on refresh. Diagnose, fix, and document.
**Constraints**: Root cause includes broken external links, circular references, stale Power Query cache, and unmasked PHI export. Fix all. Write blameless postmortem.
**Success Criteria**: Dashboard refreshes correctly, errors resolved, PHI masked, audit log added, prevention steps documented.
**Known Pitfall**: Blaming data team instead of designing resilient refresh logic. Skipping error handling causes repeated crashes.
