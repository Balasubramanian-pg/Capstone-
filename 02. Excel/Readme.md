# ARTEFACT 1: CONTEXT, PROBLEM STATEMENT, 12 SCENARIOS

## 1. BUSINESS CONTEXT & ECOSYSTEM REALITY

Nexus Pharma Solutions relies heavily on Excel for ad-hoc analytics, formulary impact modeling, DIR fee reconciliation, rebate waterfall tracking, and regional sales reporting. Data flows from Snowflake exports, payer remittance files, manufacturer contract sheets, and internal ERP dumps. Files routinely exceed 500K rows, contain inconsistent NDC formatting, late-arriving claims, unaligned formulary tiers, and unmasked PHI. Analysts share workbooks via email, overwrite versions, trigger circular references, and build dashboards that break when source files shift or refresh timing drifts.

Your mandate: train new joiners to build Excel workbooks that are robust, auditable, HIPAA-compliant, and production-ready. This capstone simulates real-world pharma analytics workflows. Success is not returning correct numbers in a vacuum. Success is delivering workbooks that survive data drift, refresh cycles, stakeholder review, and regulatory audit.

## 2. KNOWN DATA QUALITY & EXCEL REALITIES

1. **Late-Arriving Claims & DIR Fees**: Payer exports arrive 24 to 45 days post-service. `Fill_Date` is backdated. `Received_Date` reflects ingestion.
2. **NDC Supersession Drift**: Old NDCs map to new ones. Historical exports retain legacy NDCs. Manual mapping tables break when formulas hardcode ranges.
3. **Duplicate Adjudications**: Network retries create duplicate claim rows. Excel `Remove Duplicates` often drops valid reversals.
4. **Formulary SCD Misalignment**: Tier changes effective on the 1st, but mid-month claims pull stale values from static lookup sheets.
5. **PII/PHI Exposure Risk**: `Member_ID`, `DOB`, `NPI`, `ZIP` are unmasked in shared sheets. Accidental exports violate HIPAA audit requirements.
6. **Volatile Function Overload**: Workbooks use `INDIRECT`, `OFFSET`, `TODAY()`, `RAND()` across thousands of cells. Recalculation takes 4+ minutes.
7. **File Size & Memory Limits**: `.xlsx` files exceed 250MB. Excel crashes during pivot refresh. Uncompressed images, named ranges, and hidden sheets bloat size.
8. **Version Control Chaos**: Email chains with `_final_v3_REALLY.xlsx`. No change tracking. Broken external links. Formula auditing disabled.

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

## 4. EVALUATION & SUCCESS CRITERIA

| Dimension | Weight | Evaluation Standard |
|---|---|---|
| Logical Correctness | 25% | Outputs match known benchmarks. Edge cases handled explicitly. |
| Performance & Stability | 20% | Recalc under 15s. File size optimized. No crashes or `#CALC!` errors. |
| Maintainability | 20% | Structured tables, named ranges, clear formula documentation, no hardcoding. |
| Data Quality Awareness | 15% | NULL/duplicate/late-arrival handling, validation rules, fallback logic. |
| Production Readiness | 20% | Audit trail, error handling, PHI masking, version control, refresh resilience. |

**Submission Requirements**:
- Each scenario delivered as a working Excel sheet or documented workflow.
- Formulas, DAX, M-code, or VBA explicitly listed.
- Assumptions, trade-offs, and validation steps documented.
- No hard-coded paths or dates unless explicitly required.
- PHI/PII handling explicitly noted where applicable.

---

# ARTEFACT 2: COMPLETE SOLUTIONS, FORMULAS, ASSUMPTIONS, EXPLANATIONS

## SCENARIO 1: POWER QUERY INGESTION & CLEANUP

**Assumptions**:
- Raw CSV contains `Claim_ID`, `NDC`, `Fill_Date`, `Copay`, `Coinsurance`, `Payer_Paid`, `Status`, `Reversal_ID`, `Created_At`.
- NDC must be 11-digit with leading zeros.
- Duplicate adjudications retain the latest `Created_At` per `Claim_ID`.

**Power Query M-Code Steps**:
1. `Source = Csv.Document(File.Contents("path\claims.csv"))`
2. `PromoteHeaders`
3. `ChangeType` to Text/Date/Decimal
4. `TrimNDC = Text.PadStart(Text.Clean([NDC]), 11, "0")`
5. `GroupBy Claim_ID` with `Max(Created_At)` to keep latest
6. `Merge` back to original to filter duplicates
7. `ReplaceValue` on `Copay` and `Coinsurance` with `0` for null
8. `LoadToTable`

**Explanation**:
- `Text.PadStart` normalizes NDC length. `GroupBy` with `Max` preserves valid late-arriving claims while dropping exact duplicates.
- Loading as a table enables dynamic array formulas and Power Pivot integration.
- Validation: Compare row count before/after dedup. Verify `Claim_ID` uniqueness. Spot-check NDC formatting.

**Excel Performance Note**:
- Store source file in same folder as workbook. Use relative paths in PQ. Refresh via `Data > Refresh All`.

---

## SCENARIO 2: FORMULARY TIER MAPPING & LOOKUP ROBUSTNESS

**Assumptions**:
- `Formulary` sheet contains `NDC`, `Tier`, `Eff_Start`, `Eff_End`.
- Claims table has normalized 11-digit NDC.
- Supersession mapping in `NDC_Map` sheet with `Old_NDC`, `New_NDC`.

**Formula**:
```excel
=LET(
  ndc, [@NDC],
  mapped, IFERROR(XLOOKUP(ndc, NDC_Map[Old_NDC], NDC_Map[New_NDC], ndc, 0), ndc),
  tier, XLOOKUP(mapped, Formulary[NDC], Formulary[Tier], "Not on Formulary", 0),
  tier
)
```

**Explanation**:
- `LET` improves readability and prevents repeated NDC lookups.
- `XLOOKUP` handles supersession fallback. Second `XLOOKUP` maps to tier. `0` ensures exact match.
- Validation: Test with known superseded NDCs. Verify "Not on Formulary" appears for unmapped.

**Excel Performance Note**:
- Convert lookup tables to Excel Tables (`Ctrl+T`). Enables dynamic range expansion. Avoid `VLOOKUP` for left-column mismatches.

---

## SCENARIO 3: DYNAMIC ARRAY FILTERING & MULTI-CRITERIA AGGREGATION

**Assumptions**:
- `Claims` table contains `DrugClass`, `PlanType`, `Status`, `Payer_Paid`, `Fill_Date`.
- Q3 2023 = 2023-07-01 to 2023-09-30.
- Exclude `Status = "Reversed"`.

**Formula**:
```excel
=LET(
  q3, FILTER(Claims, (Claims[Fill_Date]>=DATE(2023,7,1))*(Claims[Fill_Date]<DATE(2023,10,1))),
  ma, FILTER(q3, q3[PlanType]="Medicare Advantage"),
  rev, FILTER(ma, ma[Status]<>"Reversed"),
  spend, SUM(rev[Payer_Paid]),
  count, COUNTA(rev[Claim_ID]),
  {"Q3 MA Specialty Spend", spend, "Claim Count", count}
)
```

**Explanation**:
- Boolean arrays multiply to `AND` logic. `FILTER` returns structured array. `LET` caches intermediate steps.
- Output spills automatically. No helper columns needed.
- Validation: Cross-check spend against pivot table. Verify count matches filtered rows.

**Excel Performance Note**:
- `FILTER` recalculates on source change. Keep arrays under 500K rows for smooth performance. Use `.xlsb` if larger.

---

## SCENARIO 4: POWER PIVOT DATA MODELING & RELATIONSHIPS

**Assumptions**:
- Tables: `Claims`, `Members`, `Plans`, `Formulary`, `DIR_Fees`.
- Relationships: `Claims[Member_ID] -> Members[Member_ID]`, `Claims[Plan_ID] -> Plans[Plan_ID]`, `Claims[NDC] -> Formulary[NDC]`, `Claims[Pharmacy_NPI] -> DIR_Fees[Pharmacy_NPI]`.
- Date table marked as date table.

**DAX Measures**:
```dax
Gross Spend = SUM(Claims[Payer_Paid])
Member OOP = SUM(Claims[Copay]) + SUM(Claims[Coinsurance])
Net Revenue = [Gross Spend] - [Member OOP]
DIR Adjustment = SUM(DIR_Fees[DIR_Amount])
Total Net = [Net Revenue] - [DIR Adjustment]
```

**Explanation**:
- Star schema prevents cartesian joins. Measures aggregate across relationships automatically.
- `SUM` respects filters. No need for `CALCULATE` unless time intelligence or cross-filtering required.
- Validation: Build pivot table. Drag measures to Values. Verify totals match flat-file aggregation.

**Excel Performance Note**:
- Disable "Add to Data Model" for unused columns. Reduces memory footprint. Use `.xlsb` for faster load.

---

## SCENARIO 5: ADVANCED DAX & LATE-ADJUSTMENT HANDLING

**Assumptions**:
- `PDC = MIN(Days_Covered, 180) / 180`.
- DIR fees align to `Service_Month`, not `Received_Date`.
- Rolling 3-month window.

**DAX Measures**:
```dax
Days Covered 3M = 
CALCULATE(
  SUM(Claims[Days_Supply]),
  DATESINPERIOD(Dates[Date], MAX(Dates[Date]), -3, MONTH)
)
PDC 3M = DIVIDE(MIN([Days Covered 3M], 180), 180)
DIR Lag Adjusted = 
CALCULATE(
  SUM(DIR_Fees[DIR_Amount]),
  LASTNONBLANK(DIR_Fees[Service_Month], 1),
  DATESINPERIOD(Dates[Date], MAX(Dates[Date]), -1, MONTH)
)
```

**Explanation**:
- `DATESINPERIOD` creates rolling window. `MIN` caps PDC at 1.0.
- `LASTNONBLANK` ensures late DIR fees apply to correct service month.
- Validation: Compare PDC against manual calculation for 3 sample members. Verify DIR lag matches remittance dates.

**Excel Performance Note**:
- Mark `Dates` table as Date Table. Enable `Auto-Date/Time` off in Options. Prevents hidden date tables bloating model.

---

## SCENARIO 6: WHAT-IF ANALYSIS & FORMULARY IMPACT SIMULATION

**Assumptions**:
- Input sheet: `Copay_Shift` (cell `B2`), `Adherence_Rate` (cell `B3`), `Tier_Change_NDCs` (list).
- Output: Revenue impact, adherence projection, break-even threshold.

**Setup**:
1. Link base formula: `=SUMIFS(Claims[Payer_Paid], Claims[NDC], [@NDC]) * (1 - B2) * B3`
2. Data Table: Row input = `B2`, Column input = `B3`
3. Goal Seek: Target cell = `BreakEven_Revenue`, Set to = `0`, By changing = `B3`

**Explanation**:
- Data Tables evaluate formula across grid. Goal Seek solves for unknown variable.
- Validation: Verify table outputs match manual recalcs. Check Goal Seek convergence.

**Excel Performance Note**:
- Set Calculation to Manual before running large Data Tables. Switch to Automatic after. Prevents lag.

---

## SCENARIO 7: LAMBDA & CUSTOM REUSABLE FUNCTIONS

**Assumptions**:
- PDC bands: >=0.80 "Adherent", 0.50-0.79 "Partially", <0.50 "Non".
- Function name: `PDCStatus`.

**Name Manager Entry**:
```excel
=LAMBDA(days_cov, obs_days, threshold,
  LET(
    pct, DIVIDE(days_cov, obs_days, 0),
    IF(pct >= threshold, "Adherent",
      IF(pct >= threshold * 0.625, "Partially", "Non-Adherent"))
  )
)
```

**Usage**:
`=PDCStatus([@Days_Covered], 180, 0.8)`

**Explanation**:
- `LAMBDA` creates portable function. `LET` caches division. `IF` chains enforce bands.
- Validation: Test edge cases (0, 180, 144). Verify text output consistency.

**Excel Performance Note**:
- `LAMBDA` recalculates like standard formulas. Avoid nesting >3 deep. Use `NAME.MANAGER` to document scope.

---

## SCENARIO 8: VBA AUTOMATION & ERROR-RESILIENT REFRESH

**Assumptions**:
- Workbook contains 4 Power Query connections, 3 pivots, 1 summary sheet.
- Export to PDF on `C:\Reports\`.

**VBA Code**:
```vba
Sub RefreshAndExport()
    On Error GoTo CleanUp
    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual
    
    ActiveWorkbook.RefreshAll
    DoEvents
    
    Dim ws As Worksheet
    For Each ws In ThisWorkbook.Sheets
        If ws.Name Like "*Pivot*" Then
            ws.PivotTables(1).RefreshTable
        End If
    Next ws
    
    ThisWorkbook.Sheets("Summary").ExportAsFixedFormat _
        Type:=xlTypePDF, Filename:="C:\Reports\Dir_Summary.pdf", _
        OpenAfterPublish:=False
    
CleanUp:
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    If Err.Number <> 0 Then
        Sheets("Audit").Range("A" & Rows.Count).End(xlUp).Offset(1, 0).Value = Now & " - Error: " & Err.Description
    Else
        Sheets("Audit").Range("A" & Rows.Count).End(xlUp).Offset(1, 0).Value = Now & " - Success"
    End If
End Sub
```

**Explanation**:
- `On Error GoTo` prevents crash. `DoEvents` yields control during refresh. `ExportAsFixedFormat` creates PDF.
- Validation: Test with broken connection. Verify error logs. Confirm PDF output.

**Excel Performance Note**:
- Store macro in `.xlsm`. Sign with digital certificate for enterprise trust. Disable macro warnings via GPO if deployed.

---

## SCENARIO 9: DATA VALIDATION & INPUT CONTROL GOVERNANCE

**Assumptions**:
- Input cells: `NDC`, `Eff_Date`, `Tier`, `Copay`.
- Rules: NDC 11-digit, Date >= TODAY()-30, Tier 1-5, Copay >=0.

**Setup**:
1. `Data > Data Validation > Custom`: `=AND(LEN(A2)=11, ISNUMBER(VALUE(A2)))`
2. `Custom`: `=AND(B2>=TODAY()-30, B2<=TODAY()+30)`
3. `List`: `1,2,3,4,5`
4. `Custom`: `=C2>=0`
5. Sheet Protection: Unlock input cells, lock all others. Enable `Select unlocked cells`.

**Explanation**:
- Validation blocks invalid paste. Sheet protection prevents formula overwrite.
- Validation: Attempt invalid entry. Verify alert. Test protected range edit.

**Excel Performance Note**:
- Avoid `INDIRECT` in validation lists. Use named ranges. Enable `Ignore blank` only when intentional.

---

## SCENARIO 10: VERSION CONTROL, AUDIT TRAIL & DEPENDENCY MAPPING

**Assumptions**:
- Track formula changes, sheet edits, refresh times.
- Map dependencies without add-ins.

**Setup**:
1. Audit sheet: `=NOW()`, `=USER()`, `=CELL("address", A1)`
2. `Formulas > Formula Auditing > Trace Dependents` for key outputs.
3. `=FORMULATEXT(A1)` in adjacent cell for documentation.
4. `Review > Protect Workbook > Structure` to prevent sheet deletion.
5. Save version: `File > Save As > [Date]_[Author]_[Version].xlsb`

**Explanation**:
- `FORMULATEXT` exposes logic for review. Trace arrows map flow. Structure protection locks layout.
- Validation: Edit formula. Check audit log. Verify trace arrows update.

**Excel Performance Note**:
- Disable `AutoSave` in shared workbooks to prevent version conflicts. Use manual check-in/check-out.


## SCENARIO 11: PERFORMANCE OPTIMIZATION & FILE SIZE REDUCTION

**Assumptions**:
- Current file: 320MB, recalc 4.2 mins, uses `INDIRECT`, `OFFSET`, `TODAY()`, unformatted ranges.

**Steps**:
1. Replace `TODAY()` with static date in `Config` sheet. Reference via `$B$1`.
2. Replace `OFFSET(A1,0,0,COUNTA(A:A),1)` with `Claims[NDC]` (table column).
3. Wrap complex calcs in `LET`. Remove duplicate formulas.
4. `File > Options > Advanced > Uncheck Extend data range formats and formulas`.
5. Delete hidden sheets, clear named ranges with errors.
6. `Save As > Excel Binary Workbook (.xlsb)`.

**Explanation**:
- `.xlsb` compresses up to 60%. Removing volatile functions cuts recalc chain. Tables enable efficient memory allocation.
- Validation: Measure recalc time before/after. Check file size. Verify pivot refresh works.

**Excel Performance Note**:
- Turn off `Automatic Background Refresh` for external connections. Schedule refresh during off-hours if possible.

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
