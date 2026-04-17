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
