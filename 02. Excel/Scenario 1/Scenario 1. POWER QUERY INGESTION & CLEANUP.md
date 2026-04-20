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
