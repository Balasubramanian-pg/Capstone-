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
