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
