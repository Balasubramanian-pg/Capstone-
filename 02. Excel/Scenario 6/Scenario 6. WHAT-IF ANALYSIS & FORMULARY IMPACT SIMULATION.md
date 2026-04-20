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
