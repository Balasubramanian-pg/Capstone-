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
