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
