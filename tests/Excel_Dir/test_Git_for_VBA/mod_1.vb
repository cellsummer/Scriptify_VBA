Sub showScripts()

Dim CodeCopy As VBIDE.CodeModule
Dim CodePaste As VBIDE.CodeModule
Dim numLines As Integer


For Each elm In ActiveWorkbook.VBProject.VBComponents
    num_of_lines = elm.CodeModule.CountOfLines
    If num_of_lines > 0 Then
        Script = elm.CodeModule.Lines(1, num_of_lines)
        Debug.Print Script
        Debug.Print elm.Name
    End If
Next elm




End Sub