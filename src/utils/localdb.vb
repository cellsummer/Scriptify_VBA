
Dim conn As New ADODB.Connection
conn.ConnectionString = "Provider=SQLNCLI11;Server=(LocalDB)\MSSQLLocalDB;Database=myDB;Trusted_Connection=yes;"
conn.Open()

Dim rs As New ADODB.Recordset
rs.Open("SELECT * FROM myTable", conn)

Dim result As String = ""

Do Until rs.EOF
    For i = 0 To rs.Fields.Count - 1
        result += rs.Fields(i).Value & " "
    Next
    result += vbNewLine
    rs.MoveNext()
Loop

conn.Close()
rs.Close()

MsgBox(result)
