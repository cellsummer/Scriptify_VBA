Function ExecuteSQL_Ado(byVal sql as String, byVal server as String, byVal db as String = "impexp") as Integer
  '''Execute sql command for local db
  '''Args:
  '     cmd (str): sql command.
  '     server (str): server name; if it is local db, use (localdb)\instance_name 
  '     db (str): database name. default to impexp
  '''Returns:
  '     0: Successful; -1: Fail
  '''
  Try
      Dim conn as Object
      conn = CreateObject("ADODB.Connection")
      conn.ConnectionString = String.Format(
       "Provider=SQLNCLI11;Server={0};Database={1};Trusted_Connection=yes;", 
       server,
       db
      )
      conn.Open()

      Dim cmd as Object
      cmd = CreateObject("ADODB.Command")
      cmd.ActiveConnection = conn
      cmd.CommandText = sql
      cmd.CommandType = ADODB.CommandTypeEnum.adCmdText
      cmd.Execute()

      conn.Close()

      Return 0
  Catch ex As Exception
      Return -1
  End Try

End Function

Public Function ExecuteSelectSQL(ByVal sql As String) As DataTable
    Try
        Dim conn As New ADODB.Connection
        conn.ConnectionString = "Provider=SQLNCLI11;Server=(LocalDB)\MSSQLLocalDB;Database=myDB;Trusted_Connection=yes;"
        conn.Open()

        Dim cmd As New ADODB.Command
        cmd.ActiveConnection = conn
        cmd.CommandText = sql
        cmd.CommandType = ADODB.CommandTypeEnum.adCmdText

        Dim rs As ADODB.Recordset = cmd.Execute()

        Dim dt As New DataTable()
        For i As Integer = 0 To rs.Fields.Count - 1
            dt.Columns.Add(rs.Fields(i).Name)
        Next

        While Not rs.EOF
            Dim dr As DataRow = dt.NewRow()
            For i As Integer = 0 To rs.Fields.Count - 1
                If rs.Fields(i).Value Is DBNull.Value Then
                    dr(i) = DBNull.Value
                Else
                    dr(i) = rs.Fields(i).Value
                End If
            Next
            dt.Rows.Add(dr)
            rs.MoveNext()
        End While

        conn.Close()
        rs.Close()
        Return dt
    Catch ex As Exception
        Return Nothing
    End Try
End Function

Public Function ExecuteSQL(ByVal sql As String) As Integer
    Try
        Dim conn As New ADODB.Connection
        conn.ConnectionString = "Provider=SQLNCLI11;Server=(LocalDB)\MSSQLLocalDB;Database=myDB;Trusted_Connection=yes;"
        conn.Open()

        Dim cmd As New ADODB.Command
        cmd.ActiveConnection = conn
        cmd.CommandText = sql
        cmd.CommandType = ADODB.CommandTypeEnum.adCmdText
        cmd.Execute()

        conn.Close()

        Return 0
    Catch ex As Exception
        Return -1
    End Try
End Function

' Examples for dealing with DataTable Object
' Iterate through all the rows in the DataTable and print the values of each column
For Each row As DataRow In dt.Rows
    For Each col As DataColumn In dt.Columns
        Console.Write(row(col) & " ")
    Next
    Console.WriteLine()
Next

' Read a specific cell
Console.WriteLine(dt.Rows(0)(1))

'Use the DataTable.Select() method to filter the rows and return an array of DataRow objects that match the specified filter:

Dim filteredRows() As DataRow = dt.Select("columnName = 'value'")
For Each row As DataRow In filteredRows
    For Each col As DataColumn In dt.Columns
        Console.Write(row(col) & " ")
    Next
    Console.WriteLine()
Next

'Use DataTable.AsEnumerable() method to get an IEnumerable of DataRow objects, allowing you to use Linq to query the DataTable

Dim query = From row in dt.AsEnumerable()
            Where row.Field(Of String)("columnName") = "value"
            Select row
For Each row As DataRow In query
    For Each col As DataColumn In dt.Columns
        Console.Write(row(col) & " ")
    Next
    Console.WriteLine()
Next

Public Function ExecuteSQL(ByVal sql As String) As DataTable
    Try
        Dim conn As New SqlClient.SqlConnection("Data Source=(LocalDB)\MSSQLLocalDB;Initial Catalog=myDB;Integrated Security=True")
        conn.Open()

        Dim cmd As New SqlClient.SqlCommand(sql, conn)
        cmd.CommandType = CommandType.Text
        Dim adapter As New SqlClient.SqlDataAdapter(cmd)
        Dim dt As New DataTable()
        adapter.Fill(dt)

        conn.Close()
        For Each col As DataColumn In dt.Columns
            col.AllowDBNull = True
        Next
        Return dt
    Catch ex As Exception
        Return Nothing
    End Try
End Function

Public Function ExecuteSQL(ByVal sql As String) As Integer
    Try
        Dim conn As New SqlClient.SqlConnection("Data Source=(LocalDB)\MSSQLLocalDB;Initial Catalog=myDB;Integrated Security=True")
        conn.Open()

        Dim cmd As New SqlClient.SqlCommand(sql, conn)
        cmd.CommandType = CommandType.Text
        cmd.ExecuteNonQuery()

        conn.Close()
        Return 0
    Catch ex As Exception
        Return -1
    End Try
End Function
