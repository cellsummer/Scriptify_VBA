Option Explicit
Option Base 0

'### CL 2018.02.08: SQL Server Class
'### - Manages the interaction with SQL server

Private pConnected As Boolean   '# Whether SQL server connection is established
Private pExecuted As Boolean    '# Whether an SQL query has been executed successfully
Private pDebugMode As Boolean   '# Whether print debug messages
Private pServerName As String   '# SQL server name
Private pCatalogName As String  '# SQL catalog name

Private pRunlogFile As String   '# Runlog path
Private pSQLCount As Integer    '# Count of SQL query executed

Private conn As ADODB.Connection
Private rs As ADODB.Recordset

'### Functions called at start / end
Private Sub Class_Initialize()
    Set conn = New ADODB.Connection
    Set rs = New ADODB.Recordset
    
    pConnected = False
    pExecuted = False
    pDebugMode = True
    
    '# Define runlog filename
    pRunlogFile = FileFolder(ActiveWorkbook.FullName) & "\sql-" & Format(Now(), "yyyy-mm-dd-hh-mm-ss") & ".log"
    pSQLCount = 0
End Sub
Private Sub Class_Terminate()
    Set conn = Nothing
    Set rs = Nothing
End Sub

'### Property, define and get variables
Public Property Get ServerName() As String
    ServerName = pServerName
End Property
Public Property Let ServerName(ByVal Value As String)
    If pServerName <> Value Then pConnected = False
    pServerName = Value
End Property
Public Property Get CatalogName() As String
    CatalogName = pCatalogName
End Property
Public Property Let CatalogName(ByVal Value As String)
    If pCatalogName <> Value Then pConnected = False
    pCatalogName = Value
End Property
Public Property Get DebugMode() As Boolean
    DebugMode = pDebugMode
End Property
Public Property Let DebugMode(ByVal Value As Boolean)
    pDebugMode = Value
End Property
Public Property Get Results() As ADODB.Recordset
    If Not pExecuted Then
        Err.Raise 9999, , "No query was executed, or the query was executed with error. Refer to the runlog for details."
    Else
        Set Results = rs
    End If
End Property

'### Connect to SQL server
Private Sub ConnectToServer()
    Dim strConn As String, strMessage As String
    Dim cmd As New ADODB.Command
    
    If Not pConnected Then
        If pServerName <> "" And pCatalogName <> "" Then
            '# Use native client instead of OLE DB to improve speed and error logging
            strConn = "PROVIDER=SQLNCLI11; DataTypeCompatibility=80; DATA SOURCE=" & pServerName & "; INITIAL CATALOG=" & pCatalogName & "; INTEGRATED SECURITY=sspi"
            On Error GoTo ErrHandler
            '# Connect
            conn.Open strConn
            '# Set ARITHABORT on to improve performance
            conn.CommandTimeout = 2000
            With cmd
              Set .ActiveConnection = conn
              .CommandType = ADODB.CommandTypeEnum.adCmdText
              .CommandText = "set arithabort on"
              .Execute
            End With
            Set cmd = Nothing
            On Error GoTo 0
            pConnected = True
    
ErrHandler:
            '# Print runlog
            If pDebugMode Then
                If Err.Number <> 0 Then CreateLog 0, "Run-time error '" & Err.Number & "': " & Err.Description
                strMessage = "Connect to server " & pServerName & " with initial catelog " & pCatalogName
                CreateLog 1, strMessage, pConnected
                If Not pConnected Then Err.Raise Err.Number, , Err.Description
            End If
        Else
            Err.Raise 9999, , "Server name and catalog name could not be empty."
        End If
    End If
End Sub

'### Execute query, return execution status
Public Function Execute(ByVal SQLString As String) As Boolean
    Dim x1 As Double, x2 As Double
    
    If Not pConnected Then ConnectToServer
    pExecuted = False
    Set rs = New ADODB.Recordset
    x1 = Timer  '# Runtime check
    On Error GoTo ErrHandler
    With rs
        Set .ActiveConnection = conn
        .Open SQLString
        pExecuted = True
    End With
    On Error GoTo 0

ErrHandler:
    x2 = Timer  '# Runtime check
    '# Print runlog
    If pDebugMode Then
        CreateLog 2, SQLString
        CreateLog 1, "Execute query", pExecuted, x2 - x1
        If Err.Number <> 0 Then CreateLog 0, "Run-time error '" & Err.Number & "': " & Err.Description
    End If
    '# Return execution status
    Execute = pExecuted
End Function

'### Create debug log for reference
'### RunStatus: True = success, False = failed
'### LogType: 0 or omitted = Generic runlog, 1 = Detailed runlog, 2 = SQL query
Private Sub CreateLog(ByVal LogType As Integer, ByVal Message As String, Optional ByVal RunStatus As Boolean = False, Optional ByVal RunTime As Double = -1)
    Dim OutFile As Integer, TimeStamp As String
    
    OutFile = FreeFile()
    TimeStamp = Format(Now, "yyyy-mm-dd hh:mm:ss") & "." & Format(Round((Timer - Int(Timer)) * 1000, 0), "000")
    Open pRunlogFile For Append As #OutFile

    If LogType = 0 Then  '# Generic runlog
        Print #OutFile, TimeStamp & " | " & Message
    ElseIf LogType = 1 Then  '# Runlog with run status and runtime
        Print #OutFile, TimeStamp & " | " & Message & _
                         " | Status: " & IIf(RunStatus, "Success", "Failed") & _
                         IIf(RunTime <> -1, " | Runtime: " & Format(RunTime * 1000, "0.00") & "ms", "")
    ElseIf LogType = 2 Then  '# SQL query log
        pSQLCount = pSQLCount + 1
        Print #OutFile, Message 'TimeStamp & " | SQL query " & pSQLCount & vbNewLine & Message
    End If
    
    Close #OutFile
End Sub

'### File management functions
'# Obtain folder name of a path
Private Function FileFolder(ByVal Path As String) As String
    FileFolder = Left(Path, InStrRev(Path, "\") - 1)
End Function
'# Obtain file extension of a path
Private Function FileExtension(ByVal Path As String) As String
    If InStrRev(Path, ".") = 0 Then
        FileExtension = "N/A"
    Else
        FileExtension = Right(Path, Len(Path) - InStrRev(Path, ".") + 1)
    End If
End Function
'# Obtain filename of a path
Private Function FileName(ByVal Path As String) As String
    FileName = Right(Path, Len(Path) - InStrRev(Path, "\"))
End Function
'# Add suffix to filename
Private Function AddSuffix(ByVal Path As String, ByVal Suffix As String) As String
    Dim Extension As String
    Extension = FileExtension(Path)
    AddSuffix = Left(Path, InStr(Path, Extension) - 1) & Suffix & Extension
End Function
                                                            
                                                            
                                                            '####### To use the class ######'
Sub Test_SQL_Connection()

Dim SQL As New clsSQL

With SQL
        '### Initialize SQL connection
        .ServerName = Range("SQL.Server").Value
        .CatalogName = Range("SQL.Catalog").Value
        .DebugMode = Range("SQL.EnableLog").Value
        .Execute ("select top 10 * from [RUN04_hz]")
         numVars = .Results.Fields.Count
         ReDim vars(1 To numVars) As String
         For j = 1 To numVars
               vars(j) = .Results.Fields(j - 1).Name
         Next j
         Sheets("results").[B2].Resize(1, numVars) = (vars)
         Sheets("results").[B3].CopyFromRecordset .Results
End With


End Sub
