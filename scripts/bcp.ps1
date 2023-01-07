$server = 'my_server'
$database = 'my_database'
$table = 'my_table'
$csv = 'my_data.csv'
$username = 'my_username'
$password = 'my_password'

# Connect to the database and create the table
$connectionString = "Server=$server;Database=$database;User ID=$username;Password=$password;"
$connection = New-Object System.Data.SqlClient.SqlConnection
$connection.ConnectionString = $connectionString
$connection.Open()
$columns = (Get-Content $csv | Select-Object -First 1).Split(',')
$query = "CREATE TABLE $table ({0})" -f (Join-Object -InputObject $columns -Join ', ')
$command = $connection.CreateCommand()
$command.CommandText = $query
$command.ExecuteNonQuery()

# Use bcp to import the data from the CSV file into the table
bcp "$database.dbo.$table" in "$csv" -S "$server" -d "$database" -U "$username" -P "$password" -c -t,

# Close the connection
$connection.Close()
