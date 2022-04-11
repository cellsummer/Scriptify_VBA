# Powershell  user  interface

# Default variables
$server = 'SERVERNAMED01'

# Arguments
# variable name
$argument_name = 'server'
# input prompt
$prompt = '>>Select results SQL server: '

# Selction choices
# first element of the array goes to the actual variable
# the rest of the elements are descriptive
$p = [ordered]@{
    '1' = 'SERVERNAMED01', 'Dev Server(Default)';
    '2' = 'SERVERNAMET01', 'Test Server'
}
# Output confirmation message
$confirm_msg = 'Selected server is: '

# function to generate a selection menu
function Get-Menu(){
    param(
        [string] $var_name,
        [string] $prompt,
        $selection,
        [string] $msg
         )

    foreach ($k in $selection.keys) {
        $v = $($selection[$k])[0]
        $v_info = [String]::Join('|', $selection[$k])
        $prompt = "$prompt`n$k`: $v_info"
    }

    $selected = read-host $prompt`n
    # if user enters nothing, always return the first element of selection
    if ([String]::IsNullOrEmpty($selected)){
        $selected=$($selection.keys)[0]
    }

    if (-not $selection.Contains($selected)){
        echo "Invalid selection choice $selected. Aborted."
        exit 1
    }

    write-host "$msg$($selection[$selected])" -ForegroundColor DarkGreen

    return $($selection[$selected])[0]
}

$server = Get-Menu $argument_name $prompt $p $confirm_msg
echo $server
