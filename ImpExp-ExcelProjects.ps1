function Get-MD5Hash($filePath) { 
    $bytes = [IO.File]::ReadAllBytes($filePath)
    $hash = [Security.Cryptography.MD5]::Create().ComputeHash($bytes)
    [BitConverter]::ToString($hash).Replace('-', '').ToLowerInvariant()
}

function Export-ExcelProject {
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Low')]
    param(
        [Parameter(Mandatory = $true, 
            HelpMessage = 'Specifies the path to the Excel Workbook file')]
        [string]$WorkbookPath,
        [Parameter(HelpMessage = 'Specifies export directory')]
        [string]$OutputPath,
        [Parameter(HelpMessage = 'Regular expression pattern identifying modules to be excluded')]
        [string]$Exclude,
        # [Parameter(HelpMessage = 'Export items that may be auto-named, like Class1, Module2, etc.')]
        # [switch]$IncludeAutoNamed = $false,
        [switch]$Force = $false
    )
    
    $mo = Get-ItemProperty -Path HKCU:Software\Microsoft\Office\*\Excel\Security `
        -Name AccessVBOM `
        -ErrorAction SilentlyContinue | `
        ? { !($_.AccessVBOM -eq 0) } | `
        Measure-Object

    if ($mo.Count -eq 0) {
        Write-Warning 'Access to VBA project model may be denied due to security configuration.'
    }

    Write-Verbose 'Starting Excel'
    $xl = New-Object -ComObject Excel.Application -EA Stop
    Write-Verbose "Excel $($xl.Version) started"
    $xl.DisplayAlerts = $false
    $missing = [Type]::Missing
    $extByComponentType = @{ 100 = '.cls'; 1 = '.bas'; 2 = '.cls' }
    $outputPath = ($outputPath, (Get-Item .).FullName)[[String]::IsNullOrEmpty($outputPath)]
    mkdir -ErrorAction Stop -Force $outputPath | Out-Null
    
    try {
        # Open(Filename, [UpdateLinks], [ReadOnly], [Format], [Password], [WriteResPassword], [IgnoreReadOnlyRecommended], [Origin], [Delimiter], [Editable], [Notify], [Converter], [AddToMru], [Local], [CorruptLoad]) 
        Write-Verbose $WorkbookPath
        $wb = $xl.Workbooks.Open($workbookPath, $DisplayLinks -eq $false, $ReadOnly -eq $true) #, $false, $true, `
        #        $missing, $missing, $missing, $missing, $missing, $missing, $missing, $missing, $missing, $missing, $missing, `
        #        $true)
        
        $wb | Get-Member | Out-Null # HACK! Don't know why but next line doesn't work without this
        $project = $wb.VBProject
        
        if ($project -eq $null) {
            Write-Verbose 'No VBA project found in workbook'
        }
        else {
            $tempFilePath = [IO.Path]::GetTempFileName()

            $vbcomps = $project.VBComponents

            # Filter out components that matches the $Exclude pattern
            if (![String]::IsNullOrEmpty($exclude)) {
                $verbose = ($PSCmdlet.MyInvocation.BoundParameters.ContainsKey('Verbose') -and $PSCmdlet.MyInvocation.BoundParameters['Verbose'].IsPresent)
                if ($verbose) {
                    $vbcomps | ? { $_.Name -match $exclude } | % { Write-Verbose "$($_.Name) will be excluded" }
                }
                $vbcomps = $vbcomps | ? { $_.Name -notmatch $exclude }
            }

            $vbcomps | % `
            { 
                $vbcomp = $_
                $name = $vbcomp.Name
                $ext = $extByComponentType[$vbcomp.Type]
                $cntLines = $vbcomp.CodeModule.CountOfLines
                if ($ext -eq $null) {
                    Write-Verbose "Skipped component: $($name)"
                }
                #elseif (!$includeAutoNamed -and $name -match '^(Form|Module|Class|Sheet)[0-9]+$') {
                # Don't export empty modules
                elseif ($cntLines -eq 0) {
                    Write-Verbose "Skipped empty component: $name"
                }
                else {
                    $vbcomp.Export($tempFilePath)
                    
                    $exportedFilePath = Join-Path $outputPath "$name$ext"
                    $exists = Test-Path $exportedFilePath -PathType Leaf
                    
                    if ($exists) { 
                        $oldHash = Get-MD5Hash $exportedFilePath 
                        $newHash = Get-MD5Hash $tempFilePath
                        $changed = !($oldHash -eq $newHash)
                        $status = ('Unchanged', 'Conflict', 'Unchanged', 'Changed')[[int]$changed + (2 * [int]$force.IsPresent)]
                    }
                    else {
                        $status = 'New'
                    }

                    if (($status -eq 'Changed' -or $status -eq 'New') `
                            -and $pscmdlet.ShouldProcess($name)) {
                        Move-Item -Force $tempFilePath $exportedFilePath
                    }
                    # Return exported info 
                    $rec = [ordered]@{
                        Action = 'Export';
                        Name   = $name;
                        Status = $status;
                        File   = (Get-Item $exportedFilePath -ErrorAction Stop);
                    }
                    New-Object PSObject -Property $rec 
                }
            }        
        }
        $wb.Close($false, $missing, $missing)
    }
    finally {    
        $xl.Quit()
        # http://technet.microsoft.com/en-us/library/ff730962.aspx
        [Runtime.InteropServices.Marshal]::ReleaseComObject([System.__ComObject]$xl) | Out-Null
        [GC]::Collect()
        [GC]::WaitForPendingFinalizers()
    }
}

function Import-ExcelProject {
    [CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Low')]
    param(
        [Parameter(Mandatory = $true, 
            HelpMessage = 'Specifies the path to the Excel Workbook file')]
        [string]$WorkbookPath,
        [Parameter(Mandatory = $true, HelpMessage = 'Specifies code directory')]
        [string]$CodeFolder,
        [Parameter(HelpMessage = 'Regular expression pattern identifying files to be excluded')]
        [string]$Exclude
    )

    $ModuleExists = @{}
    Get-ChildItem -LiteralPath $CodeFolder | % `
    {
        $fileName = $_.Name
        $ModuleExists[$fileName] = $false
    }

    $mo = Get-ItemProperty -Path HKCU:Software\Microsoft\Office\*\Excel\Security `
        -Name AccessVBOM `
        -ErrorAction SilentlyContinue | `
        ? { !($_.AccessVBOM -eq 0) } | `
        Measure-Object

    if ($mo.Count -eq 0) {
        Write-Warning 'Access to VBA project model may be denied due to security configuration.'
    }

    Write-Verbose 'Starting Excel'
    $xl = New-Object -ComObject Excel.Application -ErrorAction Stop
    Write-Verbose "Excel $($xl.Version) started"
    $xl.DisplayAlerts = $false
    $extByComponentType = @{ 100 = '.cls'; 1 = '.bas'; 2 = '.cls' }
    
    try {
        # Open(Filename, [UpdateLinks], [ReadOnly], [Format], [Password], [WriteResPassword], [IgnoreReadOnlyRecommended], [Origin], [Delimiter], [Editable], [Notify], [Converter], [AddToMru], [Local], [CorruptLoad]) 
        Write-Verbose "opening $WorkbookPath"
        $wb = $xl.Workbooks.Open($workbookPath, $UpdateLinks -eq $false, $ReadOnly -eq $false) #, $false, $true, `
        #        $missing, $missing, $missing, $missing, $missing, $missing, $missing, $missing, $missing, $missing, $missing, `
        #        $true)
        
        $wb | Get-Member | Out-Null # HACK! Don't know why but next line doesn't work without this
        $project = $wb.VBProject
        
        if ($project -eq $null) {
            Write-Verbose 'No VBA project found in workbook'
        }
        else {
            $vbcomps = $project.VBComponents
            $vbcomps | % `
            { 
                $vbcomp = $_
                $name = $vbcomp.Name
                $ext = $extByComponentType[$vbcomp.Type]

                $exists = Test-Path "$CodeFolder/$name$ext"
                if ($exists) {
                    $ModuleExists["$name$ext"] = $true
                }
            }        

            # Adding new module/class to the project
            Get-ChildItem -LiteralPath $CodeFolder | % `
            {
                $fileName = $_.Name 
                $moduleName = $_.BaseName
                
                if (![String]::IsNullOrEmpty($Exclude) -and $fileName -match $Exclude) {
                    Write-Verbose "Excluded: $fileName"
                    $status = 'Excluded'
                }
                
                elseif ($ModuleExists[$fileName]) {
                    Write-Verbose "Removed the existing module $ModuleName..."
                    $vbcomps.remove($vbcomps[$moduleName])
                    Write-Verbose "Repalcing with the new code in $fileName..."
                    try {
                        $token = $vbcomps.import("$CodeFolder/$fileName") 
                        $status = 'Imported'
                    }
                    catch {
                        Write-Verbose "$fileName can't be imported: invalid file."
                        $status = 'Failed'
                    }
                }

                else {
                    Write-Verbose "Importing new component: $fileName"
                    try {
                        $token = $vbcomps.import("$CodeFolder/$fileName")
                        $status = 'Imported'
                    }
                    catch {
                        Write-Verbose "$fileName can't be imported: invalid file."
                        $status = 'Failed'
                    }
                }
                $rec = [ordered]@{
                    Action = 'Import'; 
                    Name   = $moduleName; 
                    Status = $status; 
                    File   = "$CodeFolder\$fileName"
                }
                New-Object psobject -Property $rec
            }
        }
        $wb.Save()
        $wb.Close()
    }
    finally {    
        $xl.Quit()
        # http://technet.microsoft.com/en-us/library/ff730962.aspx
        [Runtime.InteropServices.Marshal]::ReleaseComObject([System.__ComObject]$xl) | Out-Null
        [GC]::Collect()
        [GC]::WaitForPendingFinalizers()
    }
}

<#
$export = Export-ExcelProject C:\Users\cells\Documents\Github\excel-games\test.xlsb -Output C:\Users\cells\Documents\Github\excel-games\vbaCode -Verbose
$import = Import-ExcelProject C:\Users\cells\Documents\Github\excel-games\test.xlsb -CodeFolder C:\Users\cells\Documents\Github\excel-games\vbaCode -Verbose 

echo $export
echo $import
#>