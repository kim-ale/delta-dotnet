#!/usr/bin/env pwsh
#Requires -Version 7.0

<#
.SYNOPSIS
    Verify the packaged kernel host exports.
.DESCRIPTION
    Requires the feature-enabled upstream imports and header-provider functions
    in the selected native artifact before packaging it.
.PARAMETER LibraryPath
    Native library to inspect with dumpbin on Windows or nm on Unix.
.PARAMETER InteropPath
    Generated managed declarations defining the upstream import inventory.
.PARAMETER InspectorPath
    Optional override for the platform symbol inspector.
.EXAMPLE
    ./.scripts/Test-KernelHostExports.ps1 -LibraryPath kernel-artifact/delta_kernel_ffi.dll
.NOTES
    Runs after staging each native kernel artifact in package CI.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$LibraryPath,

    [Parameter(Mandatory = $false)]
    [string]$InteropPath = (Join-Path $PSScriptRoot '../src/DeltaLake/Kernel/Interop/Interop.cs'),

    [Parameter(Mandatory = $false)]
    [string]$InspectorPath
)

$ErrorActionPreference = 'Stop'

function Test-KernelHostExport {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory = $true)][string]$Library,
        [Parameter(Mandatory = $true)][string]$Interop,
        [Parameter(Mandatory = $false)][string]$Inspector
    )

    $Library = (Resolve-Path $Library).Path
    $Declarations = [System.IO.File]::ReadAllText((Resolve-Path $Interop).Path)
    $Pattern = '(?m)^\s*public static extern[^\r\n]*?\s+(\w+)\('
    $Imports = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($Match in [regex]::Matches($Declarations, $Pattern)) {
        $null = $Imports.Add($Match.Groups[1].Value)
    }
    if ($Imports.Count -ne 197) {
        throw 'Kernel import inventory changed; review its feature gates.'
    }
    $Disabled = @(
        'enable_event_tracing', 'enable_log_line_tracing', 'enable_formatted_log_line_tracing',
        'get_uc_commit_client', 'free_uc_commit_client', 'get_uc_committer', 'free_uc_committer',
        'get_testing_kernel_expression', 'get_testing_kernel_predicate',
        'get_simple_testing_kernel_expression', 'get_simple_testing_kernel_predicate',
        'expressions_are_equal', 'predicates_are_equal'
    )
    foreach ($Name in $Disabled) {
        $null = $Imports.Remove($Name)
    }
    foreach ($Name in @('kernel_headers_register', 'kernel_headers_unregister', 'kernel_headers_complete',
            'kernel_engine_with_headers', 'DELTA_DOTNET_KERNEL_EXPORT_ROOTS')) {
        $null = $Imports.Add($Name)
    }
    $Exports = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    if ($IsWindows) {
        if (-not $Inspector) {
            $Inspector = (Get-Command dumpbin.exe -ErrorAction SilentlyContinue).Source
            if (-not $Inspector) {
                $VsWhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio/Installer/vswhere.exe'
                $Inspector = @(& $VsWhere -latest -products '*' -find 'VC/Tools/MSVC/**/bin/Hostx64/x64/dumpbin.exe')[0]
            }
            if (-not $Inspector) { throw 'Cannot find dumpbin.' }
        }
        $Output = @(& $Inspector /nologo /exports $Library)
        if ($LASTEXITCODE -ne 0) { throw 'Export inspection failed.' }
        foreach ($Line in $Output) {
            if ($Line -match '^\s*\d+\s+[0-9A-Fa-f]+\s+([0-9A-Fa-f]+)\s+(\w+)(.*)$') {
                if ([Convert]::ToUInt64($Matches[1], 16) -eq 0) { throw 'Zero export address.' }
                $Name = $Matches[2]
                $Suffix = $Matches[3].Trim()
                if ($Suffix -match '(?i)forwarded') { throw "Forwarded export: $Name" }
                $null = $Exports.Add($Name)
            }
        }
    }
    else {
        if (-not $Inspector) { $Inspector = (Get-Command nm -ErrorAction Stop).Source }
        $Arguments = if ($IsMacOS) { @('-gjU', $Library) } else { @('-D', '--defined-only', '--format=posix', $Library) }
        $Output = @(& $Inspector @Arguments)
        if ($LASTEXITCODE -ne 0) { throw 'Export inspection failed.' }
        foreach ($Line in $Output) {
            $Name = ($Line.Trim() -split '\s+')[0]
            if ($IsMacOS -and $Name.StartsWith('_')) { $Name = $Name.Substring(1) }
            if ($Name) { $null = $Exports.Add($Name) }
        }
    }
    $Missing = @($Imports | Where-Object { -not $Exports.Contains($_) } | Sort-Object)
    if ($Missing.Count -ne 0) { throw "Missing kernel host exports: $($Missing -join ', ')" }
    "PASS $Library : $($Imports.Count) required exports"
}

if ($MyInvocation.InvocationName -ne '.') {
    try {
        Test-KernelHostExport -Library $LibraryPath -Interop $InteropPath -Inspector $InspectorPath
        exit 0
    }
    catch {
        Write-Error -ErrorAction Continue $_
        exit 1
    }
}