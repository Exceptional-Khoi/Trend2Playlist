# Chạy các script .sh từ PowerShell trên Windows (dùng bash của Git for Windows, KHÔNG phải WSL).
#   .\scripts\run.ps1 deploy                 # = scripts/deploy.sh (profile lite)
#   .\scripts\run.ps1 deploy -Profile full
#   .\scripts\run.ps1 bootstrap-data
#   .\scripts\run.ps1 update-code
#   .\scripts\run.ps1 test-fault-tolerance kafka
#   .\scripts\run.ps1 test-scalability stream
param(
    [Parameter(Mandatory = $true, Position = 0)][string]$Script,
    [string]$Profile = "lite",
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$Rest
)
$git = (Get-Command git -ErrorAction Stop).Source            # ...\Git\cmd\git.exe
$bash = Join-Path (Split-Path (Split-Path $git)) "bin\bash.exe"
if (-not (Test-Path $bash)) { throw "Không tìm thấy Git Bash ($bash). Cài Git for Windows." }
$env:PROFILE = $Profile
$env:MSYS_NO_PATHCONV = "1"
$path = Join-Path $PSScriptRoot "$Script.sh"
& $bash $path @Rest
exit $LASTEXITCODE
