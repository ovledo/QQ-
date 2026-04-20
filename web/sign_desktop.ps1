param(
    [string]$PythonExe = "",
    [string]$SignToolPath = "",
    [string]$CertPath = "",
    [SecureString]$CertPassword,
    [string]$TimestampUrl = "http://timestamp.digicert.com"
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
& (Join-Path $scriptDir 'build_desktop.ps1') -SkipBuild -Sign -PythonExe $PythonExe -SignToolPath $SignToolPath -CertPath $CertPath -CertPassword $CertPassword -TimestampUrl $TimestampUrl
exit $LASTEXITCODE
