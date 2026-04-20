param(
    [switch]$Sign,
    [switch]$SkipBuild,
    [switch]$SkipLauncherBuild,
    [switch]$SkipClean,
    [switch]$CheckOnly,
    [string]$LauncherOutputPath = "",
    [string]$PythonExe = "",
    [string]$SignToolPath = "",
    [string]$CertPath = "",
    [SecureString]$CertPassword,
    [string]$TimestampUrl = "http://timestamp.digicert.com"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::UTF8

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BuildSpec = Join-Path $ScriptDir 'build.spec'
$LauncherSpec = Join-Path $ScriptDir 'launcher.spec'
$VersionInfo = Join-Path $ScriptDir 'version_info.txt'
$DistDir = Join-Path $ScriptDir 'dist'
$BuildDir = Join-Path $ScriptDir 'build'
$ExePath = $null
$LauncherBuildDir = Join-Path $BuildDir 'launcher'
$LauncherDistDir = Join-Path $BuildDir 'launcher-dist'
$LauncherTempExePath = $null

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    $PythonExe = $env:QQSG_PYTHON_EXE
}
if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    $PythonExe = 'python'
}
if ([string]::IsNullOrWhiteSpace($SignToolPath)) {
    $SignToolPath = $env:QQSG_SIGNTOOL
}
if ([string]::IsNullOrWhiteSpace($CertPath)) {
    $CertPath = $env:QQSG_CODESIGN_PFX
}

if ((-not $PSBoundParameters.ContainsKey('CertPassword')) -and (-not [string]::IsNullOrWhiteSpace($env:QQSG_CODESIGN_PASSWORD))) {
    $CertPassword = ConvertTo-SecureString -String $env:QQSG_CODESIGN_PASSWORD -AsPlainText -Force
}

function Test-RequiredFile {
    param([string]$PathText)
    if (-not (Test-Path -LiteralPath $PathText)) {
        throw "缺少文件: $PathText"
    }
}

function Invoke-External {
    param(
        [string]$FilePath,
        [string[]]$Arguments
    )
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        $joined = ($Arguments -join ' ')
        throw "执行失败: $FilePath $joined"
    }
}

function Resolve-SignTool {
    param([string]$ProvidedPath)
    if (-not [string]::IsNullOrWhiteSpace($ProvidedPath)) {
        if (Test-Path -LiteralPath $ProvidedPath) {
            return (Resolve-Path -LiteralPath $ProvidedPath).Path
        }
        throw "找不到 SignTool: $ProvidedPath"
    }
    $cmd = Get-Command signtool.exe -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source) {
        return $cmd.Source
    }
    $roots = @(
        'C:\Program Files (x86)\Windows Kits',
        'C:\Program Files\Windows Kits',
        'C:\Program Files (x86)\Microsoft SDKs\ClickOnce\SignTool',
        'C:\Program Files\Microsoft SDKs\ClickOnce\SignTool'
    ) | Where-Object { Test-Path -LiteralPath $_ }
    $signToolCandidates = foreach ($root in $roots) {
        Get-ChildItem -Path $root -Filter signtool.exe -File -Recurse -ErrorAction SilentlyContinue
    }
    $best = $signToolCandidates | Sort-Object FullName -Descending | Select-Object -First 1
    if ($best) {
        return $best.FullName
    }
    throw '未找到 signtool.exe，请安装 Windows SDK 或手动传入 -SignToolPath'
}

function Show-Info {
    param([string]$Message)
    Write-Host "[QQSG] $Message"
}

function ConvertTo-PlainPassword {
    param([SecureString]$Value)
    if ($null -eq $Value) {
        return ''
    }
    return [System.Net.NetworkCredential]::new('', $Value).Password
}

function Copy-Artifact {
    param(
        [string]$SourcePath,
        [string]$DestinationPath
    )
    $destDir = Split-Path -Parent $DestinationPath
    if (-not [string]::IsNullOrWhiteSpace($destDir) -and -not (Test-Path -LiteralPath $destDir)) {
        New-Item -ItemType Directory -Path $destDir -Force | Out-Null
    }
    Copy-Item -LiteralPath $SourcePath -Destination $DestinationPath -Force
}

function Resolve-GeneratedExePath {
    param([string]$SearchRoot)
    if (-not (Test-Path -LiteralPath $SearchRoot)) {
        throw "目录不存在: $SearchRoot"
    }
    $exeCandidates = Get-ChildItem -LiteralPath $SearchRoot -Filter *.exe -File -Recurse -ErrorAction Stop |
        Sort-Object FullName
    $best = $exeCandidates | Select-Object -First 1
    if (-not $best) {
        throw "未找到 exe 产物: $SearchRoot"
    }
    return $best.FullName
}

function Resolve-DefaultLauncherOutputPath {
    param([string]$PackagedExePath)
    $fileName = Split-Path -Leaf $PackagedExePath
    return Join-Path ([Environment]::GetFolderPath('Desktop')) $fileName
}

Test-RequiredFile $BuildSpec
Test-RequiredFile $LauncherSpec
Test-RequiredFile $VersionInfo

$pythonVersion = & $PythonExe --version 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "无法执行 Python: $PythonExe"
}
$pyInstallerVersion = & $PythonExe -m PyInstaller --version 2>&1
if ($LASTEXITCODE -ne 0) {
    throw 'PyInstaller 不可用，请先安装 requirements-build.txt 中的依赖'
}

Show-Info "Python: $pythonVersion"
Show-Info "PyInstaller: $pyInstallerVersion"
Show-Info "Spec: $BuildSpec"
Show-Info "LauncherSpec: $LauncherSpec"
Show-Info "DistDir: $DistDir"
if (-not [string]::IsNullOrWhiteSpace($LauncherOutputPath)) {
    Show-Info "DesktopLauncher: $LauncherOutputPath"
}

if ($CheckOnly) {
    if ($Sign -or -not [string]::IsNullOrWhiteSpace($CertPath)) {
        $resolvedSignTool = Resolve-SignTool $SignToolPath
        Show-Info "SignTool: $resolvedSignTool"
    }
    Show-Info '环境检查完成'
    exit 0
}

if (-not $SkipBuild) {
    if (-not $SkipClean) {
        if (Test-Path -LiteralPath $BuildDir) {
            Remove-Item -LiteralPath $BuildDir -Recurse -Force
        }
        if (Test-Path -LiteralPath $DistDir) {
            Remove-Item -LiteralPath $DistDir -Recurse -Force
        }
    }
    Show-Info '开始打包目录版桌面程序'
    Invoke-External $PythonExe @('-m', 'PyInstaller', '--clean', '--noconfirm', '--distpath', $DistDir, '--workpath', $BuildDir, $BuildSpec)
    $ExePath = Resolve-GeneratedExePath $DistDir
    if (-not $SkipLauncherBuild) {
        Show-Info '开始打包桌面启动器'
        Invoke-External $PythonExe @('-m', 'PyInstaller', '--clean', '--noconfirm', '--distpath', $LauncherDistDir, '--workpath', $LauncherBuildDir, $LauncherSpec)
        $LauncherTempExePath = Resolve-GeneratedExePath $LauncherDistDir
        if ([string]::IsNullOrWhiteSpace($LauncherOutputPath)) {
            $LauncherOutputPath = Resolve-DefaultLauncherOutputPath $LauncherTempExePath
        }
        Copy-Artifact -SourcePath $LauncherTempExePath -DestinationPath $LauncherOutputPath
    }
}

if ([string]::IsNullOrWhiteSpace($ExePath)) {
    $ExePath = Resolve-GeneratedExePath $DistDir
}
if ([string]::IsNullOrWhiteSpace($LauncherOutputPath)) {
    $LauncherOutputPath = Resolve-DefaultLauncherOutputPath $ExePath
}
Test-RequiredFile $ExePath
Show-Info "产物已生成: $ExePath"
if (-not $SkipLauncherBuild) {
    Test-RequiredFile $LauncherOutputPath
    Show-Info "桌面启动器已更新: $LauncherOutputPath"
}

if ($Sign -or -not [string]::IsNullOrWhiteSpace($CertPath)) {
    if ([string]::IsNullOrWhiteSpace($CertPath)) {
        throw '缺少证书路径，请传入 -CertPath 或设置 QQSG_CODESIGN_PFX'
    }
    Test-RequiredFile $CertPath
    $resolvedSignTool = Resolve-SignTool $SignToolPath
    Show-Info "使用 SignTool: $resolvedSignTool"
    $plainPassword = ConvertTo-PlainPassword $CertPassword
    $targets = @($ExePath)
    if (Test-Path -LiteralPath $LauncherOutputPath) {
        $targets += $LauncherOutputPath
    }
    $targets = $targets | Select-Object -Unique
    foreach ($target in $targets) {
        $signArgs = @('sign', '/fd', 'SHA256', '/td', 'SHA256', '/tr', $TimestampUrl, '/f', $CertPath)
        if (-not [string]::IsNullOrWhiteSpace($plainPassword)) {
            $signArgs += @('/p', $plainPassword)
        }
        $signArgs += $target
        Invoke-External $resolvedSignTool $signArgs
        Invoke-External $resolvedSignTool @('verify', '/pa', '/v', $target)
        Show-Info "已签名并验签: $target"
    }
    Show-Info '签名与验签完成'
}

Show-Info '全部完成'
