# ============================================================
# setup.ps1 — one-command setup + launch for Frontal Lobe demo
#   - checks Python >= 3.11 (fails with a clear message)
#   - creates .venv, installs requirements
#   - starts the local app (FastAPI on http://localhost:8765)
#   - opens the browser
# Run from PowerShell:
#   powershell -ExecutionPolicy Bypass -File .\setup.ps1
# ============================================================
$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

function Get-PythonCmd {
    # Prefer the py launcher, then python on PATH.
    $candidates = @()
    try { $candidates += @('py -3.11') } catch {}
    try { $candidates += @('py') } catch {}
    try { $candidates += @('python') } catch {}
    foreach ($cmd in $candidates) {
        try {
            $v = & $cmd.Split(' ') @('--version') 2>$null
            if ($LASTEXITCODE -eq 0 -and $v -match '(\d+)\.(\d+)') {
                return @{ Cmd = $cmd; Major = [int]$Matches[1]; Minor = [int]$Matches[2] }
            }
        } catch {}
    }
    return $null
}

$py = Get-PythonCmd
if ($null -eq $py) {
    Write-Host "ERROR: Python was not found. Install Python 3.11+ from https://www.python.org/downloads/" -ForegroundColor Red
    exit 1
}
if ($py.Major -lt 3 -or ($py.Major -eq 3 -and $py.Minor -lt 11)) {
    Write-Host "ERROR: Python $($py.Major).$($py.Minor) is too old. The demo requires Python 3.11+." -ForegroundColor Red
    exit 1
}
Write-Host "Python $($py.Major).$($py.Minor) detected."

# Virtual environment
if (-not (Test-Path "$Root\.venv")) {
    Write-Host "Creating virtual environment..."
    & $py.Cmd.Split(' ') @('-m', 'venv', "$Root\.venv")
}
$Python = "$Root\.venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { Write-Host "ERROR: venv python missing." -ForegroundColor Red; exit 1 }

# Dependencies
Write-Host "Installing requirements (first run may take a minute)..."
& $Python -m pip install --quiet --disable-pip-version-check -r "$Root\requirements.txt"
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: pip install failed." -ForegroundColor Red; exit 1 }

# Start the app and open the browser
Write-Host "Starting Frontal Lobe demo on http://localhost:8765 ..."
Start-Process -FilePath $Python -ArgumentList @('-m','uvicorn','api.app.server:app','--host','127.0.0.1','--port','8765','--log-level','warning') `
    -WorkingDirectory $Root -WindowStyle Hidden
Start-Sleep -Seconds 4
Start-Process "http://localhost:8765"
Write-Host "Done. The app should have opened in your browser."
