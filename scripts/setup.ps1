# SPR setup for Windows (PowerShell 5.1+). ASCII only: PowerShell 5.1 reads .ps1 files
# without a BOM in the legacy code page and would mangle non-ASCII text.
# Run from the repository root:  powershell -ExecutionPolicy Bypass -File scripts\setup.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Need($cmd, $hint) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Write-Host "Not found: $cmd. $hint" -ForegroundColor Red
        exit 1
    }
}
Need cargo  "Install Rust from https://rustup.rs (MSVC toolchain) plus 'Visual Studio Build Tools' with the 'Desktop development with C++' workload."
Need python "Install Python 3.10+ from https://www.python.org/downloads/windows/ (tick 'Add python.exe to PATH')."

Write-Host "== Building the core (cargo build --release) ==" -ForegroundColor Cyan
Push-Location core
cargo build --release
$rc = $LASTEXITCODE
Pop-Location
if ($rc -ne 0) { Write-Host "cargo build failed" -ForegroundColor Red; exit 1 }

Write-Host "== Python environment (.venv) and analytics package ==" -ForegroundColor Cyan
if (-not (Test-Path ".venv")) { python -m venv .venv }
& ".venv\Scripts\python.exe" -m pip install --upgrade pip --quiet
& ".venv\Scripts\python.exe" -m pip install -e analytics --quiet
if ($LASTEXITCODE -ne 0) { Write-Host "pip install failed" -ForegroundColor Red; exit 1 }

Write-Host ""
Write-Host "Done. Next steps (from the repository root):" -ForegroundColor Green
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host "  .\core\target\release\spr.exe symbols --out config\instruments.json"
Write-Host "  .\core\target\release\spr.exe run"
Write-Host "  python -m spr_analytics analyze"
Write-Host "  uvicorn spr_analytics.dashboard.app:app --port 8000"
