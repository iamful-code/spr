# Установка и сборка SPR на Windows (PowerShell 5.1+).
# Запуск из корня репозитория:  powershell -ExecutionPolicy Bypass -File scripts\setup.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Need($cmd, $hint) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        Write-Host "Не найдено: $cmd. $hint" -ForegroundColor Red
        exit 1
    }
}
Need cargo  "Установите Rust: https://rustup.rs (при установке выберите MSVC и поставьте 'Visual Studio Build Tools' с компонентом 'Разработка классических приложений на C++')."
Need python "Установите Python 3.10+ с https://www.python.org/downloads/windows/ (галочка 'Add python.exe to PATH')."

Write-Host "== Сборка ядра (cargo build --release) ==" -ForegroundColor Cyan
Push-Location core
cargo build --release
if ($LASTEXITCODE -ne 0) { Pop-Location; exit 1 }
Pop-Location

Write-Host "== Python-окружение (.venv) и пакет аналитики ==" -ForegroundColor Cyan
if (-not (Test-Path ".venv")) { python -m venv .venv }
& ".venv\Scripts\python.exe" -m pip install --upgrade pip --quiet
& ".venv\Scripts\python.exe" -m pip install -e analytics --quiet
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Host ""
Write-Host "Готово. Дальше (из корня репозитория):" -ForegroundColor Green
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host "  .\core\target\release\spr.exe symbols --out config\instruments.json"
Write-Host "  .\core\target\release\spr.exe run"
Write-Host "  python -m spr_analytics analyze"
Write-Host "  uvicorn spr_analytics.dashboard.app:app --port 8000"
