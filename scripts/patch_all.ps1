Set-StrictMode -Off
$ErrorActionPreference = "Continue"
$repoRoot = Get-Location

Write-Host "`n=== STEP 1: Patch workflow YAMLs ===" -ForegroundColor Cyan

$workflowDir = Join-Path $repoRoot ".github\workflows"
$workflows = Get-ChildItem -Path $workflowDir -Filter "scrape_*.yml"
$patchedWF = 0

foreach ($wf in $workflows) {
    $content = Get-Content $wf.FullName -Raw -Encoding UTF8
    $original = $content
    if ($content -notmatch "timeout-minutes:") {
        $content = $content -replace "(    runs-on:[^\n]+)", "`$1`n    timeout-minutes: 90"
    } else {
        $content = $content -replace "timeout-minutes:\s*\d+", "timeout-minutes: 90"
    }
    if ($content -notmatch "continue-on-error:") {
        $content = $content -replace "(    timeout-minutes:[^\n]+)", "`$1`n    continue-on-error: true"
    }
    if ($content -ne $original) {
        [System.IO.File]::WriteAllText($wf.FullName, $content, [System.Text.Encoding]::UTF8)
        Write-Host "patched $($wf.Name)" -ForegroundColor Green
        $patchedWF++
    } else {
        Write-Host "unchanged $($wf.Name)" -ForegroundColor DarkGray
    }
}
Write-Host "$patchedWF workflows patched`n"

Write-Host "=== STEP 2: Patch scrapers (LOOKBACK_DAYS) ===" -ForegroundColor Cyan

$scraperDir = Join-Path $repoRoot "scraper"
$scrapers = Get-ChildItem -Path $scraperDir -Filter "multi_county_*.py"
$scrapers += Get-ChildItem -Path $scraperDir -Filter "tyler_universal.py" -ErrorAction SilentlyContinue
$patchedS = 0

foreach ($f in $scrapers) {
    if ($null -eq $f) { continue }
    $content = Get-Content $f.FullName -Raw -Encoding UTF8
    $original = $content
    $content = $content -replace "LOOKBACK_DAYS\s*=\s*(?!int)\d+", 'LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", 3))'
    $content = $content -replace "MAX_PAGES\s*=\s*(?!get)\d+", 'MAX_PAGES = int(os.environ.get("MAX_PAGES", 10))'
    if ($content -ne $original) {
        [System.IO.File]::WriteAllText($f.FullName, $content, [System.Text.Encoding]::UTF8)
        Write-Host "patched $($f.Name)" -ForegroundColor Green
        $patchedS++
    }
}
Write-Host "$patchedS scrapers patched`n"

Write-Host "=== STEP 3: Verify ===" -ForegroundColor Cyan
foreach ($wf in (Get-ChildItem -Path $workflowDir -Filter "scrape_*.yml")) {
    $c = Get-Content $wf.FullName -Raw
    $t = if ($c -match "timeout-minutes: 90") { "90min" } else { "NO TIMEOUT" }
    $co = if ($c -match "continue-on-error: true") { "continue-on-error" } else { "NO COE" }
    Write-Host "  $($wf.Name): $t | $co"
}

Write-Host "`nDone. Now run:" -ForegroundColor Yellow
Write-Host "  git add scraper/ .github/workflows/" -ForegroundColor White
Write-Host '  git commit -m "fix: 90min timeouts + LOOKBACK_DAYS=3"' -ForegroundColor White
Write-Host "  git push" -ForegroundColor White
