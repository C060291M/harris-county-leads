$workflowDir = ".github\workflows"
$workflows = Get-ChildItem -Path $workflowDir -Filter "scrape_tyler_*.yml"
$workflows += Get-ChildItem -Path $workflowDir -Filter "scrape_bexar.yml"
$workflows += Get-ChildItem -Path $workflowDir -Filter "scrape_south.yml"
$workflows += Get-ChildItem -Path $workflowDir -Filter "scrape_portals.yml"

$patched = 0
foreach ($wf in $workflows) {
    $content = Get-Content $wf.FullName -Raw -Encoding UTF8
    $original = $content
    # Add env block before every "run: |" that runs a python scraper
    # Pattern: find lines with "run: |" preceded by "- name:" and followed by python call
    # Simpler: inject env: block at job level under each job definition
    # Find jobs that don't have env: LOOKBACK_DAYS
    if ($content -notmatch "LOOKBACK_DAYS:") {
        # Add env block after each "runs-on:" line at job level
        $content = $content -replace "(    runs-on:[^\n]+\n    timeout-minutes:[^\n]+\n    continue-on-error:[^\n]+)", "`$1`n    env:`n      LOOKBACK_DAYS: `"3`"`n      MAX_PAGES: `"10`""
        if ($content -ne $original) {
            [System.IO.File]::WriteAllText($wf.FullName, $content, [System.Text.Encoding]::UTF8)
            Write-Host "patched $($wf.Name)"
            $patched++
        }
    } else {
        Write-Host "already has LOOKBACK_DAYS: $($wf.Name)"
    }
}
Write-Host "$patched files patched"
