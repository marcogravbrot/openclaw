#!/usr/bin/env pwsh
# camus-update-fork.ps1
# Pulls latest upstream/main and rebases our patch branch on top.
# Reports merge conflicts and whether upstream may have superseded our fix.

$ErrorActionPreference = "Stop"
$PatchBranch = "camus/discord-streaming-fix"
$BaseBranch  = "main"
$PatchedFiles = @(
    "extensions/discord/src/monitor/message-handler.draft-preview.ts",
    "extensions/discord/src/preview-streaming.ts",
    "extensions/discord/src/draft-stream.ts"
)
# Markers our fix introduces; if upstream adds them, we may not need our patch any more.
$ObsolescenceMarkers = @("timeline:", "currentTextBuffer", "Segment\[\]", "kind: ['""]tool['""]")

function Section($t) { Write-Host "`n==== $t ====" -ForegroundColor Cyan }

Section "Fetch upstream"
git fetch upstream --tags

Section "Snapshot pre-update commit on $PatchBranch"
git checkout $PatchBranch | Out-Null
$preBase = git merge-base HEAD upstream/$BaseBranch
$preHead = git rev-parse HEAD
Write-Host "  base=$preBase  head=$preHead"

Section "Rebase $PatchBranch onto upstream/$BaseBranch"
$rebaseOk = $true
try { git rebase upstream/$BaseBranch } catch { $rebaseOk = $false }

if (-not $rebaseOk -or (git status --porcelain | Select-String -Pattern "^(UU|AA|DD)")) {
    Section "MERGE CONFLICTS"
    git status --short | Where-Object { $_ -match "^(UU|AA|DD|AU|UA|DU|UD)" }
    Write-Host "`nResolve conflicts manually, then:  git rebase --continue" -ForegroundColor Yellow
    Write-Host "Or to abort:                         git rebase --abort"     -ForegroundColor Yellow
    exit 2
}

Section "Check whether upstream may have implemented our fix"
$newBase = git merge-base HEAD upstream/$BaseBranch
foreach ($f in $PatchedFiles) {
    if (-not (Test-Path $f)) { continue }
    foreach ($m in $ObsolescenceMarkers) {
        $hits = git show "upstream/${BaseBranch}:$f" 2>$null | Select-String -Pattern $m
        if ($hits) {
            Write-Host "  HINT: upstream/$BaseBranch:$f contains marker /$m/ — review whether our patch is still needed" -ForegroundColor Yellow
        }
    }
}

Section "Done"
Write-Host "Patch branch '$PatchBranch' now sits on top of upstream/$BaseBranch ($newBase)."
Write-Host "Next: build the plugin and swap into ~/.openclaw/npm/node_modules/@openclaw/discord."
