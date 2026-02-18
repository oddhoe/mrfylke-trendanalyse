param(
  [Parameter(Mandatory=$true)]
  [string]$Repo
)

function AutoCommit([string]$When) {
  Push-Location $Repo

  # Stage kun .py (inkl. slettinger) ved å bruke pathspec/glob
  git add -A -- '*.py'

  # Sjekk om det faktisk er staged .py-endringer
  $staged = git diff --cached --name-only -- '*.py'
  if ($staged) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm"
    git commit -m "auto($When): python $ts"
    git push
  }

  Pop-Location
}

# 1) Auto-commit ved start (tar med evt. endringer fra forrige økt)
AutoCommit "start"

# 2) Åpne VS Code og vent til "wait-fila" lukkes
$vscodeDir = Join-Path $Repo ".vscode"
$waitFile  = Join-Path $vscodeDir ".autocommit.wait"

New-Item -ItemType Directory -Force $vscodeDir | Out-Null
if (-not (Test-Path $waitFile)) { New-Item -ItemType File $waitFile | Out-Null }

# --wait: VS Code CLI støtter å vente før kommandoen returnerer. [page:0]
& code -n $Repo --wait $waitFile

# 3) Auto-commit ved stopp
AutoCommit "stop"
