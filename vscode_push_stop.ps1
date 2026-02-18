param(
  [Parameter(Mandatory=$true)]
  [string]$Repo
)

function AutoCommitStop() {
  Push-Location $Repo

  # Stage kun .py (inkl. slettinger)
  git add -A -- '*.py'

  # Sjekk om det faktisk er staged .py-endringer
  $staged = git diff --cached --name-only -- '*.py'
  if ($staged) {
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm"
    git commit -m "auto(stop): python $ts"
    git push
  }

  Pop-Location
}

# Åpne VS Code og vent til "wait-fila" lukkes
$vscodeDir = Join-Path $Repo ".vscode"
$waitFile  = Join-Path $vscodeDir ".autocommit.wait"

New-Item -ItemType Directory -Force $vscodeDir | Out-Null
if (-not (Test-Path $waitFile)) { New-Item -ItemType File $waitFile | Out-Null }

# --wait: returnerer når $waitFile lukkes i VS Code
& code -n $Repo --wait $waitFile

# Auto-commit/push KUN ved “avslutt”
AutoCommitStop