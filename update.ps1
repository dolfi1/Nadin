param(
    [string]$Remote = "origin",
    [string]$Branch = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args
    )

    & git @Args
    if ($LASTEXITCODE -ne 0) {
        throw "Команда git завершилась с ошибкой: git $($Args -join ' ')"
    }
}

$stashCreated = $false

try {
    $insideWorkTree = (& git rev-parse --is-inside-work-tree 2>$null)
    if ($LASTEXITCODE -ne 0 -or $insideWorkTree.Trim() -ne "true") {
        throw "Текущая папка не является git-репозиторием."
    }

    if ([string]::IsNullOrWhiteSpace($Branch)) {
        $Branch = (& git rev-parse --abbrev-ref HEAD).Trim()
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($Branch) -or $Branch -eq "HEAD") {
            throw "Не удалось определить текущую ветку. Укажите её явно: .\update.ps1 -Branch main"
        }
    }

    & git remote get-url $Remote > $null 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Remote '$Remote' не найден."
    }

    Write-Host "Обновление ветки '$Branch' из '$Remote'..."

    Invoke-Git -Args @("fetch", $Remote)

    $statusOutput = (& git status --porcelain)
    $hasLocalChanges = -not [string]::IsNullOrWhiteSpace(($statusOutput -join "`n"))

    if ($hasLocalChanges) {
        $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        Write-Host "Найдены локальные изменения. Сохраняю их во временный stash..."
        Invoke-Git -Args @("stash", "push", "--include-untracked", "-m", "auto-stash before update $stamp")
        $stashCreated = $true
    }

    & git pull --ff-only $Remote $Branch
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Fast-forward недоступен. Пробую pull --rebase..."
        Invoke-Git -Args @("pull", "--rebase", $Remote, $Branch)
    }

    if ($stashCreated) {
        Write-Host "Возвращаю локальные изменения из stash..."
        & git stash pop
        if ($LASTEXITCODE -ne 0) {
            throw "Не удалось автоматически применить stash. Проверьте статус: git status"
        }
    }

    Write-Host "Готово. Локальные файлы обновлены."
}
catch {
    $errorMessage = $_.Exception.Message

    if ($stashCreated) {
        Write-Warning "Обновление прервано. Пытаюсь вернуть локальные изменения из stash..."
        & git stash pop
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Не удалось автоматически вернуть stash. Проверьте: git stash list и git status"
        }
    }

    Write-Error $errorMessage
    exit 1
}
