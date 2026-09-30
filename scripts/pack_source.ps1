param(
    [ValidateSet("Zip", "Wheel", "Sdist", "Python")]
    [string]$Format = "Zip",
    [string]$OutputDirectory = "dist"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    $outputPath = $OutputDirectory
} else {
    $outputPath = Join-Path $projectRoot $OutputDirectory
}
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null

function Invoke-UvBuild {
    param([string]$BuildFormat)

    & uv build "--$BuildFormat" --out-dir $outputPath
    if ($LASTEXITCODE -ne 0) {
        throw "uv build --$BuildFormat failed with exit code $LASTEXITCODE."
    }
}

switch ($Format) {
    "Zip" {
        $archivePath = Join-Path $outputPath "atm-fraud-detection-source.zip"
        $excludedDirectories = @(".git", ".venv", "__pycache__", ".pytest_cache", "build", "dist", "data", "checkpoints")
        $files = Get-ChildItem -LiteralPath $projectRoot -File -Recurse -Force | Where-Object {
            $relativePath = $_.FullName.Substring($projectRoot.Length).TrimStart([char[]]"\/")
            $parts = $relativePath -split "[\\/]"
            $directoryParts = $parts | Select-Object -First ($parts.Length - 1)
            $hasExcludedDirectory = $false
            foreach ($part in $directoryParts) {
                if ($excludedDirectories -contains $part -or $part -like "*.egg-info") {
                    $hasExcludedDirectory = $true
                    break
                }
            }
            -not $hasExcludedDirectory -and $_.Extension -ne ".pyc"
        }

        if (-not $files) {
            throw "No source files found to archive."
        }

        Add-Type -AssemblyName System.IO.Compression
        Add-Type -AssemblyName System.IO.Compression.FileSystem
        if (Test-Path -LiteralPath $archivePath) {
            Remove-Item -LiteralPath $archivePath -Force
        }

        $archive = [System.IO.Compression.ZipFile]::Open(
            $archivePath,
            [System.IO.Compression.ZipArchiveMode]::Create
        )
        try {
            foreach ($file in $files) {
                $entryName = $file.FullName.Substring($projectRoot.Length).TrimStart([char[]]"\/") -replace "\\", "/"
                [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
                    $archive,
                    $file.FullName,
                    $entryName,
                    [System.IO.Compression.CompressionLevel]::Optimal
                ) | Out-Null
            }
        } finally {
            $archive.Dispose()
        }
        Write-Host "Created $archivePath" -ForegroundColor Green
    }
    "Wheel" { Invoke-UvBuild "wheel" }
    "Sdist" { Invoke-UvBuild "sdist" }
    "Python" {
        Invoke-UvBuild "sdist"
        Invoke-UvBuild "wheel"
    }
}