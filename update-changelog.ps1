# PowerShell script to update changelog

# Get the last commit message
$COMMIT_MSG = git log -1 --pretty=%B

# Skip if this is a changelog update commit
if ($COMMIT_MSG -like "*Update changelog*") {
    Write-Host "Skipping changelog update commit"
    exit 0
}

# Get current date
$DATE = Get-Date -Format "yyyy-MM-dd"

# Path to your changelog
$CHANGELOG_FILE = "btc_bot/CHANGELOG.md"

# Check if the file exists
if (-not (Test-Path $CHANGELOG_FILE)) {
    Write-Error "Error: CHANGELOG.md not found at $CHANGELOG_FILE"
    exit 1
}

# Read the entire file
$content = Get-Content $CHANGELOG_FILE

# Find the [Unreleased] section and the first version section
$unreleasedLine = $content | Where-Object { $_ -eq "## [Unreleased]" } | Select-Object -First 1
$firstVersionLine = $content | Where-Object { $_ -match "^## \d" } | Select-Object -First 1

if (-not $unreleasedLine) {
    Write-Error "Could not find [Unreleased] section in changelog"
    exit 1
}

# Split content into three parts: header, versions, and the rest
$headerEnd = $content.IndexOf($unreleasedLine)
$versionsStart = $content.IndexOf($firstVersionLine)

# Rebuild the file
$newContent = @()

# Add header (everything before [Unreleased])
$newContent += $content[0..($headerEnd)]

# Add blank line
$newContent += ""

# Add new entry
$newContent += "### Added"
$newContent += "- ${DATE} - ${COMMIT_MSG}"
$newContent += ""

# Add all version sections
$newContent += $content[$versionsStart..($content.Length - 1)]

# Write back to the file
$newContent | Set-Content $CHANGELOG_FILE

Write-Host "Changelog updated successfully!"

# Stage the changes
git add $CHANGELOG_FILE 