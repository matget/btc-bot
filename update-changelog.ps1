# PowerShell script to update changelog

# Get the last commit message
$COMMIT_MSG = git log -1 --pretty=%B

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

# Find the [Unreleased] section
$unreleasedIndex = $content | Select-String -Pattern "## \[Unreleased\]" | Select-Object -First 1 -ExpandProperty LineNumber
if (-not $unreleasedIndex) {
    Write-Error "Could not find [Unreleased] section in changelog"
    exit 1
}

# Insert the new commit entry after the [Unreleased] section
$newContent = @()
$newContent += $content[0..($unreleasedIndex)]
$newContent += ""
$newContent += "### Added"
$newContent += "- ${DATE} - ${COMMIT_MSG}"
$newContent += ""
$newContent += $content[($unreleasedIndex + 1)..($content.Length - 1)]

# Write back to the file
$newContent | Set-Content $CHANGELOG_FILE

Write-Host "Changelog updated successfully!"

# Stage the changes
git add $CHANGELOG_FILE 