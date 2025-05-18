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

# Find the [Unreleased] section
$unreleasedIndex = $content | Select-String -Pattern "## \[Unreleased\]" | Select-Object -First 1 -ExpandProperty LineNumber
if (-not $unreleasedIndex) {
    Write-Error "Could not find [Unreleased] section in changelog"
    exit 1
}

# Create new content array
$newContent = @()

# Add everything up to [Unreleased]
$newContent += $content[0..($unreleasedIndex)]

# Add a blank line after [Unreleased]
$newContent += ""

# Add the new entry
$newContent += "### Added"
$newContent += "- ${DATE} - ${COMMIT_MSG}"

# Find the next section header (starts with ##) after [Unreleased]
$nextSectionIndex = $unreleasedIndex + 1
while ($nextSectionIndex -lt $content.Length -and -not $content[$nextSectionIndex].StartsWith("## ")) {
    $nextSectionIndex++
}

# Add the rest of the content starting from the next section
if ($nextSectionIndex -lt $content.Length) {
    $newContent += ""  # Add blank line before next section
    $newContent += $content[$nextSectionIndex..($content.Length - 1)]
} else {
    $newContent += ""  # Add final newline
}

# Write back to the file
$newContent | Set-Content $CHANGELOG_FILE

Write-Host "Changelog updated successfully!"

# Stage the changes
git add $CHANGELOG_FILE 