# FlowCTL Installer Script for Windows
# Usage: iwr -useb https://get.flowctl.dev/install.ps1 | iex
# Or download and run: powershell -ExecutionPolicy Bypass -File install.ps1

param(
    [string]$Version = "latest",
    [string]$InstallDir = "$env:USERPROFILE\.flowctl"
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

# Configuration
$GithubRepo = "withObsrvr/cdp-pipeline-workflow"
$BinaryName = "flowctl"

# Helper functions
function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] " -ForegroundColor Green -NoNewline
    Write-Host $Message
}

function Write-Warn {
    param([string]$Message)
    Write-Host "[WARN] " -ForegroundColor Yellow -NoNewline
    Write-Host $Message
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] " -ForegroundColor Red -NoNewline
    Write-Host $Message
    exit 1
}

# Detect architecture
function Get-Architecture {
    $arch = if ([Environment]::Is64BitProcess) { "amd64" } else { "386" }
    
    # Check for ARM64 (Windows on ARM)
    $processorArch = $env:PROCESSOR_ARCHITECTURE
    if ($processorArch -eq "ARM64") {
        $arch = "arm64"
    }
    
    return $arch
}

# Get latest version from GitHub
function Get-LatestVersion {
    if ($Version -eq "latest") {
        Write-Info "Fetching latest version..."
        
        try {
            $latestUrl = "https://api.github.com/repos/$GithubRepo/releases/latest"
            $response = Invoke-RestMethod -Uri $latestUrl -UseBasicParsing
            $Version = $response.tag_name
            Write-Info "Latest version: $Version"
            return $Version
        } catch {
            Write-Error "Failed to fetch latest version: $_"
        }
    }
    
    return $Version
}

# Download flowctl binary
function Download-FlowCTL {
    param(
        [string]$Version,
        [string]$Platform
    )
    
    $downloadUrl = "https://github.com/$GithubRepo/releases/download/$Version/$BinaryName-$Platform.exe"
    $binDir = Join-Path $InstallDir "bin"
    $binPath = Join-Path $binDir "$BinaryName.exe"
    
    Write-Info "Downloading flowctl $Version for Windows ($Platform)..."
    Write-Info "URL: $downloadUrl"
    
    # Create directory
    if (!(Test-Path $binDir)) {
        New-Item -ItemType Directory -Force -Path $binDir | Out-Null
    }
    
    # Download file
    try {
        $tempFile = [System.IO.Path]::GetTempFileName()
        Invoke-WebRequest -Uri $downloadUrl -OutFile $tempFile -UseBasicParsing
        
        # Verify download
        if (!(Test-Path $tempFile) -or (Get-Item $tempFile).Length -eq 0) {
            Write-Error "Downloaded file is empty or missing"
        }
        
        # Move to final location
        Move-Item -Force $tempFile $binPath
        
        Write-Info "Binary installed to: $binPath"
    } catch {
        Write-Error "Failed to download flowctl: $_"
    }
}

# Verify checksum
function Verify-Checksum {
    param(
        [string]$Version,
        [string]$Platform
    )
    
    $checksumUrl = "https://github.com/$GithubRepo/releases/download/$Version/checksums.txt"
    $binPath = Join-Path $InstallDir "bin\$BinaryName.exe"
    
    Write-Info "Verifying checksum..."
    
    try {
        # Download checksums
        $tempChecksum = [System.IO.Path]::GetTempFileName()
        Invoke-WebRequest -Uri $checksumUrl -OutFile $tempChecksum -UseBasicParsing
        
        # Read checksums
        $checksums = Get-Content $tempChecksum
        $expectedLine = $checksums | Where-Object { $_ -match "$BinaryName-$Platform.exe" }
        
        if ($expectedLine) {
            $expectedSum = ($expectedLine -split '\s+')[0]
            $actualSum = (Get-FileHash -Path $binPath -Algorithm SHA256).Hash.ToLower()
            
            if ($expectedSum -eq $actualSum) {
                Write-Info "Checksum verified"
            } else {
                Write-Error "Checksum verification failed"
            }
        } else {
            Write-Warn "Checksum not found for this platform"
        }
        
        Remove-Item $tempChecksum -Force
    } catch {
        Write-Warn "Could not verify checksum: $_"
    }
}

# Add to PATH
function Add-ToPath {
    $binDir = Join-Path $InstallDir "bin"
    
    # Get current PATH
    $currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
    
    if ($currentPath -notlike "*$binDir*") {
        Write-Info "Adding flowctl to PATH..."
        
        # Add to user PATH
        $newPath = "$currentPath;$binDir"
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
        
        # Update current session
        $env:Path = "$env:Path;$binDir"
        
        Write-Info "Added to PATH: $binDir"
        Write-Warn "You may need to restart your terminal for PATH changes to take effect"
    } else {
        Write-Info "flowctl is already in your PATH"
    }
}

# Create Start Menu shortcut
function Create-Shortcut {
    $binPath = Join-Path $InstallDir "bin\$BinaryName.exe"
    $startMenuPath = [Environment]::GetFolderPath("StartMenu")
    $shortcutPath = Join-Path $startMenuPath "Programs\FlowCTL.lnk"
    
    try {
        $WshShell = New-Object -ComObject WScript.Shell
        $shortcut = $WshShell.CreateShortcut($shortcutPath)
        $shortcut.TargetPath = "cmd.exe"
        $shortcut.Arguments = "/k `"$binPath`" --help"
        $shortcut.WorkingDirectory = $env:USERPROFILE
        $shortcut.IconLocation = $binPath
        $shortcut.Description = "FlowCTL - Stellar CDP Pipeline CLI"
        $shortcut.Save()
        
        Write-Info "Created Start Menu shortcut"
    } catch {
        Write-Warn "Could not create Start Menu shortcut: $_"
    }
}

# Test installation
function Test-Installation {
    $binPath = Join-Path $InstallDir "bin\$BinaryName.exe"
    
    try {
        $versionOutput = & $binPath version 2>&1
        Write-Info "Installation successful!"
        Write-Info "Version: $versionOutput"
        return $true
    } catch {
        Write-Error "Installation completed but flowctl is not working properly: $_"
        return $false
    }
}

# Print usage information
function Show-Usage {
    Write-Host ""
    Write-Host "FlowCTL installed successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "To get started:"
    Write-Host ""
    Write-Host "1. Open a new terminal window (to load PATH changes)"
    Write-Host ""
    Write-Host "2. Verify installation:" -ForegroundColor Yellow
    Write-Host "   flowctl version"
    Write-Host ""
    Write-Host "3. Run your first pipeline:" -ForegroundColor Yellow
    Write-Host "   flowctl run `"captive-core://localhost | latest-ledger | stdout`""
    Write-Host ""
    Write-Host "4. View help:" -ForegroundColor Yellow
    Write-Host "   flowctl run --help"
    Write-Host ""
    Write-Host "For more information, visit:"
    Write-Host "https://github.com/$GithubRepo" -ForegroundColor Cyan
    Write-Host ""
}

# Uninstall function
function Uninstall-FlowCTL {
    Write-Info "Uninstalling FlowCTL..."
    
    # Remove from PATH
    $binDir = Join-Path $InstallDir "bin"
    $currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $newPath = ($currentPath -split ';' | Where-Object { $_ -ne $binDir }) -join ';'
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    
    # Remove directory
    if (Test-Path $InstallDir) {
        Remove-Item -Recurse -Force $InstallDir
    }
    
    # Remove Start Menu shortcut
    $shortcutPath = Join-Path ([Environment]::GetFolderPath("StartMenu")) "Programs\FlowCTL.lnk"
    if (Test-Path $shortcutPath) {
        Remove-Item -Force $shortcutPath
    }
    
    Write-Info "FlowCTL uninstalled successfully"
}

# Main installation flow
function Install-FlowCTL {
    Write-Host ""
    Write-Host "Installing FlowCTL..." -ForegroundColor Cyan
    Write-Host ""
    
    # Check if uninstalling
    if ($args -contains "--uninstall") {
        Uninstall-FlowCTL
        return
    }
    
    # Get architecture
    $arch = Get-Architecture
    $platform = "windows-$arch"
    Write-Info "Detected platform: $platform"
    
    # Get version
    $installVersion = Get-LatestVersion
    
    # Download binary
    Download-FlowCTL -Version $installVersion -Platform $platform
    
    # Verify checksum
    Verify-Checksum -Version $installVersion -Platform $platform
    
    # Add to PATH
    Add-ToPath
    
    # Create shortcuts
    Create-Shortcut
    
    # Test installation
    if (Test-Installation) {
        Show-Usage
    }
}

# Run main function
Install-FlowCTL @args