# Fix for Claude VSCode Extension

I've identified that Claude is running as a VSCode extension, not as a standalone CLI tool as initially assumed. The scripts and configuration files we created won't interfere with its operation, but here's how to properly use Claude in your environment:

## Current Setup
- Claude is installed as a VSCode extension (saoudrizwan.claude-dev-3.18.3)
- It's running a service on port 35266 (this is why we saw that port in our checks)
- The service is managed by VSCode, not by a standalone CLI command

## How to Use Claude in VSCode
1. Simply open VSCode and use the Claude extension directly within the editor
2. You don't need to manually start Claude from the command line

## How to Connect from Virtual Desktop

### Option 1: SSH with X Forwarding (if your Virtual Desktop supports graphical applications)
1. Connect to your WSL machine with X forwarding enabled:
   ```bash
   ssh -X admsia@SEA-1802220383
   ```
2. Launch VSCode with:
   ```bash
   code
   ```
3. Use the Claude extension within the forwarded VSCode window

### Option 2: Port Forward the VSCode Server
1. On your WSL machine, find your VSCode server port:
   ```bash
   netstat -tuln | grep vscode
   ```
   Look for ports typically in the range 49000-65000

2. From your virtual desktop, create an SSH tunnel:
   ```bash
   ssh -L <vscode-port>:localhost:<vscode-port> admsia@SEA-1802220383
   ```

3. Connect to the forwarded VSCode server from your browser on the virtual desktop:
   ```
   http://localhost:<vscode-port>
   ```

### Option 3: Remote SSH Extension in VSCode (Recommended)
1. Install the "Remote - SSH" extension in VSCode on your virtual desktop
2. Configure a new SSH host pointing to your WSL machine
3. Connect to the WSL machine through VSCode's Remote SSH feature
4. This will give you full access to the extensions installed on the WSL machine

## Recovery Steps
If Claude extension is not working properly:
1. Restart VSCode
2. Check if the extension is enabled in VSCode
3. If needed, reinstall the extension:
   - Open VSCode
   - Go to Extensions (Ctrl+Shift+X)
   - Search for "Claude"
   - Install or update as needed

No changes we made should have impacted the VSCode extension's functionality.
