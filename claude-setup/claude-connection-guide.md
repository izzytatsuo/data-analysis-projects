# Connecting to Claude on WSL from Virtual Desktop

## Summary of Findings

- Claude server is running on port 35266 on your WSL instance
- The server uses WebSocket protocol for communication
- Authentication token: `0595a2a0-d1b5-4cd4-bd6d-d6557cfb15c8`

## Connection Method: SSH Port Forwarding

The most reliable way to connect to your WSL Claude instance from your virtual desktop is through SSH port forwarding. Here's the step-by-step guide:

### From Your Virtual Desktop

1. Open a terminal on your virtual desktop
2. Run the following SSH command to create a remote port forwarding:

```bash
ssh -R 35266:localhost:35266 admsia@SEA-1802220383
```

3. This command forwards port 35266 on your virtual desktop to port 35266 on your WSL machine
4. Keep this SSH connection open as long as you need to use Claude

### Configuring Claude Client on Virtual Desktop

If you have the Claude CLI installed on your virtual desktop, you might need to:

1. Create or modify the Claude configuration on your virtual desktop
2. Ensure it points to the localhost:35266 endpoint
3. Use the authentication token found in the WSL instance: `0595a2a0-d1b5-4cd4-bd6d-d6557cfb15c8`

## Alternative: Direct Installation

If possible, the simplest approach would be to install Claude CLI directly on your virtual desktop. This would avoid the need for tunneling and port forwarding.

## Troubleshooting

If you encounter issues:

1. Verify that Claude is running on your WSL instance:
   ```bash
   ps aux | grep claude
   ```

2. Check if port 35266 is listening on your WSL instance:
   ```bash
   netstat -tuln | grep 35266
   ```

3. Test the connection locally on WSL:
   ```bash
   curl -v localhost:35266
   ```

4. Check for SSH tunnel status on the virtual desktop:
   ```bash
   netstat -tuln | grep 35266
   ```

5. Verify the authentication token is current:
   ```bash
   cat ~/.claude/ide/35266.lock
   ```

## Additional Notes

- The SSH tunnel must remain active while you're using Claude
- If your virtual desktop is in a different network with restrictive firewall rules, you might need to adjust the approach
