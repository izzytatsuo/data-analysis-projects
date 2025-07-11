# Connecting to Claude CLI on WSL from a Virtual Desktop

This guide provides step-by-step instructions for connecting to your Claude CLI running on WSL from your virtual desktop.

## System Setup

Your current setup:
- Claude CLI installed on Windows Subsystem for Linux (WSL)
- Working inside a virtual desktop accessed through SSH
- Claude is running a service on port 35266 on your WSL instance

## Connection Options

There are two main approaches to connect to Claude running on WSL from your virtual desktop:

1. **SSH Remote Port Forwarding** (Recommended)
2. **SSH Local Port Forwarding**
3. **Direct Claude installation on the virtual desktop** (Simplest if possible)

## Option 1: SSH Remote Port Forwarding (Recommended)

This method forwards a port on your virtual desktop to your WSL instance.

### Step 1: Verify Claude is Running on WSL

1. On your WSL, run Claude CLI:
   ```bash
   claude
   ```

2. Verify the service is running on port 35266:
   ```bash
   netstat -tuln | grep 35266
   ```

3. Note down the authentication token:
   ```bash
   cat ~/.claude/ide/35266.lock
   ```

### Step 2: Set up SSH Remote Port Forwarding

1. Open a terminal on your virtual desktop
2. Run the following command:
   ```bash
   ssh -R 35266:localhost:35266 admsia@SEA-1802220383
   ```
   Replace `admsia@SEA-1802220383` with your WSL username and hostname

3. Enter your password when prompted
4. Keep this SSH connection open for the duration of your Claude session

### Step 3: Configure Claude on the Virtual Desktop

1. If Claude is not installed on your virtual desktop, you'll need to install it
2. Create or edit the Claude configuration file:
   ```bash
   mkdir -p ~/.claude
   nano ~/.claude/config
   ```

3. Add the following configuration (adjust as needed):
   ```json
   {
     "serverPort": 35266,
     "authToken": "0595a2a0-d1b5-4cd4-bd6d-d6557cfb15c8"
   }
   ```

4. Save the file and exit

### Step 4: Access Claude on the Virtual Desktop

1. With the SSH tunnel running, open a new terminal on your virtual desktop
2. Run the Claude CLI:
   ```bash
   claude
   ```

3. You should now be connected to the Claude instance running on WSL

## Option 2: SSH Local Port Forwarding

This method is an alternative if Option 1 doesn't work.

### Step 1: Verify Claude is Running on WSL

1. On your WSL, run Claude CLI:
   ```bash
   claude
   ```

2. Verify the service is running on port 35266:
   ```bash
   netstat -tuln | grep 35266
   ```

### Step 2: Set up SSH Local Port Forwarding

1. On your WSL terminal, run:
   ```bash
   ssh -L 35266:localhost:35266 your-vd-username@your-vd-hostname
   ```
   Replace `your-vd-username@your-vd-hostname` with your virtual desktop username and hostname

2. Enter your password when prompted
3. Keep this SSH connection open for the duration of your Claude session

### Step 3: Configure Claude on the Virtual Desktop

1. Follow the same configuration steps as in Option 1, Step 3

## Option 3: Direct Installation (Simplest)

If possible, installing Claude CLI directly on your virtual desktop is the simplest solution.

1. Follow Claude's installation instructions for your virtual desktop OS
2. Configure authentication as needed
3. Run Claude directly on your virtual desktop

## Troubleshooting

### Connection Refused

If you see a "Connection Refused" message:

1. Verify Claude is running on WSL:
   ```bash
   ps aux | grep claude
   ```

2. Check if the port is listening:
   ```bash
   netstat -tuln | grep 35266
   ```

3. Ensure no firewall rules are blocking the connection

### Authentication Errors

If you encounter authentication errors:

1. Verify the authentication token in the virtual desktop's configuration matches the one on WSL:
   ```bash
   cat ~/.claude/ide/35266.lock | grep authToken
   ```

2. Ensure the configuration file on the virtual desktop has the correct format

### Port Already In Use

If port 35266 is already in use:

1. Check what process is using the port:
   ```bash
   lsof -i :35266
   ```

2. Either terminate that process or use a different port for forwarding

## Advanced: Persisting the Connection

To make the connection more persistent:

1. Use `autossh` instead of regular SSH:
   ```bash
   autossh -M 0 -R 35266:localhost:35266 admsia@SEA-1802220383
   ```

2. Consider creating a systemd service on your virtual desktop to maintain the tunnel

## Summary of Key Information

- Claude Server Port: 35266
- Authentication Token: 0595a2a0-d1b5-4cd4-bd6d-d6557cfb15c8
- WSL Hostname: SEA-1802220383
- WSL Username: admsia
