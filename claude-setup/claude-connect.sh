#!/bin/bash
# Claude Connection Utility Script

set -e

CLAUDE_PORT=35266
AUTH_TOKEN=$(cat ~/.claude/ide/35266.lock 2>/dev/null | grep -o '"authToken":"[^"]*"' | cut -d'"' -f4 || echo "unknown")
WSL_HOSTNAME=$(hostname)
WSL_USERNAME=$(whoami)

clear
echo "======================================================="
echo "  CLAUDE CONNECTION UTILITY"
echo "======================================================="
echo
echo "This script helps you establish a connection to Claude"
echo "running on your WSL instance from your virtual desktop."
echo
echo "Current Configuration:"
echo "  - Claude Port: $CLAUDE_PORT"
echo "  - Authentication Token: $AUTH_TOKEN"
echo "  - WSL Hostname: $WSL_HOSTNAME"
echo "  - WSL Username: $WSL_USERNAME"
echo

# Check if Claude server is running
if netstat -tuln | grep -q "$CLAUDE_PORT"; then
    echo "✓ Claude server is running on port $CLAUDE_PORT"
else
    echo "✗ Claude server is NOT running on port $CLAUDE_PORT"
    echo "  You should start Claude first by running the 'claude' command"
    echo
    read -p "Would you like to start Claude now? (y/n): " start_claude
    if [[ "$start_claude" == "y" ]]; then
        echo "Starting Claude..."
        claude &
        sleep 3
        # Re-check if Claude server is running
        if netstat -tuln | grep -q "$CLAUDE_PORT"; then
            echo "✓ Claude server is now running on port $CLAUDE_PORT"
        else
            echo "✗ Failed to start Claude server"
        fi
    fi
fi

echo
echo "Connection Options:"
echo "  1) Connect from Virtual Desktop to WSL (Remote Port Forwarding)"
echo "  2) Connect from WSL to Virtual Desktop (Local Port Forwarding)"
echo "  3) Generate connection instructions only"
echo "  4) Exit"
echo

read -p "Select an option (1-4): " option

case $option in
    1)
        clear
        echo "======================================================="
        echo "  REMOTE PORT FORWARDING SETUP"
        echo "======================================================="
        echo
        echo "This option is for connecting FROM your virtual desktop TO your WSL."
        echo
        echo "Instructions for your virtual desktop:"
        echo "--------------------------------------"
        echo "Run this command on your virtual desktop:"
        echo
        echo "ssh -R $CLAUDE_PORT:localhost:$CLAUDE_PORT $WSL_USERNAME@$WSL_HOSTNAME"
        echo
        echo "After connecting:"
        echo "1. Claude will be accessible at localhost:$CLAUDE_PORT on your virtual desktop"
        echo "2. Use the authentication token: $AUTH_TOKEN"
        echo
        echo "Press any key to continue..."
        read -n 1
        ;;
    2)
        clear
        echo "======================================================="
        echo "  LOCAL PORT FORWARDING SETUP"
        echo "======================================================="
        echo
        echo "This option is for connecting FROM your WSL TO your virtual desktop."
        echo
        read -p "Enter your virtual desktop username: " vd_username
        read -p "Enter your virtual desktop hostname or IP: " vd_hostname
        
        echo
        echo "Connecting to virtual desktop..."
        echo "ssh -L $CLAUDE_PORT:localhost:$CLAUDE_PORT $vd_username@$vd_hostname"
        echo
        echo "After connecting:"
        echo "1. On your virtual desktop, start Claude configured to use localhost:$CLAUDE_PORT"
        echo "2. Use the authentication token: $AUTH_TOKEN"
        echo
        echo "Attempting connection now..."
        ssh -L $CLAUDE_PORT:localhost:$CLAUDE_PORT $vd_username@$vd_hostname
        ;;
    3)
        cat > ~/claude-connection-instructions.txt << EOL
# Claude Connection Instructions

## Authentication Details
- Claude Port: $CLAUDE_PORT
- Authentication Token: $AUTH_TOKEN
- WSL Hostname: $WSL_HOSTNAME
- WSL Username: $WSL_USERNAME

## Option 1: Connect from Virtual Desktop to WSL
Run on virtual desktop:
\`\`\`
ssh -R $CLAUDE_PORT:localhost:$CLAUDE_PORT $WSL_USERNAME@$WSL_HOSTNAME
\`\`\`

## Option 2: Connect from WSL to Virtual Desktop
Run on WSL:
\`\`\`
ssh -L $CLAUDE_PORT:localhost:$CLAUDE_PORT your_vd_username@your_vd_hostname
\`\`\`

## Configuration on Virtual Desktop
If you have Claude CLI on your virtual desktop:
1. Create/edit ~/.claude/config file
2. Ensure it uses localhost:$CLAUDE_PORT endpoint
3. Use the authentication token: $AUTH_TOKEN

## Troubleshooting
- Ensure Claude is running on host system
- Check that SSH server is running on the target system
- Verify there are no firewall rules blocking the port
- Check that the port isn't already in use on either system
EOL
        echo
        echo "Connection instructions saved to ~/claude-connection-instructions.txt"
        echo
        echo "Press any key to view the instructions..."
        read -n 1
        clear
        cat ~/claude-connection-instructions.txt
        echo
        echo "Press any key to continue..."
        read -n 1
        ;;
    4)
        echo "Exiting..."
        exit 0
        ;;
    *)
        echo "Invalid option. Exiting..."
        exit 1
        ;;
esac

echo
echo "Done!"
