#!/usr/bin/env python3
"""
Simple WebSocket Test Client for CDP Pipeline Workflow

This script connects to the WebSocket server, registers with filters,
and prints all received events to the console.

Usage:
    python3 test_websocket_client.py

Install dependencies:
    pip install websocket-client
"""

import websocket
import json
import sys
import time
from datetime import datetime


def on_message(ws, message):
    """Handle incoming WebSocket messages"""
    try:
        data = json.loads(message)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if data.get('type') == 'welcome':
            print(f"\n[{timestamp}] 📡 WELCOME MESSAGE")
            print(f"Message: {data.get('message')}")
            print(f"Expected format: {json.dumps(data.get('format'), indent=2)}")

            # Send registration message
            print(f"\n[{timestamp}] 📝 SENDING REGISTRATION...")
            registration = {
                'type': 'register',
                'filters': {
                    'types': [],           # Empty = receive all types
                    'account_ids': [],     # Empty = all accounts
                    'asset_codes': [],     # Empty = all assets
                    'min_amount': ''       # Empty = no minimum
                }
            }
            ws.send(json.dumps(registration))
            print(f"Registered with filters: {json.dumps(registration['filters'], indent=2)}")

        elif data.get('type') == 'registered':
            print(f"\n[{timestamp}] ✅ REGISTRATION CONFIRMED")
            print(f"Active filters: {json.dumps(data.get('filters'), indent=2)}")
            print(f"\n[{timestamp}] 👂 LISTENING FOR EVENTS...")
            print("-" * 80)

        else:
            # Regular event message
            print(f"\n[{timestamp}] 📨 EVENT RECEIVED")
            print(json.dumps(data, indent=2))
            print("-" * 80)

    except json.JSONDecodeError as e:
        print(f"Error parsing message: {e}")
        print(f"Raw message: {message}")


def on_error(ws, error):
    """Handle WebSocket errors"""
    print(f"❌ ERROR: {error}")


def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket connection close"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{timestamp}] 🔌 CONNECTION CLOSED")
    if close_status_code or close_msg:
        print(f"Status: {close_status_code}")
        print(f"Message: {close_msg}")


def on_open(ws):
    """Handle WebSocket connection open"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{timestamp}] 🚀 CONNECTED TO WEBSOCKET SERVER")
    print("-" * 80)


def main():
    """Main function to run WebSocket client"""
    # WebSocket server URL
    ws_url = "ws://localhost:8080/ws"

    print("=" * 80)
    print("CDP Pipeline Workflow - WebSocket Test Client")
    print("=" * 80)
    print(f"Connecting to: {ws_url}")
    print("Press Ctrl+C to exit")
    print("=" * 80)

    # Enable debugging (optional)
    # websocket.enableTrace(True)

    try:
        # Create WebSocket connection
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )

        # Run forever (blocking call)
        ws.run_forever(ping_interval=30, ping_timeout=10)

    except KeyboardInterrupt:
        print("\n\n👋 Exiting gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
