# WebSocket Streaming Consumer Guide

The `SaveToWebSocket` consumer creates a WebSocket server that streams blockchain events to connected clients in real-time. Clients can apply filters to receive only the events they're interested in.

---

## How It Works

1. **Pipeline processes events** → RPC source fetches contract events
2. **WebSocket server broadcasts** → Events are sent to all connected clients
3. **Clients filter events** → Each client registers with filters to receive specific events
4. **Real-time streaming** → Events are pushed to clients as they arrive

---

## Configuration

### Basic Configuration

```yaml
consumers:
  - type: SaveToWebSocket
    config:
      port: "8080"           # Port to listen on (default: 8080)
      path: "/ws"            # WebSocket endpoint (default: /ws)
      max_queue_size: 1000   # Messages to queue per client (default: 1000)
```

### Connection URL

Once running, clients connect to:
```
ws://localhost:8080/ws
```

For remote servers:
```
ws://your-server.com:8080/ws
```

---

## Client Registration Flow

### Step 1: Connect to WebSocket

Client connects to `ws://localhost:8080/ws`

### Step 2: Receive Welcome Message

Server sends welcome message with registration instructions:

```json
{
  "type": "welcome",
  "message": "Please send registration message to begin receiving events",
  "format": {
    "type": "register",
    "filters": {
      "types": [],
      "account_ids": [],
      "asset_codes": [],
      "min_amount": ""
    }
  }
}
```

### Step 3: Send Registration Message

Client must register within **10 seconds** or connection is closed:

```json
{
  "type": "register",
  "filters": {
    "types": ["payment", "trade"],
    "account_ids": ["GABC123...", "GDEF456..."],
    "asset_codes": ["USDC", "XLM"],
    "min_amount": "100"
  }
}
```

**Filter Fields:**
- `types` - Array of event types to receive (e.g., "payment", "trade", "contract_event")
- `account_ids` - Array of Stellar account IDs to filter by
- `asset_codes` - Array of asset codes to filter by
- `min_amount` - Minimum amount threshold for filtering
- Empty arrays = no filtering on that field

### Step 4: Receive Confirmation

Server confirms registration and processes any queued events:

```json
{
  "type": "registered",
  "message": "Registration successful, processing queued events",
  "filters": {
    "types": ["payment", "trade"],
    "account_ids": ["GABC123..."],
    "asset_codes": ["USDC"],
    "min_amount": "100"
  }
}
```

### Step 5: Receive Events

Server streams events matching your filters:

```json
{
  "type": "contract_event",
  "ledger": 1454282,
  "transaction_hash": "abc123...",
  "contract_id": "CABC...",
  "topics": [...],
  "data": {...}
}
```

---

## Client Examples

### JavaScript (Browser)

```javascript
// Connect to WebSocket
const ws = new WebSocket('ws://localhost:8080/ws');

// Handle connection open
ws.onopen = () => {
  console.log('Connected to WebSocket server');

  // Register with filters
  ws.send(JSON.stringify({
    type: 'register',
    filters: {
      types: ['contract_event'],
      account_ids: [],
      asset_codes: ['USDC'],
      min_amount: ''
    }
  }));
};

// Handle incoming messages
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);

  if (data.type === 'welcome') {
    console.log('Received welcome:', data.message);
  } else if (data.type === 'registered') {
    console.log('Registration confirmed:', data.filters);
  } else {
    console.log('Received event:', data);
    // Process your event here
  }
};

// Handle errors
ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

// Handle close
ws.onclose = () => {
  console.log('WebSocket connection closed');
};
```

### Python

```python
import websocket
import json
import time

def on_message(ws, message):
    data = json.loads(message)

    if data['type'] == 'welcome':
        print(f"Welcome: {data['message']}")

        # Send registration
        registration = {
            'type': 'register',
            'filters': {
                'types': ['contract_event'],
                'account_ids': [],
                'asset_codes': ['USDC'],
                'min_amount': ''
            }
        }
        ws.send(json.dumps(registration))

    elif data['type'] == 'registered':
        print(f"Registered with filters: {data['filters']}")

    else:
        print(f"Received event: {data}")
        # Process your event here

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected to WebSocket server")

# Create WebSocket connection
ws = websocket.WebSocketApp(
    "ws://localhost:8080/ws",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

# Run forever
ws.run_forever()
```

**Install dependencies:**
```bash
pip install websocket-client
```

### Node.js

```javascript
const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8080/ws');

ws.on('open', () => {
  console.log('Connected to WebSocket server');

  // Send registration
  ws.send(JSON.stringify({
    type: 'register',
    filters: {
      types: ['contract_event'],
      account_ids: [],
      asset_codes: ['USDC'],
      min_amount: ''
    }
  }));
});

ws.on('message', (data) => {
  const message = JSON.parse(data);

  if (message.type === 'welcome') {
    console.log('Welcome:', message.message);
  } else if (message.type === 'registered') {
    console.log('Registered with filters:', message.filters);
  } else {
    console.log('Received event:', message);
    // Process your event here
  }
});

ws.on('error', (error) => {
  console.error('WebSocket error:', error);
});

ws.on('close', () => {
  console.log('Connection closed');
});
```

**Install dependencies:**
```bash
npm install ws
```

### Go

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "github.com/gorilla/websocket"
)

type RegistrationMessage struct {
    Type    string        `json:"type"`
    Filters ClientFilters `json:"filters"`
}

type ClientFilters struct {
    Types      []string `json:"types"`
    AccountIDs []string `json:"account_ids"`
    AssetCodes []string `json:"asset_codes"`
    MinAmount  string   `json:"min_amount"`
}

func main() {
    // Connect to WebSocket
    c, _, err := websocket.DefaultDialer.Dial("ws://localhost:8080/ws", nil)
    if err != nil {
        log.Fatal("dial:", err)
    }
    defer c.Close()

    // Read welcome message
    _, message, err := c.ReadMessage()
    if err != nil {
        log.Fatal("read:", err)
    }
    fmt.Printf("Welcome: %s\n", message)

    // Send registration
    registration := RegistrationMessage{
        Type: "register",
        Filters: ClientFilters{
            Types:      []string{"contract_event"},
            AccountIDs: []string{},
            AssetCodes: []string{"USDC"},
            MinAmount:  "",
        },
    }

    regJSON, _ := json.Marshal(registration)
    err = c.WriteMessage(websocket.TextMessage, regJSON)
    if err != nil {
        log.Fatal("write:", err)
    }

    // Read messages
    for {
        _, message, err := c.ReadMessage()
        if err != nil {
            log.Println("read:", err)
            return
        }

        var data map[string]interface{}
        json.Unmarshal(message, &data)
        fmt.Printf("Received: %v\n", data)
    }
}
```

---

## Filter Examples

### No Filters (Receive All Events)

```json
{
  "type": "register",
  "filters": {
    "types": [],
    "account_ids": [],
    "asset_codes": [],
    "min_amount": ""
  }
}
```

### Specific Event Types

```json
{
  "type": "register",
  "filters": {
    "types": ["payment", "trade", "contract_event"],
    "account_ids": [],
    "asset_codes": [],
    "min_amount": ""
  }
}
```

### Specific Accounts

```json
{
  "type": "register",
  "filters": {
    "types": [],
    "account_ids": [
      "GABC123...",
      "GDEF456...",
      "GHIJ789..."
    ],
    "asset_codes": [],
    "min_amount": ""
  }
}
```

### Specific Assets

```json
{
  "type": "register",
  "filters": {
    "types": [],
    "account_ids": [],
    "asset_codes": ["USDC", "XLM", "BTC"],
    "min_amount": ""
  }
}
```

### Combined Filters

Only payments for USDC over 100 units:

```json
{
  "type": "register",
  "filters": {
    "types": ["payment"],
    "account_ids": [],
    "asset_codes": ["USDC"],
    "min_amount": "100"
  }
}
```

### Update Filters

Send a new registration message to update filters:

```json
{
  "type": "register",
  "filters": {
    "types": ["trade"],
    "account_ids": [],
    "asset_codes": ["XLM"],
    "min_amount": "1000"
  }
}
```

---

## Features

### Message Queueing

- Events are queued for **unregistered clients** (up to `max_queue_size`)
- Once registered, queued messages matching filters are sent
- Prevents missing events during registration process

### Heartbeat / Keep-Alive

- Server sends **ping** every 30 seconds
- Client must respond with **pong**
- Connection closed if no response after 90 seconds
- Most WebSocket libraries handle this automatically

### Connection Management

- **10-second timeout** for registration
- Clients must register within 10 seconds or connection is closed
- Prevents resource waste from unregistered connections

### Concurrent Clients

- Supports **unlimited concurrent clients**
- Each client has independent filters
- Messages broadcast to all matching clients

---

## Complete Pipeline Example

```yaml
pipelines:
  ContractEventsWebSocket:
    source:
      type: RPCSourceAdapter
      config:
        rpc_url: "https://rpc-mainnet.stellar.org"
        auth_header: ""
        start_ledger: 2000000
        poll_interval: 5
        rpc_method: "getEvents"

        filters:
          - type: "contract"
            contractIds:
              - "CCW67TSZV3SSS2HXMBQ5JFGCKJNXKZM7UQUWUZPUTHXSTZLEO7SJMI75"  # USDC
            topics:
              - ["*"]

        pagination:
          limit: 1000

        xdrFormat: "json"

    processors:
      - type: GetEventsRPC
        config: {}

    consumers:
      # Stream to WebSocket for real-time clients
      - type: SaveToWebSocket
        config:
          port: "8080"
          path: "/ws"
          max_queue_size: 5000

      # Also save to database for historical queries
      - type: SaveToPostgreSQL
        config:
          host: "localhost"
          database: "stellar_events"
          username: "postgres"
          password: "password"
```

---

## Docker Usage

### Run Pipeline with WebSocket

```bash
docker run --rm \
  -p 8080:8080 \
  -v $(pwd)/config:/app/config \
  withobsrvr/obsrvr-flow-pipeline:latest \
  /app/cdp-pipeline-workflow -config /app/config/base/websocket_streaming.yaml
```

**Note:** Use `-p 8080:8080` to expose the WebSocket port.

### Connect from Host

```bash
# Test connection with websocat
websocat ws://localhost:8080/ws

# Or with curl
curl --include \
  --no-buffer \
  --header "Connection: Upgrade" \
  --header "Upgrade: websocket" \
  --header "Sec-WebSocket-Key: SGVsbG8sIHdvcmxkIQ==" \
  --header "Sec-WebSocket-Version: 13" \
  http://localhost:8080/ws
```

---

## Troubleshooting

### Connection Refused

**Error:** `Connection refused`

**Fix:** Ensure port is exposed in Docker:
```bash
docker run -p 8080:8080 ...
```

### Registration Timeout

**Error:** Connection closed after 10 seconds

**Fix:** Send registration message immediately after connecting:
```javascript
ws.onopen = () => {
  ws.send(JSON.stringify({
    type: 'register',
    filters: {...}
  }));
};
```

### No Events Received

**Possible causes:**
1. Filters too restrictive - try with empty filters first
2. No events matching filters in current ledger range
3. Client not registered - check registration confirmation

### Message Queue Full

**Error:** `message queue full`

**Fix:** Increase `max_queue_size` in config:
```yaml
config:
  max_queue_size: 10000  # Increase from default 1000
```

---

## Best Practices

1. **Register Immediately:** Send registration within 10 seconds of connecting
2. **Handle Reconnection:** Implement auto-reconnect logic for production
3. **Filter Early:** Use filters to reduce bandwidth and processing
4. **Monitor Queue:** Watch for queue full errors in logs
5. **Multiple Consumers:** Use WebSocket + Database for complete solution
6. **Error Handling:** Gracefully handle connection drops and errors

---

## Use Cases

### Real-Time Dashboard

Stream events to web dashboard:
- Connect browser clients via JavaScript
- Display live contract activity
- Update UI in real-time

### Trading Bots

Monitor DEX trades:
- Filter for specific trading pairs
- React to price changes instantly
- Execute automated trading strategies

### Notification System

Alert on specific events:
- Filter for large transfers
- Send push notifications
- Track user-specific activity

### Data Analytics

Real-time analytics pipeline:
- Stream events to analytics engine
- Calculate metrics on-the-fly
- Update dashboards in real-time

---

## Performance Notes

- **Low Latency:** Events streamed immediately upon arrival
- **Scalable:** Supports many concurrent clients
- **Efficient:** Only matching events sent to each client
- **Reliable:** Message queuing prevents missed events during registration

For high-volume scenarios, consider using multiple consumers:
- WebSocket for real-time streaming
- PostgreSQL for historical queries
- Redis for caching

---

## Next Steps

- Test with simple client (JavaScript/Python)
- Implement reconnection logic
- Add error handling
- Build your real-time application!
