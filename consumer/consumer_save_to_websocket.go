package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type Client struct {
	conn         *websocket.Conn
	send         chan []byte
	filters      ClientFilters
	mu           sync.RWMutex
	lastPing     time.Time
	connected    bool
	registered   bool
	registerChan chan struct{}
	messageQueue [][]byte   // Queue to store messages before registration
	queueMu      sync.Mutex // Separate mutex for queue operations
	maxQueueSize int        // Maximum number of messages to queue per client
}

// ClientFilters contains filter criteria for events
type ClientFilters struct {
	Types      []string        `json:"types"`
	AccountIDs []string        `json:"account_ids"`
	AssetCodes []string        `json:"asset_codes"`
	MinAmount  string          `json:"min_amount"`
	Custom     json.RawMessage `json:"custom"`
}

// RegistrationMessage represents the initial registration message
type RegistrationMessage struct {
	Type    string        `json:"type"`    // "register" for registration messages
	Filters ClientFilters `json:"filters"` // Initial filters to apply
}

type SaveToWebSocket struct {
	clients      map[*Client]bool
	broadcast    chan []byte
	register     chan *Client
	unregister   chan *Client
	processors   []processor.Processor
	upgrader     websocket.Upgrader
	hub          *Hub
	maxQueueSize int // Maximum number of messages to queue per client
}

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

func NewHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			log.Printf("New client connected, awaiting registration...")
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				log.Printf("Client unregistered, total clients: %d", len(h.clients))
			}
		case message := <-h.broadcast:
			for client := range h.clients {
				if !client.connected || !client.registered {
					continue // Skip unregistered clients
				}
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}

func NewSaveToWebSocket(config map[string]interface{}) (*SaveToWebSocket, error) {
	port, ok := config["port"].(string)
	if !ok {
		port = "8080"
	}

	path, ok := config["path"].(string)
	if !ok {
		path = "/ws"
	}

	maxQueueSize, ok := config["max_queue_size"].(int)
	if !ok {
		maxQueueSize = 1000 // Default queue size
	}

	hub := NewHub()
	go hub.run()

	ws := &SaveToWebSocket{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		hub:          hub,
		maxQueueSize: maxQueueSize,
	}

	http.HandleFunc(path, ws.handleWebSocket)
	go func() {
		addr := fmt.Sprintf(":%s", port)
		log.Printf("Starting WebSocket server on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("WebSocket server error: %v", err)
		}
	}()

	return ws, nil
}

func (w *SaveToWebSocket) handleWebSocket(rw http.ResponseWriter, req *http.Request) {
	conn, err := w.upgrader.Upgrade(rw, req, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	client := &Client{
		conn:         conn,
		send:         make(chan []byte, 256),
		connected:    true,
		lastPing:     time.Now(),
		registered:   false,
		registerChan: make(chan struct{}),
	}

	// Start registration timeout timer
	regTimer := time.NewTimer(10 * time.Second)
	go func() {
		select {
		case <-regTimer.C:
			if !client.registered {
				log.Printf("Client registration timeout")
				client.conn.Close()
				return
			}
		case <-client.registerChan:
			regTimer.Stop()
		}
	}()

	// Register client
	w.hub.register <- client

	// Send welcome message with registration instructions
	welcome := map[string]interface{}{
		"type":    "welcome",
		"message": "Please send registration message to begin receiving events",
		"format": map[string]interface{}{
			"type": "register",
			"filters": map[string]interface{}{
				"types":       []string{},
				"account_ids": []string{},
				"asset_codes": []string{},
				"min_amount":  "",
			},
		},
	}
	welcomeJSON, _ := json.Marshal(welcome)
	client.send <- welcomeJSON

	// Handle client messages
	go w.readPump(client)
	go w.writePump(client)
}

func (w *SaveToWebSocket) readPump(client *Client) {
	defer func() {
		w.hub.unregister <- client
		client.conn.Close()
	}()

	client.conn.SetReadLimit(512 * 1024)
	client.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	client.conn.SetPongHandler(func(string) error {
		client.mu.Lock()
		client.lastPing = time.Now()
		client.mu.Unlock()
		client.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := client.conn.ReadMessage()
		if err != nil {
			// ... error handling ...
			break
		}

		// Parse message
		var regMsg RegistrationMessage
		if err := json.Unmarshal(message, &regMsg); err != nil {
			log.Printf("Error parsing message: %v", err)
			continue
		}

		client.mu.Lock()
		if regMsg.Type == "register" {
			if !client.registered {
				client.registered = true
				client.filters = regMsg.Filters
				close(client.registerChan)
				log.Printf("Client registered with filters: %+v", client.filters)

				// Process queued messages that match the filters
				go func() {
					client.queueMu.Lock()
					queued := client.messageQueue
					client.messageQueue = nil
					client.queueMu.Unlock()

					for _, msg := range queued {
						var data map[string]interface{}
						if err := json.Unmarshal(msg, &data); err != nil {
							log.Printf("Error unmarshaling queued message: %v", err)
							continue
						}

						if w.shouldSendToClient(client, data) {
							select {
							case client.send <- msg:
								// Message sent successfully
							default:
								log.Printf("Failed to send queued message - channel full")
							}
						}
					}
				}()

				// Send confirmation
				confirm := map[string]interface{}{
					"type":    "registered",
					"message": "Registration successful, processing queued events",
					"filters": client.filters,
				}
				confirmJSON, _ := json.Marshal(confirm)
				client.send <- confirmJSON
			} else {
				// Update filters for already registered client
				client.filters = regMsg.Filters
				log.Printf("Client filters updated: %+v", client.filters)
			}
		}
		client.mu.Unlock()
	}
}

// Add queue management methods to SaveToWebSocket
func (w *SaveToWebSocket) cleanupQueues() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		for client := range w.hub.clients {
			client.queueMu.Lock()
			if len(client.messageQueue) > w.maxQueueSize {
				// Remove oldest messages if queue is too large
				client.messageQueue = client.messageQueue[len(client.messageQueue)-w.maxQueueSize:]
			}
			client.queueMu.Unlock()
		}
	}
}

func (w *SaveToWebSocket) writePump(client *Client) {
	ticker := time.NewTicker(time.Second * 30)
	defer func() {
		ticker.Stop()
		client.conn.Close()
	}()

	for {
		select {
		case message, ok := <-client.send:
			client.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				client.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := client.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			if _, err := w.Write(message); err != nil {
				return
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			client.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := client.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

			client.mu.RLock()
			lastPing := client.lastPing
			client.mu.RUnlock()

			if time.Since(lastPing) > 90*time.Second {
				log.Printf("Client timed out")
				return
			}
		}
	}
}

func (c *Client) queueMessage(msg []byte) error {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()

	// Check queue size limit
	if len(c.messageQueue) >= c.maxQueueSize {
		return fmt.Errorf("message queue full")
	}

	// Make a copy of the message for queuing
	msgCopy := make([]byte, len(msg))
	copy(msgCopy, msg)
	c.messageQueue = append(c.messageQueue, msgCopy)
	return nil
}

func (c *Client) processQueuedMessages() {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()

	// Process all queued messages
	for _, msg := range c.messageQueue {
		select {
		case c.send <- msg:
			// Message sent successfully
		default:
			log.Printf("Failed to send queued message - channel full")
		}
	}

	// Clear the queue
	c.messageQueue = nil
}

func (w *SaveToWebSocket) Subscribe(processor processor.Processor) {
	w.processors = append(w.processors, processor)
}

func (w *SaveToWebSocket) Process(ctx context.Context, msg processor.Message) error {
	payload, ok := msg.Payload.([]byte)
	if !ok {
		log.Printf("Expected []byte payload, got %T", msg.Payload)
		return nil
	}

	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return fmt.Errorf("error unmarshaling payload: %w", err)
	}

	for client := range w.hub.clients {
		if !client.connected {
			continue
		}

		client.mu.RLock()
		registered := client.registered
		client.mu.RUnlock()

		if !registered {
			// Queue message for unregistered clients if it matches their future filters
			if err := client.queueMessage(payload); err != nil {
				log.Printf("Failed to queue message: %v", err)
			}
			continue
		}

		// For registered clients, check filters and send immediately
		if w.shouldSendToClient(client, data) {
			select {
			case client.send <- payload:
			default:
				close(client.send)
				delete(w.hub.clients, client)
			}
		}
	}

	return nil
}

// Rest of the code (shouldSendToClient and Close methods) remains the same

func (w *SaveToWebSocket) shouldSendToClient(client *Client, data map[string]interface{}) bool {
	client.mu.RLock()
	filters := client.filters
	client.mu.RUnlock()

	// If no filters set, send all messages
	if len(filters.Types) == 0 && len(filters.AccountIDs) == 0 && len(filters.AssetCodes) == 0 {
		return true
	}

	// Check message type
	if msgType, ok := data["type"].(string); ok {
		if len(filters.Types) > 0 {
			typeMatch := false
			for _, t := range filters.Types {
				if t == msgType {
					typeMatch = true
					break
				}
			}
			if !typeMatch {
				return false
			}
		}
	}

	// Check account IDs
	if len(filters.AccountIDs) > 0 {
		accountMatch := false
		// Check both buyer and seller account IDs for payments
		if buyerID, ok := data["buyer_account_id"].(string); ok {
			for _, id := range filters.AccountIDs {
				if id == buyerID {
					accountMatch = true
					break
				}
			}
		}
		if sellerID, ok := data["seller_account_id"].(string); ok {
			for _, id := range filters.AccountIDs {
				if id == sellerID {
					accountMatch = true
					break
				}
			}
		}
		if !accountMatch {
			return false
		}
	}

	// Check asset codes
	if len(filters.AssetCodes) > 0 {
		if assetCode, ok := data["asset_code"].(string); ok {
			assetMatch := false
			for _, code := range filters.AssetCodes {
				if code == assetCode {
					assetMatch = true
					break
				}
			}
			if !assetMatch {
				return false
			}
		}
	}

	return true
}

func (w *SaveToWebSocket) Close() error {
	// Close all client connections
	for client := range w.hub.clients {
		client.conn.Close()
	}
	return nil
}
