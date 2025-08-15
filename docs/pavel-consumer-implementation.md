# PAVEL Consumer Implementation Guide
## Public, Append-Only, Verifiable Event Log for StellarCarbon

This document describes the implementation of a CDP Pipeline consumer that creates a tamper-evident, publicly accessible event log for StellarCarbon's sink_carbon contract invocations.

## Overview

The PAVEL consumer processes filtered `ContractInvocation` messages and:
1. Transforms them into canonical events with reverse-linked CIDs
2. Maintains a hot storage layer (last 24h) for real-time access
3. Seals immutable segments with cryptographic signatures
4. Creates IPFS CAR files for content-addressed storage
5. Optionally anchors Merkle roots on-chain for additional trust

## Architecture

```
ContractInvocation → PAVELConsumer → {
    Hot Storage (NDJSON + index)
    Cold Storage (sealed segments)  
    IPFS Archive (CAR files)
    Signature & Anchoring
}
```

## Implementation

### 1. Consumer Structure

```go
// consumer/consumer_pavel.go
package consumer

import (
    "compress/gzip"
    "context"
    "crypto/ed25519"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "path/filepath"
    "sync"
    "time"
    
    "github.com/pkg/errors"
    "github.com/stellar/go/clients/stellarrpc"
    "github.com/stellar/go/keypair"
    "github.com/stellar/go/network"
    "github.com/stellar/go/txnbuild"
    
    // IPFS dependencies
    "github.com/ipfs/go-cid"
    "github.com/ipfs/go-ipld-cbor"
    "github.com/ipld/go-car/v2"
    "github.com/multiformats/go-multihash"
    
    // Cloud storage
    "cloud.google.com/go/storage"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    
    "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type PAVELConsumer struct {
    config          PAVELConfig
    processors      []processor.Processor
    
    // Storage clients
    storageClient   StorageClient
    ipfsClient      IPFSClient
    
    // Current state
    currentSegment  *Segment
    hotFile         *HotFile
    lastCID         string
    lastTOID        uint64
    
    // Signing
    signingKey      ed25519.PrivateKey
    
    // Stellar anchoring (optional)
    anchorKeypair   *keypair.Full
    rpcClient       *stellarrpc.Client
    
    // Synchronization
    mu              sync.RWMutex
    stats           ConsumerStats
}

type PAVELConfig struct {
    // Network configuration
    Network             string   `yaml:"network"`              // mainnet/testnet
    StreamID            string   `yaml:"stream_id"`            // e.g., "stellarcarbon/sink_carbon"
    
    // Contract tracking
    ContractIDs         []string `yaml:"contract_ids"`         // List of contract IDs to track
    FunctionFilter      string   `yaml:"function_filter"`      // e.g., "sink_carbon"
    SchemaVersion       int      `yaml:"schema_version"`       // Current schema version
    
    // Storage configuration
    StorageType         string   `yaml:"storage_type"`         // "gcs", "s3", "r2"
    BucketName          string   `yaml:"bucket_name"`
    PublicURL           string   `yaml:"public_url"`           // Base URL for public access
    
    // Segmentation
    EventsPerSegment    int      `yaml:"events_per_segment"`   // Default: 10000
    SegmentRotationMins int      `yaml:"segment_rotation_mins"`// Default: 60
    HotWindowHours      int      `yaml:"hot_window_hours"`     // Default: 24
    
    // IPFS configuration
    IPFSGateway         string   `yaml:"ipfs_gateway"`         // IPFS HTTP API endpoint
    IPFSPinService      string   `yaml:"ipfs_pin_service"`     // Optional pinning service
    
    // Signing & verification
    SigningKeyPath      string   `yaml:"signing_key_path"`     // Ed25519 private key file
    
    // Stellar anchoring (optional)
    AnchorEnabled       bool     `yaml:"anchor_enabled"`
    AnchorSecretKey     string   `yaml:"anchor_secret_key"`    // Stellar secret key
    AnchorNetwork       string   `yaml:"anchor_network"`       // "public" or "testnet"
    AnchorThreshold     int      `yaml:"anchor_threshold"`     // Anchor every N segments
}

type ConsumerStats struct {
    EventsProcessed     uint64
    SegmentsSealed      uint32
    LastEventTime       time.Time
    LastSegmentTime     time.Time
    LastAnchorTxHash    string
    LastAnchorTime      time.Time
}
```

### 2. Canonical Event Structure

```go
// StellarCarbonEvent represents the canonical event format
type StellarCarbonEvent struct {
    // Core fields (immutable across schema versions)
    TOID            uint64    `json:"toid"`
    Ledger          uint32    `json:"ledger"`
    Timestamp       time.Time `json:"timestamp"`
    ContractID      string    `json:"contract_id"`
    TransactionHash string    `json:"transaction_hash"`
    Function        string    `json:"function"`
    SchemaVersion   int       `json:"schema_version"`
    ContractVersion int       `json:"contract_version"`
    
    // Business data (schema-versioned)
    Data            SinkCarbonData `json:"data"`
    
    // Chain linkage
    PrevCID         string    `json:"prev_cid"`
}

// SinkCarbonData represents the sink_carbon function arguments
type SinkCarbonData struct {
    Funder      string `json:"funder"`
    Recipient   string `json:"recipient"`
    Amount      int64  `json:"amount"`
    ProjectID   string `json:"project_id"`
    MemoText    string `json:"memo_text"`
    Email       string `json:"email"`
}

// EventWithMetadata wraps an event with processing metadata
type EventWithMetadata struct {
    Event       StellarCarbonEvent `json:"event"`
    CID         string             `json:"cid"`
    SegmentID   uint32             `json:"segment_id"`
    IndexOffset int64              `json:"index_offset"`
}
```

### 3. Core Processing Logic

```go
func NewPAVELConsumer(config map[string]interface{}) (*PAVELConsumer, error) {
    var cfg PAVELConfig
    if err := parseConfig(config, &cfg); err != nil {
        return nil, errors.Wrap(err, "error parsing PAVEL config")
    }
    
    // Initialize storage client
    storageClient, err := NewStorageClient(cfg)
    if err != nil {
        return nil, errors.Wrap(err, "error creating storage client")
    }
    
    // Load signing key
    signingKey, err := loadSigningKey(cfg.SigningKeyPath)
    if err != nil {
        return nil, errors.Wrap(err, "error loading signing key")
    }
    
    // Initialize Stellar RPC client if anchoring is enabled
    var anchorKeypair *keypair.Full
    var rpcClient *stellarrpc.Client
    if cfg.AnchorEnabled {
        anchorKeypair, err = keypair.ParseFull(cfg.AnchorSecretKey)
        if err != nil {
            return nil, errors.Wrap(err, "error parsing anchor keypair")
        }
        
        rpcURL := getRPCURL(cfg.AnchorNetwork)
        rpcClient = &stellarrpc.Client{
            URL:  rpcURL,
            HTTP: &http.Client{Timeout: 30 * time.Second},
        }
    }
    
    consumer := &PAVELConsumer{
        config:        cfg,
        storageClient: storageClient,
        signingKey:    signingKey,
        anchorKeypair: anchorKeypair,
        rpcClient:     rpcClient,
    }
    
    // Initialize hot file
    if err := consumer.initializeHotFile(); err != nil {
        return nil, errors.Wrap(err, "error initializing hot file")
    }
    
    // Load last state
    if err := consumer.loadLastState(); err != nil {
        return nil, errors.Wrap(err, "error loading last state")
    }
    
    return consumer, nil
}

func (c *PAVELConsumer) Process(ctx context.Context, msg processor.Message) error {
    // Extract ExtractedContractInvocation from message
    extracted, ok := msg.Payload.(*processor.ExtractedContractInvocation)
    if !ok {
        return nil // Skip non-extracted invocation messages
    }
    
    // Filter by contract ID and function
    if !c.shouldProcess(extracted) {
        return nil
    }
    
    // Transform to canonical event
    event, err := c.transformToCanonicalEvent(extracted)
    if err != nil {
        return errors.Wrap(err, "error transforming invocation")
    }
    
    // Add chain linkage
    c.mu.Lock()
    event.PrevCID = c.lastCID
    c.mu.Unlock()
    
    // Calculate CID for this event
    eventCID, err := c.calculateEventCID(event)
    if err != nil {
        return errors.Wrap(err, "error calculating event CID")
    }
    
    // Write to hot storage
    indexOffset, err := c.appendToHotStorage(event)
    if err != nil {
        return errors.Wrap(err, "error appending to hot storage")
    }
    
    // Update state
    c.mu.Lock()
    c.lastCID = eventCID
    c.lastTOID = event.TOID
    c.stats.EventsProcessed++
    c.stats.LastEventTime = time.Now()
    
    // Check if we need to seal current segment
    shouldSeal := c.shouldSealSegment()
    c.mu.Unlock()
    
    if shouldSeal {
        if err := c.sealAndRotateSegment(ctx); err != nil {
            return errors.Wrap(err, "error sealing segment")
        }
    }
    
    return nil
}

func (c *PAVELConsumer) shouldProcess(extracted *processor.ExtractedContractInvocation) bool {
    // Check if contract is in our tracked list
    for _, contractID := range c.config.ContractIDs {
        if extracted.ContractID == contractID {
            // Check function filter
            if c.config.FunctionFilter == "" || extracted.FunctionName == c.config.FunctionFilter {
                // Verify it's a successful invocation
                return extracted.Successful
            }
        }
    }
    return false
}

func (c *PAVELConsumer) transformToCanonicalEvent(extracted *processor.ExtractedContractInvocation) (*StellarCarbonEvent, error) {
    // TOID is already calculated by the extractor
    toid := extracted.Toid
    
    // Determine contract version based on contract ID
    contractVersion := c.getContractVersion(extracted.ContractID)
    
    // Parse timestamp
    timestamp, err := time.Parse(time.RFC3339, extracted.Timestamp)
    if err != nil {
        return nil, errors.Wrap(err, "error parsing timestamp")
    }
    
    // Map extracted fields to SinkCarbonData
    data := SinkCarbonData{
        Funder:    extracted.Funder,
        Recipient: extracted.Recipient,
        Amount:    int64(extracted.Amount), // Convert uint64 to int64
        ProjectID: extracted.ProjectID,
        MemoText:  extracted.MemoText,
        Email:     extracted.Email,
    }
    
    // Validate required fields
    if data.Funder == "" || data.Recipient == "" || data.Amount == 0 {
        return nil, fmt.Errorf("missing required fields in sink_carbon data")
    }
    
    return &StellarCarbonEvent{
        TOID:            toid,
        Ledger:          extracted.Ledger,
        Timestamp:       timestamp,
        ContractID:      extracted.ContractID,
        TransactionHash: extracted.TxHash,
        Function:        extracted.FunctionName,
        SchemaVersion:   c.config.SchemaVersion,
        ContractVersion: contractVersion,
        Data:            data,
    }, nil
}
```

### 4. Hot Storage Management

```go
type HotFile struct {
    file       *os.File
    gzWriter   *gzip.Writer
    jsonWriter *json.Encoder
    index      *ByteOffsetIndex
    mu         sync.Mutex
    
    currentSize int64
    eventCount  int
    startTime   time.Time
}

type ByteOffsetIndex struct {
    entries []IndexEntry
    file    *os.File
}

type IndexEntry struct {
    TOID   uint64 `json:"toid"`
    Offset int64  `json:"offset"`
    Length int32  `json:"length"`
}

func (c *PAVELConsumer) appendToHotStorage(event *StellarCarbonEvent) (int64, error) {
    c.hotFile.mu.Lock()
    defer c.hotFile.mu.Unlock()
    
    // Get current offset
    startOffset := c.hotFile.currentSize
    
    // Marshal event to JSON
    eventJSON, err := json.Marshal(event)
    if err != nil {
        return 0, err
    }
    
    // Write to NDJSON (newline-delimited JSON)
    if _, err := c.hotFile.file.Write(eventJSON); err != nil {
        return 0, err
    }
    if _, err := c.hotFile.file.Write([]byte("\n")); err != nil {
        return 0, err
    }
    
    // Update index
    indexEntry := IndexEntry{
        TOID:   event.TOID,
        Offset: startOffset,
        Length: int32(len(eventJSON) + 1), // +1 for newline
    }
    
    if err := c.hotFile.index.append(indexEntry); err != nil {
        return 0, err
    }
    
    // Update counters
    c.hotFile.currentSize += int64(len(eventJSON) + 1)
    c.hotFile.eventCount++
    
    // Upload to cloud storage (append mode)
    remotePath := fmt.Sprintf("%s/%s/hot/today.ndjson", 
        c.config.StreamID, c.config.Network)
    
    if err := c.storageClient.AppendToObject(remotePath, eventJSON); err != nil {
        return 0, errors.Wrap(err, "error appending to cloud storage")
    }
    
    return startOffset, nil
}

func (c *PAVELConsumer) rotateHotFile() error {
    c.hotFile.mu.Lock()
    defer c.hotFile.mu.Unlock()
    
    // Close current file
    if err := c.hotFile.close(); err != nil {
        return err
    }
    
    // Move to cold storage
    timestamp := c.hotFile.startTime.Format("2006/01/02")
    segmentID := c.getNextSegmentID()
    
    coldPath := fmt.Sprintf("%s/%s/events/%s/%04d.ndjson.gz",
        c.config.StreamID, c.config.Network, timestamp, segmentID)
    
    // Upload to cold storage
    if err := c.uploadToColdStorage(c.hotFile.file.Name(), coldPath); err != nil {
        return err
    }
    
    // Create new hot file
    return c.initializeHotFile()
}
```

### 5. Segment Sealing and IPFS Archive

```go
type Segment struct {
    ID          uint32                `json:"id"`
    Events      []StellarCarbonEvent  `json:"events"`
    StartTOID   uint64                `json:"start_toid"`
    EndTOID     uint64                `json:"end_toid"`
    StartTime   time.Time             `json:"start_time"`
    EndTime     time.Time             `json:"end_time"`
    MerkleRoot  string                `json:"merkle_root"`
    Signature   string                `json:"signature"`
    PrevSegment string                `json:"prev_segment_cid"`
    CID         string                `json:"cid"`
}

func (c *PAVELConsumer) sealAndRotateSegment(ctx context.Context) error {
    c.mu.Lock()
    segment := c.currentSegment
    c.mu.Unlock()
    
    if segment == nil || len(segment.Events) == 0 {
        return nil
    }
    
    // Calculate Merkle root
    merkleRoot, err := c.calculateMerkleRoot(segment.Events)
    if err != nil {
        return errors.Wrap(err, "error calculating merkle root")
    }
    segment.MerkleRoot = hex.EncodeToString(merkleRoot)
    
    // Sign the merkle root
    signature := ed25519.Sign(c.signingKey, merkleRoot)
    segment.Signature = hex.EncodeToString(signature)
    
    // Create IPFS CAR file
    carCID, carPath, err := c.createCARFile(segment)
    if err != nil {
        return errors.Wrap(err, "error creating CAR file")
    }
    segment.CID = carCID
    
    // Upload CAR file to storage
    remoteCARPath := fmt.Sprintf("%s/%s/ipfs/%04d.car",
        c.config.StreamID, c.config.Network, segment.ID)
    
    if err := c.storageClient.UploadFile(carPath, remoteCARPath); err != nil {
        return errors.Wrap(err, "error uploading CAR file")
    }
    
    // Update segment index
    if err := c.updateSegmentIndex(segment); err != nil {
        return errors.Wrap(err, "error updating segment index")
    }
    
    // Optionally anchor on Stellar
    if c.shouldAnchor(segment.ID) {
        if err := c.anchorOnStellar(ctx, segment); err != nil {
            // Log but don't fail
            log.Printf("Warning: failed to anchor segment %d: %v", segment.ID, err)
        }
    }
    
    // Start new segment
    c.mu.Lock()
    c.currentSegment = &Segment{
        ID:          segment.ID + 1,
        Events:      make([]StellarCarbonEvent, 0),
        StartTime:   time.Now(),
        PrevSegment: segment.CID,
    }
    c.stats.SegmentsSealed++
    c.stats.LastSegmentTime = time.Now()
    c.mu.Unlock()
    
    return nil
}

func (c *PAVELConsumer) createCARFile(segment *Segment) (string, string, error) {
    // Create IPLD nodes for each event
    nodes := make([]ipld.Node, len(segment.Events))
    
    for i, event := range segment.Events {
        node, err := cbornode.WrapObject(event, multihash.SHA2_256, -1)
        if err != nil {
            return "", "", err
        }
        nodes[i] = node
    }
    
    // Create root node with segment metadata
    rootData := map[string]interface{}{
        "segment_id":   segment.ID,
        "events_count": len(segment.Events),
        "start_toid":   segment.StartTOID,
        "end_toid":     segment.EndTOID,
        "merkle_root":  segment.MerkleRoot,
        "signature":    segment.Signature,
        "events":       nodes,
    }
    
    rootNode, err := cbornode.WrapObject(rootData, multihash.SHA2_256, -1)
    if err != nil {
        return "", "", err
    }
    
    // Create CAR file
    carPath := filepath.Join(c.getTempDir(), fmt.Sprintf("segment-%04d.car", segment.ID))
    
    f, err := os.Create(carPath)
    if err != nil {
        return "", "", err
    }
    defer f.Close()
    
    // Write CAR v2
    writer, err := car.NewWriter(f, rootNode.Cid())
    if err != nil {
        return "", "", err
    }
    
    // Add all nodes
    if err := writer.Put(rootNode); err != nil {
        return "", "", err
    }
    
    for _, node := range nodes {
        if err := writer.Put(node); err != nil {
            return "", "", err
        }
    }
    
    if err := writer.Close(); err != nil {
        return "", "", err
    }
    
    return rootNode.Cid().String(), carPath, nil
}
```

### 6. Stellar Anchoring

```go
func (c *PAVELConsumer) anchorOnStellar(ctx context.Context, segment *Segment) error {
    if !c.config.AnchorEnabled {
        return nil
    }
    
    // Create anchor data
    anchorData := map[string]string{
        "type":        "stellarcarbon_pavel_anchor",
        "segment_id":  fmt.Sprintf("%d", segment.ID),
        "merkle_root": segment.MerkleRoot,
        "cid":         segment.CID,
        "network":     c.config.Network,
    }
    
    anchorJSON, err := json.Marshal(anchorData)
    if err != nil {
        return err
    }
    
    // Create memo (max 28 bytes for hash memo)
    memoHash := sha256.Sum256(anchorJSON)
    memo := txnbuild.MemoHash(memoHash[:28])
    
    // Get latest ledger for sequence number
    latestLedgerResp, err := c.rpcClient.GetLatestLedger(ctx)
    if err != nil {
        return errors.Wrap(err, "error getting latest ledger")
    }
    
    // Create source account from latest ledger info
    sourceAccount := txnbuild.NewSimpleAccount(
        c.anchorKeypair.Address(),
        int64(latestLedgerResp.Sequence),
    )
    
    // Create transaction
    tx, err := txnbuild.NewTransaction(
        txnbuild.TransactionParams{
            SourceAccount:        &sourceAccount,
            IncrementSequenceNum: true,
            Operations: []txnbuild.Operation{
                &txnbuild.ManageData{
                    Name:  fmt.Sprintf("pavel_seg_%d", segment.ID),
                    Value: []byte(segment.CID),
                },
            },
            Memo:         memo,
            Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(300)},
            BaseFee:      txnbuild.MinBaseFee,
        },
    )
    if err != nil {
        return err
    }
    
    // Sign transaction
    tx, err = tx.Sign(getNetworkPassphrase(c.config.AnchorNetwork), c.anchorKeypair)
    if err != nil {
        return err
    }
    
    // Submit via RPC
    sendTxResp, err := c.rpcClient.SendTransaction(ctx, tx)
    if err != nil {
        return errors.Wrap(err, "error sending transaction")
    }
    
    // Check submission status
    var txHash string
    switch sendTxResp.Status {
    case stellarrpc.SendTransactionStatusSuccess:
        txHash = sendTxResp.Hash
    case stellarrpc.SendTransactionStatusPending:
        // Transaction is pending, we can optionally wait for it
        txHash = sendTxResp.Hash
        log.Printf("Transaction %s is pending", txHash)
    default:
        return fmt.Errorf("transaction submission failed with status: %s", sendTxResp.Status)
    }
    
    // Update stats
    c.mu.Lock()
    c.stats.LastAnchorTxHash = txHash
    c.stats.LastAnchorTime = time.Now()
    c.mu.Unlock()
    
    log.Printf("Anchored segment %d on Stellar: %s", segment.ID, txHash)
    
    return nil
}

// Helper functions

func getRPCURL(network string) string {
    switch network {
    case "public", "mainnet":
        return "https://soroban-rpc.stellar.org"
    case "testnet":
        return "https://soroban-testnet.stellar.org" 
    case "futurenet":
        return "https://soroban-futurenet.stellar.org"
    default:
        return "https://soroban-rpc.stellar.org"
    }
}

func getNetworkPassphrase(network string) string {
    switch network {
    case "public", "mainnet":
        return network.PublicNetworkPassphrase
    case "testnet":
        return network.TestNetworkPassphrase
    case "futurenet":
        return network.FutureNetworkPassphrase
    default:
        return network.PublicNetworkPassphrase
    }
}
```

### 7. Public Index Files

```go
// StreamMetadata is written to stream.json
type StreamMetadata struct {
    Network         string             `json:"network"`
    StreamID        string             `json:"stream_id"`
    ContractLineage []ContractVersion  `json:"contract_lineage"`
    SchemaVersions  []SchemaVersion    `json:"schema_versions"`
    SignerPublicKey string             `json:"signer_public_key"`
    AnchorAccount   string             `json:"anchor_account,omitempty"`
    Created         time.Time          `json:"created"`
    LastUpdated     time.Time          `json:"last_updated"`
}

type ContractVersion struct {
    ContractID string `json:"contract_id"`
    StartTOID  uint64 `json:"start_toid"`
    Version    int    `json:"version"`
}

type SchemaVersion struct {
    Version    int    `json:"version"`
    SinceTOID  uint64 `json:"since_toid"`
    Schema     string `json:"schema"`
}

// SegmentIndex is written to index/segments.json
type SegmentIndex struct {
    Segments []SegmentInfo `json:"segments"`
    Updated  time.Time     `json:"updated"`
}

type SegmentInfo struct {
    ID         uint32    `json:"id"`
    StartTOID  uint64    `json:"start_toid"`
    EndTOID    uint64    `json:"end_toid"`
    CID        string    `json:"cid"`
    Path       string    `json:"path"`
    CARPath    string    `json:"car_path"`
    Timestamp  time.Time `json:"timestamp"`
    EventCount int       `json:"event_count"`
}

func (c *PAVELConsumer) updatePublicIndexes() error {
    // Update stream metadata
    streamMeta := StreamMetadata{
        Network:         c.config.Network,
        StreamID:        c.config.StreamID,
        ContractLineage: c.getContractLineage(),
        SchemaVersions:  c.getSchemaVersions(),
        SignerPublicKey: hex.EncodeToString(c.signingKey.Public().(ed25519.PublicKey)),
        AnchorAccount:   c.getAnchorAccount(),
        LastUpdated:     time.Now(),
    }
    
    if err := c.uploadJSON("stream.json", streamMeta); err != nil {
        return err
    }
    
    // Update HEAD pointer
    head := map[string]interface{}{
        "head_toid": c.lastTOID,
        "head_cid":  c.lastCID,
        "updated":   time.Now(),
    }
    
    if err := c.uploadJSON("HEAD.json", head); err != nil {
        return err
    }
    
    return nil
}
```

### 8. Storage Client Interface

```go
type StorageClient interface {
    // Upload a complete file
    UploadFile(localPath, remotePath string) error
    
    // Append to an existing object (for hot storage)
    AppendToObject(remotePath string, data []byte) error
    
    // Upload JSON object
    UploadJSON(remotePath string, data interface{}) error
    
    // Set object metadata (e.g., content-type, cache-control)
    SetObjectMetadata(remotePath string, metadata map[string]string) error
}

// GCSStorageClient implements StorageClient for Google Cloud Storage
type GCSStorageClient struct {
    client     *storage.Client
    bucket     *storage.BucketHandle
    publicURL  string
}

func (g *GCSStorageClient) UploadFile(localPath, remotePath string) error {
    ctx := context.Background()
    
    f, err := os.Open(localPath)
    if err != nil {
        return err
    }
    defer f.Close()
    
    obj := g.bucket.Object(remotePath)
    w := obj.NewWriter(ctx)
    
    // Set public read ACL if configured
    w.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}
    
    // Set cache control for immutable objects
    if strings.Contains(remotePath, "/events/") || strings.Contains(remotePath, "/ipfs/") {
        w.CacheControl = "public, max-age=31536000, immutable"
    }
    
    if _, err := io.Copy(w, f); err != nil {
        return err
    }
    
    return w.Close()
}
```

## Pipeline Configuration

The PAVEL consumer works with the contract invocation processors in a pipeline:

```yaml
# config/base/stellarcarbon_sink_carbon_pipeline.yaml
pipelines:
  StellarCarbonSinkCarbon:
    source:
      type: BufferedStorageSourceAdapter
      config:
        bucket_name: "obsrvr-stellar-ledger-data-pubnet-data"
        network: "mainnet"
        num_workers: 10
        retry_limit: 3
        retry_wait: 5
        start_ledger: 58466921  # When StellarCarbon started using Soroban
        ledgers_per_file: 1
        files_per_partition: 64000
    
    processors:
      # First, extract all contract invocations
      - type: "ContractInvocation"
        name: "contract-invocation-processor"
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
      
      # Then, extract business data using the schema
      - type: "ContractInvocationExtractor"
        name: "sink-carbon-extractor"
        config:
          schemas:
            - schema_name: "sink_carbon_v1"
              function_name: "sink_carbon"
              contract_ids:
                - "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"
              extractors:
                funder:
                  argument_index: 0
                  field_type: "address"
                  required: true
                recipient:
                  argument_index: 1
                  field_type: "address"
                  required: true
                amount:
                  argument_index: 2
                  field_type: "uint64"
                  required: true
                project_id:
                  argument_index: 3
                  field_type: "symbol"
                  required: true
                memo_text:
                  argument_index: 4
                  field_type: "string"
                  required: true
                email:
                  argument_index: 5
                  field_type: "string"
                  required: true
    
    consumers:
      # Finally, store in PAVEL format
      - type: "PAVELConsumer"
        config:
          # Network configuration
          network: mainnet
          stream_id: stellarcarbon/sink_carbon
          
          # Contract tracking
          contract_ids:
            - "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"
          function_filter: sink_carbon
          schema_version: 1
          
          # Storage configuration
          storage_type: gcs
          bucket_name: stellarcarbon-public-data
          public_url: https://storage.googleapis.com/stellarcarbon-public-data
          
          # Segmentation
          events_per_segment: 10000
          segment_rotation_mins: 60
          hot_window_hours: 24
          
          # IPFS configuration
          ipfs_gateway: https://ipfs.io
          ipfs_pin_service: pinata
          
          # Signing & verification
          signing_key_path: /secrets/pavel_signing_key.pem
          
          # Stellar anchoring
          anchor_enabled: true
          anchor_secret_key: ${ANCHOR_SECRET_KEY}
          anchor_network: public
          anchor_threshold: 100  # Anchor every 100 segments
```

## Configuration Example

For standalone consumer configuration:

```yaml
# config/consumers/stellarcarbon_pavel.yaml
type: PAVELConsumer
config:
  # Network configuration
  network: mainnet
  stream_id: stellarcarbon/sink_carbon
  
  # Contract tracking
  contract_ids:
    - "CASJKXVOKEBFC6HRNLLZKMEFJXYS3S5GOXM5DQRD7NDPIOQHCPAOLH7O"
  function_filter: sink_carbon
  schema_version: 1
  
  # Storage configuration
  storage_type: gcs
  bucket_name: stellarcarbon-public-data
  public_url: https://storage.googleapis.com/stellarcarbon-public-data
  
  # Segmentation
  events_per_segment: 10000
  segment_rotation_mins: 60
  hot_window_hours: 24
  
  # IPFS configuration
  ipfs_gateway: https://ipfs.io
  ipfs_pin_service: pinata
  
  # Signing & verification
  signing_key_path: /secrets/pavel_signing_key.pem
  
  # Stellar anchoring
  anchor_enabled: true
  anchor_secret_key: ${ANCHOR_SECRET_KEY}
  anchor_network: public
  anchor_threshold: 100  # Anchor every 100 segments
```

## Client Usage Example

```javascript
// Simple JavaScript client for fetching events
class PAVELClient {
    constructor(baseURL) {
        this.baseURL = baseURL;
    }
    
    async fetchEventsSince(lastTOID) {
        // 1. Check if we need hot or cold data
        const hotIndex = await fetch(`${this.baseURL}/hot/today.ndx`).then(r => r.json());
        
        if (hotIndex.length > 0 && hotIndex[0].toid > lastTOID) {
            // Need cold data first
            const segments = await fetch(`${this.baseURL}/index/segments.json`).then(r => r.json());
            // ... fetch from cold storage
        }
        
        // 2. Find offset in hot data
        const offset = this.findOffset(hotIndex, lastTOID);
        
        // 3. Range request for events
        const response = await fetch(`${this.baseURL}/hot/today.ndjson`, {
            headers: { 'Range': `bytes=${offset}-` }
        });
        
        // 4. Parse NDJSON
        const text = await response.text();
        return text.trim().split('\n').map(line => JSON.parse(line));
    }
}
```

## Verification

Clients can verify the integrity of the data by:

1. **Checking signatures**: Each segment has an Ed25519 signature of its Merkle root
2. **Following CID chains**: Each event points to the previous via `prev_cid`
3. **Verifying anchors**: Check Stellar transactions for on-chain anchors
4. **Comparing Merkle roots**: Reconstruct and verify Merkle trees

## Benefits

- **Zero-trust reading**: No API keys needed to access the data
- **Tamper-evident**: Cryptographic proofs ensure data integrity
- **Cost-effective**: Static file hosting scales cheaply
- **Simple cursoring**: Byte-offset indexes enable efficient range queries
- **Archive-friendly**: IPFS CAR files provide permanent addressing
- **Audit-ready**: Complete provenance from source ledger to final event