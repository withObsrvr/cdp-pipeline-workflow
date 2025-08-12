# Plugin Integration in CDP/FlowCTL Pipelines

## Overview

This document demonstrates how plugins integrate seamlessly into CDP/FlowCTL pipelines, showing real-world examples across different use cases. Plugins appear as natural pipeline stages alongside built-in processors.

## Basic Pipeline Structure with Plugins

### Traditional Pipeline (No Plugins)
```yaml
pipelines:
  mainnet-contract-data:
    source:
      type: captive-core
      config:
        network: mainnet
        start_ledger: 50000000
    processors:
      - type: contract-data
        config:
          contract_ids: ["CDLZ...", "CBQH..."]
      - type: transform-to-app-payment
    consumers:
      - type: save-to-parquet
        config:
          path: gs://stellar-data/contracts
```

### Enhanced Pipeline with Plugins
```yaml
pipelines:
  mainnet-contract-data-enhanced:
    # Load required plugins
    plugins:
      - name: contract-validator
        type: grpc
        path: ./plugins/contract-validator
        config:
          validation_rules: strict
          
      - name: data-enricher
        type: grpc
        path: ./plugins/enrichment-service
        config:
          api_key: ${ENRICHMENT_API_KEY}
          
      - name: high-speed-filter
        type: wasm
        path: ./plugins/filters/contract-filter.wasm
        config:
          min_value: 1000000

    source:
      type: captive-core
      config:
        network: mainnet
        start_ledger: 50000000

    # Mix of built-in and plugin processors
    processors:
      - type: contract-data                    # Built-in
        config:
          contract_ids: ["CDLZ...", "CBQH..."]
          
      - plugin: high-speed-filter             # WASM plugin (fast filtering)
      
      - plugin: contract-validator            # gRPC plugin (validation)
      
      - plugin: data-enricher                 # gRPC plugin (enrichment)
      
      - type: transform-to-app-payment        # Built-in
      
    consumers:
      - type: save-to-parquet                 # Built-in
        config:
          path: gs://stellar-data/contracts
```

## Real-World Pipeline Examples

### 1. DeFi Analytics Pipeline

```yaml
name: defi-analytics-pipeline
description: Process DeFi transactions with fraud detection and market analysis

plugins:
  # Machine Learning fraud detector (Python)
  - name: ml-fraud-detector
    type: grpc
    path: ghcr.io/stellar-plugins/fraud-detector:2.1.0  # Docker image
    config:
      model_version: "2024-01-15"
      threshold: 0.85
      feature_set: "extended"
      
  # High-performance DEX analyzer (Rust/WASM)
  - name: dex-analyzer
    type: wasm
    path: https://plugins.stellar.org/dex-analyzer-v3.wasm
    config:
      track_pairs: ["XLM/USDC", "USDC/EURC", "XLM/BTC"]
      
  # Real-time notification service (Node.js)
  - name: alert-service
    type: grpc
    path: ./plugins/alert-service
    config:
      webhooks:
        - url: ${SLACK_WEBHOOK}
          events: ["high_fraud_score", "large_trade"]
        - url: ${PAGERDUTY_WEBHOOK}
          events: ["system_anomaly"]

pipelines:
  defi-processor:
    source:
      type: captive-core
      config:
        network: mainnet
        
    processors:
      # Extract Soroban events
      - type: contract-events
        config:
          contract_type: "soroban"
          
      # Analyze DEX operations (WASM - runs in microseconds)
      - plugin: dex-analyzer
        
      # Built-in payment filter
      - type: filter-payments
        config:
          min_amount: 100
          
      # ML fraud detection (gRPC - Python with TensorFlow)
      - plugin: ml-fraud-detector
        
      # Route based on results
      - type: router
        config:
          routes:
            - condition: metadata.fraud_score > 0.8
              processors:
                - plugin: alert-service
            - condition: metadata.trade_value > 1000000
              processors:
                - type: save-to-postgresql
                  config:
                    table: large_trades
                    
    consumers:
      # Standard archival
      - type: save-to-parquet
        config:
          path: gs://defi-analytics/processed
          
      # Real-time metrics
      - type: save-to-prometheus
        config:
          metrics:
            - name: defi_transaction_volume
              labels: ["asset", "protocol"]
```

### 2. Compliance and Regulatory Pipeline

```yaml
name: compliance-pipeline
description: KYC/AML compliance checking with regulatory reporting

plugins:
  # KYC verification service (Java/gRPC)
  - name: kyc-verifier
    type: grpc
    path: kyc-service:50051  # Kubernetes service
    config:
      providers: ["jumio", "onfido"]
      cache_ttl: 86400
      
  # Sanctions screening (Go/gRPC)
  - name: sanctions-screener
    type: grpc
    path: ghcr.io/compliance/sanctions-screener:latest
    config:
      lists: ["OFAC", "EU", "UN"]
      fuzzy_match: true
      threshold: 0.9
      
  # Report generator (Python)
  - name: regulatory-reporter
    type: grpc
    path: ./plugins/regulatory-reporter
    config:
      jurisdictions: ["US", "EU", "UK"]
      report_format: "CSV"

pipelines:
  compliance-check:
    source:
      type: stellar-rpc
      config:
        endpoint: ${HORIZON_URL}
        stream_type: "transactions"
        
    processors:
      # Extract account operations
      - type: account-transactions
      
      # KYC verification (external service)
      - plugin: kyc-verifier
        
      # Sanctions screening (external service)
      - plugin: sanctions-screener
        
      # Built-in risk scoring
      - type: risk-calculator
        config:
          factors: ["volume", "frequency", "counterparties"]
          
      # Conditional reporting
      - type: conditional
        config:
          condition: |
            metadata.risk_score > 7 || 
            metadata.sanctions_hit == true ||
            metadata.kyc_status == "failed"
          processors:
            - plugin: regulatory-reporter
            
    consumers:
      # Compliance database
      - type: save-to-postgresql
        config:
          connection: ${COMPLIANCE_DB_URL}
          table: compliance_checks
          
      # Audit trail
      - type: save-to-immutable-log
        config:
          path: gs://compliance-audit/logs
          encryption: AES256
```

### 3. Smart Contract Analytics Pipeline

```yaml
name: soroban-analytics
description: Advanced Soroban contract analysis and monitoring

plugins:
  # Contract bytecode analyzer (Rust/WASM)
  - name: bytecode-analyzer
    type: wasm
    path: ./plugins/bytecode-analyzer.wasm
    config:
      detect_patterns: ["reentrancy", "overflow", "unauthorized"]
      
  # Gas optimization analyzer (Go/gRPC)
  - name: gas-optimizer
    type: grpc
    path: unix:///tmp/gas-optimizer.sock  # Unix socket for performance
    config:
      optimization_level: 3
      suggest_improvements: true
      
  # Contract relationship mapper (Python/NetworkX)
  - name: contract-graph
    type: grpc
    path: ./plugins/contract-graph
    config:
      max_depth: 5
      include_token_flows: true

# Single pipeline with parallel processing
pipeline:
  name: soroban-processor
  
  source:
    type: captive-core
    config:
      network: mainnet
      
  # Define parallel processing branches
  branches:
    # Branch 1: Security analysis
    security:
      - type: contract-invocation
        config:
          extract_bytecode: true
          
      - plugin: bytecode-analyzer
      
      - type: save-to-elasticsearch
        config:
          index: contract-security
          
    # Branch 2: Performance analysis
    performance:
      - type: contract-invocation
        config:
          track_gas: true
          
      - plugin: gas-optimizer
      
      - type: save-to-timescaledb
        config:
          table: gas_metrics
          
    # Branch 3: Network analysis
    network:
      - type: contract-data
        config:
          track_relationships: true
          
      - plugin: contract-graph
      
      - type: save-to-neo4j
        config:
          uri: bolt://neo4j:7687
          
  # Merge results
  merge:
    - type: aggregate-results
      config:
        group_by: contract_id
        
    - type: save-to-parquet
      config:
        path: gs://soroban-analytics/unified
```

### 4. Real-time Trading Pipeline

```yaml
name: trading-pipeline
description: Low-latency trading signal generation and execution

plugins:
  # Technical indicators (C++/WASM for speed)
  - name: ta-indicators
    type: wasm
    path: https://plugins.stellar.org/ta-indicators-simd.wasm
    config:
      indicators: ["RSI", "MACD", "BB", "EMA"]
      periods: [14, 26, 20]
      
  # ML price predictor (Python/gRPC with GPU)
  - name: price-predictor
    type: grpc
    path: price-predictor-gpu:50051
    config:
      model: "LSTM-attention-v3"
      horizon: 300  # 5 minutes
      confidence_threshold: 0.75
      resources:
        gpu: true
        
  # Order executor (Rust/gRPC for low latency)
  - name: order-executor
    type: grpc
    path: order-executor:50052
    config:
      exchange_connections:
        - stellarx
        - ultrastellar
      max_slippage: 0.001

pipeline:
  trading-signals:
    source:
      type: market-data-stream
      config:
        pairs: ["XLM/USDC", "USDC/EURC"]
        interval: 1000  # 1 second
        
    processors:
      # Calculate indicators (WASM - runs in 50μs)
      - plugin: ta-indicators
      
      # ML predictions (gRPC - GPU accelerated)
      - plugin: price-predictor
      
      # Trading logic
      - type: trading-strategy
        config:
          strategy: "momentum-mean-reversion"
          risk_limit: 10000  # USDC
          
      # Execute trades
      - plugin: order-executor
      
    consumers:
      # Real-time P&L tracking
      - type: save-to-redis
        config:
          key_prefix: "trading:pnl:"
          ttl: 86400
          
      # Trade history
      - type: save-to-clickhouse
        config:
          table: trades
          buffer_size: 100
```

### 5. Multi-Network Bridge Monitor

```yaml
name: bridge-monitor
description: Monitor cross-chain bridge operations

plugins:
  # Ethereum event listener (Node.js)
  - name: eth-listener
    type: grpc
    path: ./plugins/eth-listener
    config:
      rpc_url: ${ETH_RPC_URL}
      contracts:
        - address: "0x..."  # Bridge contract
          events: ["Lock", "Unlock"]
          
  # Bitcoin monitor (Python)
  - name: btc-monitor
    type: grpc
    path: ./plugins/btc-monitor
    config:
      electrum_servers:
        - "electrum1.example.com:50001"
      watch_addresses: ["bc1q..."]
      
  # Cross-chain validator (Go)
  - name: bridge-validator
    type: grpc
    path: ghcr.io/bridge/validator:stable
    config:
      networks: ["stellar", "ethereum", "bitcoin"]
      validation_rules: "./rules/bridge-rules.yaml"

# Multiple sources with plugin processors
pipelines:
  stellar-side:
    source:
      type: contract-events
      config:
        contract_id: "BRIDGE_CONTRACT_ID"
        
    processors:
      - plugin: bridge-validator
      
  ethereum-side:
    source:
      plugin: eth-listener  # Plugin as source!
      
    processors:
      - plugin: bridge-validator
      
  bitcoin-side:
    source:
      plugin: btc-monitor  # Plugin as source!
      
    processors:
      - plugin: bridge-validator
      
  # Unified consumer for all chains
  consumers:
    - type: save-to-postgresql
      config:
        table: bridge_operations
    - type: save-to-prometheus
      config:
        metrics:
          - name: bridge_volume_total
            labels: ["from_chain", "to_chain", "asset"]
```

## Advanced Plugin Features

### 1. Plugin Chaining and Composition

```yaml
# Compose multiple plugins into reusable components
components:
  fraud-detection-stack:
    - plugin: velocity-checker
    - plugin: ml-fraud-detector  
    - plugin: rule-engine
    
  compliance-stack:
    - plugin: kyc-verifier
    - plugin: sanctions-screener
    - plugin: regulatory-reporter

pipeline:
  processors:
    - type: filter-payments
    - component: fraud-detection-stack  # Use composed stack
    - component: compliance-stack
```

### 2. Dynamic Plugin Loading

```yaml
pipeline:
  processors:
    - type: dynamic-loader
      config:
        # Load plugins based on conditions
        rules:
          - condition: "ledger.protocol_version >= 20"
            load_plugin: "soroban-processor-v2"
          - condition: "network == 'testnet'"
            load_plugin: "testnet-debugger"
```

### 3. Plugin Versioning and Rollback

```yaml
plugins:
  - name: ml-model
    type: grpc
    path: model-service:50051
    version_strategy:
      type: "canary"
      stable: "v2.1.0"
      canary: "v2.2.0-rc1"
      traffic_split: 10  # 10% to canary
      rollback_on_error: true
```

### 4. Plugin Marketplace Integration

```yaml
# Automatic plugin discovery and installation
marketplace:
  registries:
    - url: https://plugins.stellar.org
      auth: ${PLUGIN_REGISTRY_TOKEN}
    - url: https://private-registry.company.com
      
  auto_install:
    - name: "fraud-detector"
      version: ">=2.0.0 <3.0.0"
    - name: "compliance-suite"
      version: "latest"
```

## Performance Characteristics

### Plugin Performance Comparison

| Plugin Type | Latency | Throughput | Use Cases |
|------------|---------|------------|-----------|
| Native | <1μs | 1M+ msg/s | Core processors |
| WASM | 1-10μs | 500K msg/s | Filters, transforms |
| gRPC (local) | 100-500μs | 50K msg/s | Complex processing |
| gRPC (remote) | 1-10ms | 5K msg/s | External services |

### Example Latency Budget

```yaml
# Pipeline with 10ms latency budget
pipeline:
  processors:
    - type: latest-ledger          # 0.1ms (native)
    - plugin: wasm-filter          # 0.01ms (WASM)
    - plugin: ml-enricher          # 5ms (gRPC remote)
    - plugin: validator            # 0.5ms (gRPC local)
    - type: transform              # 0.1ms (native)
    # Total: ~5.71ms (within budget)
```

## Deployment Patterns

### 1. Kubernetes Sidecar Pattern

```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  # Main CDP pipeline
  - name: cdp-pipeline
    image: cdp-pipeline:latest
    
  # Plugin sidecars
  - name: fraud-detector
    image: fraud-detector-plugin:latest
    ports:
    - containerPort: 50051
    
  - name: enricher
    image: enricher-plugin:latest
    ports:
    - containerPort: 50052
```

### 2. Serverless Plugins

```yaml
plugins:
  - name: report-generator
    type: grpc
    path: lambda://arn:aws:lambda:region:account:function:report-gen
    config:
      timeout: 300s
      memory: 3008MB
```

### 3. Edge Computing

```yaml
# Run WASM plugins at edge locations
plugins:
  - name: geo-filter
    type: wasm
    deployment:
      type: edge
      locations: ["us-east", "eu-west", "asia-pac"]
      replication: 3
```

## Monitoring and Debugging

### Pipeline Visualization

```
Source: captive-core
  ↓ 1,234 ledgers/sec
[latest-ledger] ✓ 0.1ms
  ↓ 1,234 msgs/sec
[wasm:filter] ✓ 0.01ms
  ↓ 456 msgs/sec (filtered)
[grpc:ml-fraud] ⚠ 5.2ms (slow)
  ↓ 456 msgs/sec
[grpc:enricher] ✓ 2.1ms
  ↓ 456 msgs/sec
[save-to-parquet] ✓ 1.5ms
```

### Debug Commands

```bash
# List loaded plugins
$ flowctl plugins list
NAME              TYPE    VERSION   STATUS    LATENCY    PROCESSED
fraud-detector    grpc    2.1.0     healthy   5.2ms      1.2M
wasm-filter       wasm    1.0.0     healthy   0.01ms     45.6M
enricher          grpc    3.4.1     healthy   2.1ms      1.2M

# Inspect plugin
$ flowctl plugins inspect fraud-detector
Plugin: fraud-detector
Type: gRPC
Version: 2.1.0
Status: healthy
Endpoint: fraud-detector:50051
Config:
  model_version: "2024-01-15"
  threshold: 0.85
Resources:
  CPU: 2.3 cores
  Memory: 1.2GB
  Network: 45MB/s
Metrics:
  Processed: 1,234,567
  Errors: 12 (0.001%)
  Avg Latency: 5.2ms
  P99 Latency: 8.9ms

# Test plugin
$ flowctl plugins test fraud-detector --sample-data ledger.json
Processing sample data...
Result: {
  "fraud_score": 0.23,
  "fraud_detected": false,
  "processing_time": "4.8ms"
}
```

## V2 Configuration Examples with Plugins

The v2 configuration format dramatically simplifies plugin usage with its streamlined syntax and smart defaults.

### V2 Basic Plugin Example

```yaml
# v2 format - minimal configuration
plugins:
  - fraud-detector    # Auto-discovers from registry
  - fast-filter:      # With inline config
      threshold: 1000000

source: mainnet       # Smart defaults applied

process:
  - contract_data     # Built-in (using alias)
  - fast-filter       # Plugin
  - fraud-detector    # Plugin
  
save_to: parquet      # Smart defaults for path
```

### V2 DeFi Analytics (Simplified)

```yaml
# Before (v1): 50+ lines
# After (v2): 15 lines

plugins:
  - ml-fraud@2.1.0    # Version pinning
  - dex-analyzer      # Latest version
  - alerts:           # Inline config
      slack: ${SLACK_WEBHOOK}

source: 
  network: mainnet
  start: latest-1000  # Smart ledger math

process:
  - soroban_events    # Alias for contract-events
  - dex-analyzer
  - payments > 100    # Inline filter syntax
  - ml-fraud
  - route:
      fraud > 0.8: alerts
      value > 1M: save_to.postgres

save_to:
  - parquet          # Default: gs://cdp-data/{network}/
  - prometheus       # Default metrics
```

### V2 Compliance Pipeline

```yaml
plugins:
  kyc: kyc-service:50051      # Direct endpoint
  sanctions: ghcr.io/comply    # Docker image
  report: ./plugins/reporter   # Local path

source: horizon               # Alias for stellar-rpc

process:
  - account_tx               # Alias
  - parallel:                # Parallel execution
      - kyc
      - sanctions
  - risk_score               # Built-in
  - when:                    # Conditional
      risk > 7 OR sanctions: report

save_to:
  postgres: ${COMPLIANCE_DB}
  audit: immutable-log       # Alias with encryption
```

### V2 Smart Contract Analytics

```yaml
# Parallel branches in v2
plugins:
  - bytecode-analyzer.wasm   # WASM plugin
  - gas-optimizer            # Auto-detect type
  - contract-graph           # Default config

source: mainnet

branches:                    # v2 parallel syntax
  security:
    - contract_code          # Extract bytecode
    - bytecode-analyzer
    - save_to: elastic/security
    
  performance:
    - contract_gas           # Track gas
    - gas-optimizer  
    - save_to: timescale
    
  network:
    - contract_data
    - contract-graph
    - save_to: neo4j

merge: aggregate(contract_id)
save_to: parquet
```

### V2 Trading Pipeline (Ultra-Compact)

```yaml
plugins:
  ta: ta-indicators-simd.wasm
  ml: price-predictor-gpu:50051
  exec: order-executor:50052

source: 
  market_data: [XLM/USDC, USDC/EURC]
  interval: 1s

process:
  - ta                      # WASM: 50μs
  - ml                      # GPU: 5ms  
  - strategy: momentum      # Built-in
  - exec                    # Execute trades

save_to:
  - redis: trading:pnl:     # Real-time P&L
  - clickhouse              # Trade history
```

### V2 Multi-Network Bridge

```yaml
# Multiple sources with plugins
plugins:
  eth: ./plugins/eth-listener
  btc: ./plugins/btc-monitor
  validator: bridge-validator

pipelines:                  # v2 multi-pipeline
  stellar:
    source: contract_events(BRIDGE_ID)
    process: validator
    
  ethereum:
    source: plugin:eth      # Plugin as source!
    process: validator
    
  bitcoin:
    source: plugin:btc      # Plugin as source!
    process: validator

# Unified output
save_to:
  - postgres: bridge_ops
  - metrics:
      bridge_volume: [from, to, asset]
```

### V2 Plugin Management

```yaml
# Global plugin configuration
plugins:
  # Auto-install from marketplace
  - fraud-detector: latest
  - compliance-suite: ">=2.0.0 <3.0.0"
  
  # Local development
  - name: my-processor
    path: ./dev/my-processor
    hot_reload: true
    
  # Canary deployment
  - ml-model:
      stable: v2.1.0
      canary: v2.2.0-rc1
      traffic: 10%          # 10% to canary

# Plugin marketplace
marketplace:
  registries:
    - https://plugins.stellar.org
    - company-registry.internal
  
  auto_update: true
  verify_signatures: true
```

### V2 One-Liner with Plugins

```bash
# Install and run with plugins
flowctl run "mainnet | contract_data | plugin:fraud-detector | parquet"

# With inline plugin config
flowctl run "mainnet | payments > 1000 | plugin:ml-fraud(threshold=0.9) | postgres"

# Chain multiple plugins
flowctl run "testnet | plugin:validator | plugin:enricher | plugin:reporter | s3"
```

### V2 Environment-Based Plugin Selection

```yaml
# Automatic plugin selection based on environment
plugins:
  fraud:
    production: ml-fraud-prod:50051
    staging: ml-fraud-staging:50052
    development: ./plugins/mock-fraud

source: ${NETWORK}

process:
  - payments
  - fraud              # Selects based on ENV
  
save_to: ${OUTPUT_PATH}
```

### V2 Plugin Composition

```yaml
# Define reusable plugin stacks
stacks:
  fraud_stack:
    - velocity-checker
    - ml-fraud
    - rule-engine
    
  compliance_stack:
    - kyc
    - sanctions
    - reporter

# Use in pipeline
process:
  - payments
  - stack:fraud_stack      # Expand stack
  - stack:compliance_stack
  - save_to: postgres
```

### V2 vs V1 Comparison

#### V1 Configuration (verbose)
```yaml
pipelines:
  mainnet-processor:
    name: "Mainnet Processor"
    source:
      type: "CaptiveCoreInboundAdapter"
      config:
        network: "pubnet"
        stellar_core_binary_path: "/usr/bin/stellar-core"
        stellar_core_config_path: "/etc/stellar-core.cfg"
        history_archive_urls:
          - "https://history.stellar.org/prd/core-live/core_live_001"
        start_ledger: 50000000
        
    processors:
      - type: "ContractData"
        config:
          network_passphrase: "Public Global Stellar Network ; September 2015"
          contract_ids:
            - "CDLZ..."
            
      - type: "ExternalProcessor"
        config:
          endpoint: "fraud-detector:50051"
          timeout: "30s"
          max_retries: 3
          
    consumers:
      - type: "SaveToParquet"
        config:
          storage_type: "GCS"
          bucket_name: "stellar-archive"
          path: "mainnet/contracts/"
          file_format: "parquet"
          compression: "snappy"
```

#### V2 Configuration (concise)
```yaml
# 80% less configuration!
plugins:
  - fraud-detector

source: mainnet

process:
  - contract_data: [CDLZ...]
  - fraud-detector
  
save_to: parquet   # All defaults applied
```

### V2 Advanced Features

```yaml
# Type inference
process:
  - payments > 1000        # Infers filter_payments
  - contains("USDC")       # Infers asset filter
  - fraud_score            # Infers plugin output

# Smart routing
route:
  high_value: 
    amount > 1M: priority_queue
  suspicious:
    fraud > 0.8: alert_channel
  default: standard_processing

# Automatic batching
save_to:
  parquet:
    batch: adaptive       # Adjusts based on throughput
    compress: auto        # Selects best algorithm
```

## Conclusion

Plugins in CDP/FlowCTL pipelines:
- **Seamlessly integrate** with built-in processors
- **Appear as natural** pipeline stages
- **Support any language** through gRPC
- **Enable hot-swapping** without downtime
- **Scale independently** from the core

The v2 configuration format makes plugins even more accessible:
- **80% less configuration** through smart defaults
- **Inline plugin config** for simplicity
- **Automatic discovery** from registries
- **Environment-aware** selection
- **One-liner support** for quick pipelines

The plugin system transforms CDP from a monolithic processor into an extensible platform where teams can contribute specialized processors in their preferred languages while maintaining the performance and reliability of the core system.