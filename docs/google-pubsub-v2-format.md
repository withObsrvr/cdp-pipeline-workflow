# Google Pub/Sub V2 Message Format

## Overview

The `PublishToGooglePubSubV2` consumer publishes Stellar payment events to Google Pub/Sub using a standardized message format designed for downstream service consumption.

## V2 Message Format

```json
{
  "chainIdentifier": "StellarMainnet" | "StellarTestnet",
  "payload": {
    "paymentId": "0x123abc...",
    "merchantAddress": "GABC...XYZ",
    "amount": "1000000",
    "royaltyFee": "50000",
    "payerAddress": "GDEF...123"
  },
  "details": {
    "hash": "abc123...",
    "block": 12345,
    "to": "GABC...XYZ",
    "from": "GDEF...123"
  }
}
```

### Field Descriptions

#### Top Level

- **chainIdentifier** (string): Network identifier
  - `"StellarMainnet"` - Stellar public network
  - `"StellarTestnet"` - Stellar test network

#### Payload Object

Contains the core payment data:

- **paymentId** (string): Unique payment identifier from contract event (hex format with 0x prefix)
- **merchantAddress** (string): Stellar account address of the merchant (G-format)
- **amount** (string): Payment amount as string (preserves precision for large numbers)
- **royaltyFee** (string): Royalty fee amount as string
- **payerAddress** (string): Stellar account address of the payer (G-format)

#### Details Object

Contains transaction-level metadata:

- **hash** (string): Transaction hash on the Stellar network
- **block** (number): Ledger sequence number (block height)
- **to** (string): Destination address (typically same as merchantAddress)
- **from** (string): Source address (typically same as payerAddress)

## Pub/Sub Message Attributes

In addition to the JSON message body, the following attributes are set:

- `event_type`: Always `"payment"`
- `chain_identifier`: `"StellarMainnet"` or `"StellarTestnet"`
- `block_height`: Ledger sequence as string
- `payment_id`: Payment ID from the event
- `message_version`: Always `"v2"`

These attributes allow for efficient message filtering and routing in Pub/Sub subscriptions.

## Configuration

### Basic Configuration

```yaml
consumers:
  - type: PublishToGooglePubSubV2
    config:
      project_id: "your-gcp-project-id"
      topic_id: "payments"
      chain_identifier: "StellarTestnet"
      credentials_file: "/path/to/service-account-key.json"
```

### Configuration Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `project_id` | string | Yes | - | Google Cloud Project ID |
| `topic_id` | string | Yes | - | Pub/Sub topic name |
| `chain_identifier` | string | No | `StellarTestnet` | Network identifier |
| `credentials_file` | string | No | - | Path to GCP service account JSON |
| `credentials_json` | string | No | - | GCP service account JSON as string |

### Environment Variables

The consumer supports the following environment variables:

- `PUBSUB_PROJECT_ID` - Overrides `project_id` config
- `PUBSUB_EMULATOR_HOST` - Use local Pub/Sub emulator (e.g., `localhost:8085`)
- `CHAIN_IDENTIFIER` - Overrides `chain_identifier` config
- `GCLOUD_PUBSUB_PUBLISHER_SERVICE_ACCOUNT_KEY` - JSON credentials string
- `GOOGLE_APPLICATION_CREDENTIALS` - Path to credentials file

## Differences from V1 Format

### V1 Format (Legacy)

```json
{
  "id": "...",
  "payment_id": "0x123...",
  "token_id": "CABC...XYZ",
  "amount": 1000000,
  "from_id": "GDEF...123",
  "merchant_id": "GABC...XYZ",
  "royalty_amount": 50000,
  "tx_hash": "abc123...",
  "block_height": 12345,
  "block_timestamp": "2024-01-01T00:00:00Z"
}
```

### Key Changes in V2

1. **Structured Format**: Three-level structure with `chainIdentifier`, `payload`, and `details`
2. **Chain Identifier**: Explicit network identification at top level
3. **String Amounts**: Amounts are strings (not numbers) to preserve precision
4. **Renamed Fields**: More descriptive field names
   - `merchant_id` → `merchantAddress`
   - `from_id` → `payerAddress`
   - `royalty_amount` → `royaltyFee`
   - `tx_hash` → `details.hash`
   - `block_height` → `details.block`
5. **Removed Fields**:
   - `id` (internal database ID)
   - `token_id` (contract address)
   - `block_timestamp` (can be derived from block number)
6. **Message Version Attribute**: Added `message_version: "v2"` for versioning

### Migration Path

To migrate from V1 to V2:

1. Update your pipeline config to use `PublishToGooglePubSubV2` instead of `PublishToGooglePubSub`
2. Add `chain_identifier` to your config
3. Update downstream consumers to parse the new V2 format
4. Use the `message_version` attribute to support both formats during migration

## Local Testing with Pub/Sub Emulator

### Start the Emulator

```bash
# Install gcloud SDK first
gcloud components install pubsub-emulator

# Start emulator
gcloud beta emulators pubsub start --host-port=localhost:8085
```

### Configure Environment

```bash
export PUBSUB_EMULATOR_HOST="localhost:8085"
export PUBSUB_PROJECT_ID="local-dev-project"
```

### Run Pipeline

```bash
./cdp-pipeline-workflow -config config/examples/kwickbit-payments-pubsub-v2.yaml
```

### Subscribe to Messages

```bash
# Create subscription
gcloud pubsub subscriptions create test-sub \
  --topic=payments \
  --project=local-dev-project

# Pull messages
gcloud pubsub subscriptions pull test-sub \
  --auto-ack \
  --limit=10 \
  --project=local-dev-project
```

## Example Usage

### TypeScript/Node.js Consumer

```typescript
import { PubSub, Message } from '@google-cloud/pubsub';

interface PubSubMessageV2 {
  chainIdentifier: 'StellarMainnet' | 'StellarTestnet';
  payload: {
    paymentId: string;
    merchantAddress: string;
    amount: string;
    royaltyFee: string;
    payerAddress: string;
  };
  details: {
    hash: string;
    block: number;
    to: string;
    from: string;
  };
}

const pubsub = new PubSub();
const subscription = pubsub.subscription('payments-sub');

subscription.on('message', (message: Message) => {
  // Check message version
  if (message.attributes.message_version === 'v2') {
    const data: PubSubMessageV2 = JSON.parse(message.data.toString());

    console.log('Payment received:', {
      chain: data.chainIdentifier,
      paymentId: data.payload.paymentId,
      merchant: data.payload.merchantAddress,
      amount: data.payload.amount,
      block: data.details.block,
    });

    message.ack();
  }
});
```

### Python Consumer

```python
from google.cloud import pubsub_v1
import json

def callback(message):
    # Check message version
    if message.attributes.get('message_version') == 'v2':
        data = json.loads(message.data.decode('utf-8'))

        print(f"Payment received:")
        print(f"  Chain: {data['chainIdentifier']}")
        print(f"  Payment ID: {data['payload']['paymentId']}")
        print(f"  Merchant: {data['payload']['merchantAddress']}")
        print(f"  Amount: {data['payload']['amount']}")
        print(f"  Block: {data['details']['block']}")

        message.ack()

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('your-project', 'payments-sub')

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print('Listening for messages...')

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
```

## Performance Considerations

- **Message Size**: V2 messages are slightly larger than V1 due to the structured format
- **Throughput**: Same throughput as V1 (no performance degradation)
- **Filtering**: More efficient filtering using message attributes
- **Ordering**: Messages are published in the order they are processed

## Security

- Use service account credentials with minimum required permissions:
  - `pubsub.topics.publish`
  - `pubsub.topics.get` (for topic existence check)
  - `pubsub.topics.create` (optional, if creating topics)
- Store credentials securely (environment variables or secret managers)
- Use the emulator for local development (no credentials needed)
- Enable audit logging for production deployments

## Troubleshooting

### Common Issues

1. **Authentication Failed**
   - Verify credentials file path
   - Check service account permissions
   - Ensure GOOGLE_APPLICATION_CREDENTIALS is set correctly

2. **Topic Not Found**
   - Topic will be auto-created if permissions allow
   - Create topic manually: `gcloud pubsub topics create payments`

3. **Message Not Received**
   - Check subscription exists
   - Verify subscription is pulling from correct topic
   - Check message attributes for filtering

4. **Invalid Chain Identifier**
   - Must be exactly `StellarMainnet` or `StellarTestnet`
   - Case-sensitive

### Debug Logging

Enable debug logging to see published messages:

```bash
export LOG_LEVEL=debug
./cdp-pipeline-workflow -config config/examples/kwickbit-payments-pubsub-v2.yaml
```

## See Also

- [Example Configuration](../config/examples/kwickbit-payments-pubsub-v2.yaml)
- [EventPayment Processor](./processors/event-payment-extractor.md)
- [Google Cloud Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
