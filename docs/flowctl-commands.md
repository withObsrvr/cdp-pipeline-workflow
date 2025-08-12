# Flowctl Commands for CDP Pipeline Monitoring

## Viewing Pipeline Status

Once your CDP pipeline is registered with flowctl, you can use these commands to monitor it:

### List All Services
```bash
flowctl status
```
This will show all registered services including your CDP pipelines.

### Get Detailed Pipeline Status
```bash
flowctl status cdp-ContractEventPipeline-1755026214
```
Replace with your actual pipeline ID from the registration log.

### View Pipeline Metrics
```bash
flowctl status cdp-ContractEventPipeline-1755026214 --json | jq '.metrics'
```
This shows all metrics being reported by the pipeline components.

### Monitor Health
```bash
flowctl health cdp-ContractEventPipeline-1755026214
```
Check if the pipeline is healthy based on heartbeat timing.

## Example Output

When you run `flowctl status`, you might see:
```
ID                                      TYPE           STATUS    UPTIME
cdp-ContractEventPipeline-1755026214   cdp-pipeline   healthy   2m 30s
```

For detailed status:
```
Pipeline: cdp-ContractEventPipeline-1755026214
Type: cdp-pipeline
Status: healthy
Config: ContractEventPipeline
Source Type: BufferedStorageSourceAdapter
Version: dev
Uptime: 2m 30s

Metrics:
  pipeline.uptime_seconds: 150.5
  pipeline.component_count: 3
  processor.ContractEventProcessor.processed_ledgers: 1250
  processor.ContractEventProcessor.events_found: 456
  processor.ContractEventProcessor.successful_events: 450
  processor.ContractEventProcessor.failed_events: 6
```

## Troubleshooting

If the pipeline doesn't appear in `flowctl status`:
1. Check the CDP pipeline logs for registration confirmation
2. Ensure FLOWCTL_ENDPOINT is set correctly
3. Verify flowctl server is running and accessible

If metrics aren't updating:
1. Wait at least 10 seconds (heartbeat interval)
2. Check if the pipeline is still running
3. Look for heartbeat errors in CDP pipeline logs

## Advanced Monitoring

### Set up alerts
```bash
flowctl alert create \
  --service cdp-ContractEventPipeline-1755026214 \
  --condition "metrics['processor.ContractEventProcessor.failed_events'] > 100" \
  --webhook https://your-webhook.com/alerts
```

### Export metrics to Prometheus
```bash
flowctl metrics export --prometheus-endpoint :9090
```

### View historical metrics
```bash
flowctl metrics cdp-ContractEventPipeline-1755026214 --duration 1h
```