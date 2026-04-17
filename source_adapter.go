package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/storage"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	cdpProcessor "github.com/withObsrvr/cdp-pipeline-workflow/processor"
)

type CaptiveCoreInboundAdapter struct {
	TomlParams         ledgerbackend.CaptiveCoreTomlParams
	processors         []processor.Processor
	historyArchiveURLs []string
	networkPassphrase  string
	stats              struct {
		processedLedgers int64
		lastLogTime      time.Time
	}
}

func NewCaptiveCoreInboundAdapter(config map[string]interface{}) (SourceAdapter, error) {
	historyArchiveURLsRaw, ok := config["history_archive_urls"]
	if !ok {
		return nil, errors.New("history_archive_urls is missing")
	}

	historyArchiveURLs, ok := historyArchiveURLsRaw.([]interface{})
	if !ok {
		return nil, errors.New("history_archive_urls must be a slice")
	}

	urls := make([]string, len(historyArchiveURLs))
	for i, url := range historyArchiveURLs {
		urls[i], ok = url.(string)
		if !ok {
			return nil, errors.New("all history_archive_urls must be strings")
		}
	}

	network, ok := config["network"].(string)
	if !ok {
		return nil, errors.New("network must be a string")
	}

	var networkPassphrase string
	switch network {
	case "testnet":
		networkPassphrase = "Test SDF Network ; September 2015"
	case "pubnet":
		networkPassphrase = "Public Global Stellar Network ; September 2015"
	default:
		return nil, fmt.Errorf("unsupported network: %s", network)
	}

	coreBinaryPath, ok := config["core_binary_path"].(string)
	if !ok {
		return nil, errors.New("core_binary_path must be a string")
	}

	return &CaptiveCoreInboundAdapter{
		historyArchiveURLs: urls,
		networkPassphrase:  networkPassphrase,
		TomlParams: ledgerbackend.CaptiveCoreTomlParams{
			NetworkPassphrase:  networkPassphrase,
			HistoryArchiveURLs: urls,
			CoreBinaryPath:     coreBinaryPath,
		},
	}, nil
}

func (adapter *CaptiveCoreInboundAdapter) Subscribe(receiver processor.Processor) {
	adapter.processors = append(adapter.processors, receiver)
}

func (adapter *CaptiveCoreInboundAdapter) Run(ctx context.Context) error {
	// Setup captive core config to use the network
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreToml(adapter.TomlParams)
	if err != nil {
		return errors.Wrap(err, "error creating captive core toml")
	}

	captiveConfig := ledgerbackend.CaptiveCoreConfig{
		BinaryPath:         adapter.TomlParams.CoreBinaryPath,
		HistoryArchiveURLs: adapter.TomlParams.HistoryArchiveURLs,
		NetworkPassphrase:  adapter.networkPassphrase,
		Context:            ctx,
		Toml:               captiveCoreToml,
	}

	// Create a new captive core backend
	captiveBackend, err := ledgerbackend.NewCaptive(captiveConfig)
	if err != nil {
		return errors.Wrap(err, "error creating captive core instance")
	}

	// Create a client to the network's history archives
	historyArchive, err := historyarchive.NewArchivePool(adapter.historyArchiveURLs, historyarchive.ArchiveOptions{
		ConnectOptions: storage.ConnectOptions{
			UserAgent: "my_app",
			Context:   ctx,
		},
	})

	if err != nil {
		return errors.Wrap(err, "error creating history archive client")
	}

	// Acquire the most recent ledger on network
	rootHAS, err := historyArchive.GetRootHAS()
	if err != nil {
		return errors.Wrap(err, "error getting root HAS")
	}
	latestNetworkLedger := rootHAS.CurrentLedger

	// Tell the captive core instance to emit LedgerCloseMeta starting at
	// latest network ledger and continuing indefinitely, streaming.
	if err := captiveBackend.PrepareRange(ctx, ledgerbackend.UnboundedRange(uint32(latestNetworkLedger))); err != nil {
		return errors.Wrap(err, "error preparing captive core ledger range")
	}

	adapter.stats.lastLogTime = time.Now()

	// Run endless loop that receives LedgerCloseMeta from captive core
	for nextLedger := latestNetworkLedger; ; nextLedger++ {
		ledgerCloseMeta, err := captiveBackend.GetLedger(ctx, uint32(nextLedger))
		if err != nil {
			return errors.Wrapf(err, "failed to retrieve ledger %d from the ledger backend", nextLedger)
		}

		if err := adapter.processLedger(ctx, ledgerCloseMeta); err != nil {
			return errors.Wrapf(err, "failed to process ledger %d", nextLedger)
		}

		adapter.stats.processedLedgers++
		if time.Since(adapter.stats.lastLogTime) > time.Second*10 {
			rate := float64(adapter.stats.processedLedgers) / time.Since(adapter.stats.lastLogTime).Seconds()
			log.Printf("Processed %d ledgers so far (%.2f ledgers/sec)",
				adapter.stats.processedLedgers, rate)
			adapter.stats.lastLogTime = time.Now()
		}
	}
}

func (adapter *CaptiveCoreInboundAdapter) processLedger(ctx context.Context, ledger xdr.LedgerCloseMeta) error {
	sequence := ledger.LedgerSequence()
	log.Printf("Processing ledger %d", sequence)

	for _, processor := range adapter.processors {
		if err := processor.Process(ctx, cdpProcessor.Message{Payload: ledger}); err != nil {
			log.Printf("Error processing ledger %d in processor %T: %v",
				sequence, processor, err)
			return errors.Wrap(err, "error in processor")
		}
		log.Printf("Successfully processed ledger %d in processor %T",
			sequence, processor)
	}

	return nil
}
