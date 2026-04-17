package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

type DailyCount struct {
	Date             time.Time `json:"date"`
	TransactionCount int64     `json:"transaction_count"`
}

type AccountYearMetrics struct {
	AccountID          string    `json:"account_id"`
	TotalTransactions  int64     `json:"total_transactions"`
	HighestTxCountDate time.Time `json:"highest_transaction_count_date"`
	HighestTxCount     int64     `json:"highest_transaction_count"`
	// Internal tracking
	dailyCounts map[string]int64 // date string -> count
}

type AccountYearAnalytics struct {
	processors        []Processor
	targetAccount     string
	targetYear        int
	startTime         time.Time
	endTime           time.Time
	networkPassphrase string
	metrics           *AccountYearMetrics
	mu                sync.RWMutex
}

func NewAccountYearAnalytics(config map[string]interface{}) (*AccountYearAnalytics, error) {
	account, ok := config["account_id"].(string)
	if !ok {
		return nil, fmt.Errorf("missing account_id in configuration")
	}

	year, ok := config["year"].(int)
	if !ok {
		return nil, fmt.Errorf("missing year in configuration")
	}

	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	startTime := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := startTime.AddDate(1, 0, 0)

	return &AccountYearAnalytics{
		targetAccount:     account,
		targetYear:        year,
		startTime:         startTime,
		endTime:           endTime,
		networkPassphrase: networkPassphrase,
		metrics: &AccountYearMetrics{
			AccountID:   account,
			dailyCounts: make(map[string]int64),
		},
	}, nil
}

func (p *AccountYearAnalytics) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *AccountYearAnalytics) forwardToProcessors(ctx context.Context) error {
	p.mu.RLock()
	// Update highest transaction count before forwarding
	p.updateHighestTxCount()
	metrics := *p.metrics // Make a copy of the metrics
	p.mu.RUnlock()

	// Convert to JSON
	jsonBytes, err := json.Marshal(metrics)
	if err != nil {
		return fmt.Errorf("error marshaling metrics: %w", err)
	}

	// Forward to all registered processors
	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}
	return nil
}

func (p *AccountYearAnalytics) updateHighestTxCount() {
	highestCount := int64(0)
	var highestDate time.Time

	for dateStr, count := range p.metrics.dailyCounts {
		if count > highestCount {
			highestCount = count
			date, _ := time.Parse("2006-01-02", dateStr)
			highestDate = date
		}
	}

	p.metrics.HighestTxCount = highestCount
	p.metrics.HighestTxCountDate = highestDate
}

func (p *AccountYearAnalytics) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	closeTime := time.Unix(int64(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0)

	// Skip if outside target year
	if closeTime.Before(p.startTime) || closeTime.After(p.endTime) {
		return nil
	}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	metricsUpdated := false
	p.mu.Lock()

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			p.mu.Unlock()
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Only process successful transactions
		if !tx.Result.Successful() {
			continue
		}

		// Check if transaction involves target account
		if p.isAccountInvolved(tx, p.targetAccount) {
			dateKey := closeTime.Format("2006-01-02")

			// Update daily count
			p.metrics.dailyCounts[dateKey]++

			// Update total transactions
			p.metrics.TotalTransactions++

			metricsUpdated = true
		}
	}

	p.mu.Unlock()

	// Forward to consumers if metrics were updated
	if metricsUpdated {
		if err := p.forwardToProcessors(ctx); err != nil {
			return fmt.Errorf("error forwarding to processors: %w", err)
		}
	}

	return nil
}

func (p *AccountYearAnalytics) GetMetrics() *AccountYearMetrics {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.updateHighestTxCount()
	return p.metrics
}

func (p *AccountYearAnalytics) isAccountInvolved(tx ingest.LedgerTransaction, account string) bool {
	// Check source account
	if tx.Envelope.SourceAccount().ToAccountId().Address() == account {
		return true
	}

	// Check operations
	for _, op := range tx.Envelope.Operations() {
		if op.SourceAccount != nil && op.SourceAccount.Address() == account {
			return true
		}
		// Check operation-specific destination accounts
		switch op.Body.Type {
		case xdr.OperationTypePayment:
			if op.Body.PaymentOp.Destination.Address() == account {
				return true
			}
		case xdr.OperationTypeCreateAccount:
			if op.Body.CreateAccountOp.Destination.Address() == account {
				return true
			}
			// Add other operation types as needed
		}
	}
	return false
}
