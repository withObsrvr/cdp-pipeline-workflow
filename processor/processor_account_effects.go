package processor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

type AccountEffect struct {
	Timestamp       time.Time              `json:"timestamp"`
	EffectType      string                 `json:"effect_type"`
	LedgerSequence  uint32                 `json:"ledger_sequence"`
	AccountID       string                 `json:"account_id"`
	Details         map[string]interface{} `json:"details"`
	TransactionHash string                 `json:"transaction_hash"`
}

type AccountEffectProcessor struct {
	targetAccount     string
	processors        []Processor
	networkPassphrase string
	mu                sync.RWMutex
	stats             struct {
		ProcessedEffects uint64
		LastEffectTime   time.Time
	}
}

func NewAccountEffectProcessor(config map[string]interface{}) (*AccountEffectProcessor, error) {
	var account string
	if acc, ok := config["account"].(string); ok && acc != "" {
		account = acc
	}

	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("missing network_passphrase in configuration")
	}

	return &AccountEffectProcessor{
		targetAccount:     account,
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *AccountEffectProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *AccountEffectProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	closeTime := time.Unix(int64(ledgerCloseMeta.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0)
	sequence := ledgerCloseMeta.LedgerSequence()

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading transaction: %w", err)
		}

		// Generate placeholder hash only when required
		txHash := tx.Result.TransactionHash
		if txHash == (xdr.Hash{}) && sequence <= 4 { // Only for early ledgers
			txHash = generatePlaceholderHash(sequence)
			log.Printf("Generated placeholder hash for ledger %d transaction", sequence)
		}

		effects, err := p.extractEffects(tx, closeTime, sequence, txHash.HexString())
		if err != nil {
			log.Printf("Error extracting effects: %v", err)
			continue
		}

		// Forward effects to processors
		for _, effect := range effects {
			if err := p.forwardEffect(ctx, effect); err != nil {
				return fmt.Errorf("error forwarding effect: %w", err)
			}
		}
	}

	return nil
}

func (p *AccountEffectProcessor) extractEffects(tx ingest.LedgerTransaction, closeTime time.Time, sequence uint32, txHash string) ([]*AccountEffect, error) {
	var effects []*AccountEffect

	// Process each operation
	for opIndex, op := range tx.Envelope.Operations() {
		// Skip if we have a target account and this operation doesn't involve it
		if p.targetAccount != "" && !p.isAccountInvolved(tx, op, p.targetAccount) {
			continue
		}

		// Extract operation-specific effects
		opEffects, err := p.extractOperationEffects(tx, op, opIndex, closeTime, sequence, txHash)
		if err != nil {
			return nil, fmt.Errorf("error extracting operation effects: %w", err)
		}
		effects = append(effects, opEffects...)

		// Extract any state changes (trustlines, data entries, etc.)
		changes, err := tx.GetOperationChanges(uint32(opIndex))
		if err != nil {
			return nil, fmt.Errorf("error getting operation changes: %w", err)
		}

		changeEffects, err := p.extractChangeEffects(changes, tx, closeTime, sequence, txHash)
		if err != nil {
			return nil, fmt.Errorf("error extracting change effects: %w", err)
		}
		effects = append(effects, changeEffects...)
	}

	return effects, nil
}

func (p *AccountEffectProcessor) extractOperationEffects(tx ingest.LedgerTransaction, op xdr.Operation, opIndex int, closeTime time.Time, sequence uint32, txHash string) ([]*AccountEffect, error) {
	var effects []*AccountEffect

	switch op.Body.Type {
	case xdr.OperationTypeCreateAccount:
		create := op.Body.MustCreateAccountOp()
		effects = append(effects, &AccountEffect{
			Timestamp:       closeTime,
			EffectType:      "account_created",
			LedgerSequence:  sequence,
			AccountID:       create.Destination.Address(),
			TransactionHash: txHash,
			Details: map[string]interface{}{
				"starting_balance": create.StartingBalance,
				"funder":           tx.Envelope.SourceAccount().ToAccountId().Address(),
			},
		})

	case xdr.OperationTypePayment:
		payment := op.Body.MustPaymentOp()
		effects = append(effects,
			&AccountEffect{
				Timestamp:       closeTime,
				EffectType:      "account_credited",
				LedgerSequence:  sequence,
				AccountID:       payment.Destination.Address(),
				TransactionHash: txHash,
				Details: map[string]interface{}{
					"amount": payment.Amount,
					"asset":  payment.Asset.StringCanonical(),
					"from":   tx.Envelope.SourceAccount().ToAccountId().Address(),
				},
			},
			&AccountEffect{
				Timestamp:       closeTime,
				EffectType:      "account_debited",
				LedgerSequence:  sequence,
				AccountID:       tx.Envelope.SourceAccount().ToAccountId().Address(),
				TransactionHash: txHash,
				Details: map[string]interface{}{
					"amount": payment.Amount,
					"asset":  payment.Asset.StringCanonical(),
					"to":     payment.Destination.Address(),
				},
			},
		)

	case xdr.OperationTypeChangeTrust:
		trust := op.Body.MustChangeTrustOp()
		sourceAcc := tx.Envelope.SourceAccount().ToAccountId().Address()
		if trust.Limit == 0 {
			effects = append(effects, &AccountEffect{
				Timestamp:       closeTime,
				EffectType:      "trustline_removed",
				LedgerSequence:  sequence,
				AccountID:       sourceAcc,
				TransactionHash: txHash,
				Details: map[string]interface{}{
					"asset": trust.Line.ToAsset().StringCanonical(),
				},
			})
		}

	case xdr.OperationTypeAllowTrust:
		allow := op.Body.MustAllowTrustOp()
		effectType := "trustline_authorized"
		if allow.Authorize == 0 {
			effectType = "trustline_deauthorized"
		}
		effects = append(effects, &AccountEffect{
			Timestamp:       closeTime,
			EffectType:      effectType,
			LedgerSequence:  sequence,
			AccountID:       allow.Trustor.Address(),
			TransactionHash: txHash,
			Details: map[string]interface{}{
				"asset":      allow.Asset.ToAsset(tx.Envelope.SourceAccount().ToAccountId()).StringCanonical(),
				"trustor":    allow.Trustor.Address(),
				"authorizer": tx.Envelope.SourceAccount().ToAccountId().Address(),
			},
		})

	case xdr.OperationTypeAccountMerge:
		destination := op.Body.MustDestination().ToAccountId()
		effects = append(effects, &AccountEffect{
			Timestamp:       closeTime,
			EffectType:      "account_removed",
			LedgerSequence:  sequence,
			AccountID:       tx.Envelope.SourceAccount().ToAccountId().Address(),
			TransactionHash: txHash,
			Details: map[string]interface{}{
				"destination": destination.Address(),
			},
		})

	case xdr.OperationTypeManageData:
		data := op.Body.MustManageDataOp()
		effectType := "data_created"
		if data.DataValue == nil {
			effectType = "data_removed"
		}
		effects = append(effects, &AccountEffect{
			Timestamp:       closeTime,
			EffectType:      effectType,
			LedgerSequence:  sequence,
			AccountID:       tx.Envelope.SourceAccount().ToAccountId().Address(),
			TransactionHash: txHash,
			Details: map[string]interface{}{
				"name":      string(data.DataName),
				"has_value": data.DataValue != nil,
			},
		})
	}

	return effects, nil
}

func (p *AccountEffectProcessor) extractChangeEffects(changes []ingest.Change, tx ingest.LedgerTransaction, closeTime time.Time, sequence uint32, txHash string) ([]*AccountEffect, error) {
	var effects []*AccountEffect

	for _, change := range changes {
		switch change.Type {
		case xdr.LedgerEntryTypeTrustline:
			if change.Pre == nil && change.Post != nil {
				// Trustline created
				line := change.Post.Data.TrustLine
				effects = append(effects, &AccountEffect{
					Timestamp:       closeTime,
					EffectType:      "trustline_created",
					LedgerSequence:  sequence,
					AccountID:       line.AccountId.Address(),
					TransactionHash: txHash,
					Details: map[string]interface{}{
						"asset": line.Asset.ToAsset().StringCanonical(),
						"limit": line.Limit,
					},
				})
			} else if change.Pre != nil && change.Post != nil {
				// Trustline updated
				preLine := change.Pre.Data.TrustLine
				postLine := change.Post.Data.TrustLine
				if preLine.Limit != postLine.Limit {
					effects = append(effects, &AccountEffect{
						Timestamp:       closeTime,
						EffectType:      "trustline_updated",
						LedgerSequence:  sequence,
						AccountID:       postLine.AccountId.Address(),
						TransactionHash: txHash,
						Details: map[string]interface{}{
							"asset":     postLine.Asset.ToAsset().StringCanonical(),
							"limit":     postLine.Limit,
							"old_limit": preLine.Limit,
						},
					})
				}
			} else if change.Pre != nil && change.Post == nil {
				// Trustline removed
				line := change.Pre.Data.TrustLine
				effects = append(effects, &AccountEffect{
					Timestamp:       closeTime,
					EffectType:      "trustline_removed",
					LedgerSequence:  sequence,
					AccountID:       line.AccountId.Address(),
					TransactionHash: txHash,
					Details: map[string]interface{}{
						"asset": line.Asset.ToAsset().StringCanonical(),
					},
				})
			}
		}
	}

	return effects, nil
}

func (p *AccountEffectProcessor) forwardEffect(ctx context.Context, effect *AccountEffect) error {
	log.Printf("Forwarding effect: %+v", effect)

	p.mu.Lock()
	p.stats.ProcessedEffects++
	p.stats.LastEffectTime = effect.Timestamp
	p.mu.Unlock()

	// Marshal the effect to JSON before forwarding
	jsonBytes, err := json.Marshal(effect)
	if err != nil {
		return fmt.Errorf("error marshaling effect: %w", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %w", err)
		}
	}

	return nil
}

func (p *AccountEffectProcessor) isAccountInvolved(tx ingest.LedgerTransaction, op xdr.Operation, account string) bool {
	// Check source account
	if tx.Envelope.SourceAccount().ToAccountId().Address() == account {
		return true
	}

	// Check operation source account
	if op.SourceAccount != nil && op.SourceAccount.Address() == account {
		return true
	}

	// Check operation-specific accounts
	switch op.Body.Type {
	case xdr.OperationTypeCreateAccount:
		return op.Body.MustCreateAccountOp().Destination.Address() == account
	case xdr.OperationTypePayment:
		return op.Body.MustPaymentOp().Destination.ToAccountId().Address() == account
		// Add more operation types...
	}

	return false
}

// Helper function to generate a placeholder hash
func generatePlaceholderHash(ledgerSequence uint32) xdr.Hash {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("ledger-%d-placeholder", ledgerSequence)))
	var hash xdr.Hash
	copy(hash[:], h.Sum(nil))
	return hash
}
