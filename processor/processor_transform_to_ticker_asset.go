package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// TickerAsset represents the final ticker asset data structure
// Amount represents the operation amount in stroops (1 XLM = 10^7 stroops)
// - For payments: the payment amount
// - For claimable balances: the balance amount
// - For trustline operations: not used
type TickerAsset struct {
	Code                        string    `json:"code"`
	Issuer                      string    `json:"issuer"`
	AssetType                   string    `json:"asset_type"`
	Amount                      string    `json:"amount"`
	AuthRequired                bool      `json:"auth_required"`
	AuthRevocable               bool      `json:"auth_revocable"`
	AuthImmutable               bool      `json:"auth_immutable"`
	AuthClawback                bool      `json:"auth_clawback"`
	IsValid                     bool      `json:"is_valid"`
	ValidationError             string    `json:"validation_error"`
	LastValid                   time.Time `json:"last_valid"`
	LastChecked                 time.Time `json:"last_checked"`
	FirstSeenLedger             uint32    `json:"first_seen_ledger"`
	DisplayDecimals             int       `json:"display_decimals"`
	HomeDomain                  string    `json:"home_domain"`
	Name                        string    `json:"name"`
	Desc                        string    `json:"description"`
	Conditions                  string    `json:"conditions"`
	IsAssetAnchored             bool      `json:"is_asset_anchored"`
	FixedNumber                 int       `json:"fixed_number"`
	MaxNumber                   int       `json:"max_number"`
	IsUnlimited                 bool      `json:"is_unlimited"`
	RedemptionInstructions      string    `json:"redemption_instructions"`
	CollateralAddresses         string    `json:"collateral_addresses"`
	CollateralAddressSignatures string    `json:"collateral_address_signatures"`
	Countries                   string    `json:"countries"`
	Status                      string    `json:"status"`
	Type                        string    `json:"type"`
	OperationType               string    `json:"operation_type"`
}

// TransformToTickerAssetProcessor transforms xdr.LedgerCloseMeta data into TickerAsset format
type TransformToTickerAssetProcessor struct {
	processors        []Processor
	networkPassphrase string
}

func NewTransformToTickerAssetProcessor(config map[string]interface{}) (*TransformToTickerAssetProcessor, error) {
	networkPassphrase, ok := config["network_passphrase"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration: missing 'network_passphrase'")
	}
	return &TransformToTickerAssetProcessor{
		networkPassphrase: networkPassphrase,
	}, nil
}

func (p *TransformToTickerAssetProcessor) Subscribe(processor Processor) {
	p.processors = append(p.processors, processor)
}

func (p *TransformToTickerAssetProcessor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return fmt.Errorf("expected xdr.LedgerCloseMeta, got %T", msg.Payload)
	}

	// Read ledger transactions
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return fmt.Errorf("error creating transaction reader: %v", err)
	}
	defer txReader.Close()
	ledgerSeqNum := ledgerCloseMeta.LedgerSequence()

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading transaction: %v", err)
		}

		// Process each operation in the transaction
		for _, op := range tx.Envelope.Operations() {
			switch op.Body.Type {
			case xdr.OperationTypeSetTrustLineFlags,
				xdr.OperationTypeChangeTrust,
				xdr.OperationTypePayment,
				xdr.OperationTypeCreateClaimableBalance:
				// Extract asset data from relevant operations
				asset, err := p.extractAssetData(op, tx, ledgerSeqNum)
				if err != nil {
					return fmt.Errorf("error extracting asset data: %v", err)
				}

				// Skip if no asset was extracted
				if asset == nil {
					continue
				}

				// Validate and enrich asset data
				asset = p.enrichAssetData(asset)

				// Forward processed asset to next processors
				if err := p.forwardToProcessors(ctx, *asset); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (p *TransformToTickerAssetProcessor) extractAssetData(op xdr.Operation, tx ingest.LedgerTransaction, ledgerSeqNum uint32) (*TickerAsset, error) {
	asset := &TickerAsset{
		LastChecked:     time.Now(),
		FirstSeenLedger: ledgerSeqNum,
	}
	var err error
	switch op.Body.Type {
	case xdr.OperationTypeSetTrustLineFlags:
		asset.OperationType = "set_trust_line_flags"
		asset.Type = "set_trust_line_flags"
		err = p.populateFromTrustlineFlags(op, asset)
	case xdr.OperationTypeChangeTrust:
		asset.OperationType = "change_trust"
		asset.Type = "change_trust"
		err = p.populateFromChangeTrust(op, asset)
	case xdr.OperationTypePayment:
		asset.OperationType = "payment"
		asset.Type = "payment"
		err = p.populateFromPayment(op, asset)
	case xdr.OperationTypeCreateClaimableBalance:
		asset.OperationType = "create_claimable_balance"
		asset.Type = "claimable_balance"
		err = p.populateFromClaimableBalance(op, asset)
	default:
		s := strings.ToLower(cleanString(op.Body.Type.String()))
		if s == "create_claimable_balance" {
			asset.OperationType = "create_claimable_balance"
			asset.Type = "claimable_balance"
			err = p.populateFromClaimableBalance(op, asset)
		} else {
			return nil, fmt.Errorf("unsupported operation type: %s", s)
		}
	}
	if err != nil {
		return nil, err
	}
	return asset, nil
}

func (p *TransformToTickerAssetProcessor) populateFromTrustlineFlags(op xdr.Operation, asset *TickerAsset) error {
	flags := op.Body.MustSetTrustLineFlagsOp()
	asset.AuthRequired = (uint32(flags.SetFlags) & uint32(xdr.TrustLineFlagsAuthorizedFlag)) != 0
	asset.AuthRevocable = (uint32(flags.SetFlags) & uint32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag)) != 0

	switch flags.Asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		asset.Code = "XLM"
		asset.AssetType = "native"
		asset.Issuer = ""
	default:
		asset.Code = cleanString(flags.Asset.GetCode())
		asset.Issuer = flags.Asset.GetIssuer()
		asset.AssetType = getAssetTypeString(flags.Asset.Type)
	}
	return nil
}

// Helper function to convert asset type to clean string
func getAssetTypeString(assetType xdr.AssetType) string {
	switch assetType {
	case xdr.AssetTypeAssetTypeNative:
		return "native"
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		return "credit_alphanum4"
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		return "credit_alphanum12"
	case xdr.AssetTypeAssetTypePoolShare:
		return "liquidity_pool_share"
	default:
		return "unknown"
	}
}

func (p *TransformToTickerAssetProcessor) populateFromChangeTrust(op xdr.Operation, asset *TickerAsset) error {
	changeTrust := op.Body.ChangeTrustOp

	switch changeTrust.Line.Type {
	case xdr.AssetTypeAssetTypeNative:
		asset.Code = "XLM"
		asset.AssetType = "native"
		asset.Issuer = ""
	case xdr.AssetTypeAssetTypePoolShare:
		// Handle pool shares by deriving a descriptive pool id from liquidity pool parameters.
		asset.AssetType = "liquidity_pool_share"
		if changeTrust.Line.LiquidityPool != nil {
			lp := changeTrust.Line.LiquidityPool

			// Extract AssetA code from ConstantProduct parameters.
			var assetACode string
			switch lp.ConstantProduct.AssetA.Type {
			case xdr.AssetTypeAssetTypeNative:
				assetACode = "XLM"
			default:
				assetACode = cleanString(lp.ConstantProduct.AssetA.GetCode())
			}

			// Extract AssetB code from ConstantProduct parameters.
			var assetBCode string
			switch lp.ConstantProduct.AssetB.Type {
			case xdr.AssetTypeAssetTypeNative:
				assetBCode = "XLM"
			default:
				assetBCode = cleanString(lp.ConstantProduct.AssetB.GetCode())
			}

			// Create a descriptive liquidity pool identifier.
			// For example, it will look like: "lp:XLM-USD:30"
			asset.Issuer = fmt.Sprintf("lp:%s-%s:%d", assetACode, assetBCode, lp.ConstantProduct.Fee)
			asset.Code = "POOL"
		} else {
			asset.Code = "POOL"
			asset.Issuer = "unknown-pool"
		}
	default:
		// For the remaining asset types, use the standard conversion.
		xdrAsset := changeTrust.Line.ToAsset()
		if xdrAsset == (xdr.Asset{}) {
			return fmt.Errorf("invalid asset in change trust operation")
		}
		asset.Code = cleanString(xdrAsset.GetCode())
		asset.Issuer = xdrAsset.GetIssuer()
		asset.AssetType = getAssetTypeString(changeTrust.Line.Type)
	}
	return nil
}

func (p *TransformToTickerAssetProcessor) populateFromPayment(op xdr.Operation, asset *TickerAsset) error {
	payment := op.Body.PaymentOp

	switch payment.Asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		asset.Code = "XLM"
		asset.AssetType = "native"
		asset.Issuer = ""
		asset.Amount = strconv.FormatInt(int64(payment.Amount), 10)
		return nil
	default:
		asset.Code = cleanString(payment.Asset.GetCode())
		asset.Issuer = payment.Asset.GetIssuer()
		if asset.Issuer == "" {
			return fmt.Errorf("missing issuer for non-native asset")
		}
		asset.AssetType = getAssetTypeString(payment.Asset.Type)
		asset.Amount = strconv.FormatInt(int64(payment.Amount), 10)
	}

	return nil
}

func (p *TransformToTickerAssetProcessor) populateFromClaimableBalance(op xdr.Operation, asset *TickerAsset) error {
	log.Println("populateFromClaimableBalance called")
	claimableBalance := op.Body.CreateClaimableBalanceOp
	log.Printf("claimableBalance asset type: %v", claimableBalance.Asset.Type)

	switch claimableBalance.Asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		asset.Code = "XLM"
		asset.AssetType = "native"
		asset.Issuer = ""
		asset.Amount = strconv.FormatInt(int64(claimableBalance.Amount), 10)
		return nil
	default:
		asset.Code = cleanString(claimableBalance.Asset.GetCode())
		asset.Issuer = claimableBalance.Asset.GetIssuer()
		if asset.Issuer == "" {
			return fmt.Errorf("missing issuer for non-native asset")
		}
		asset.AssetType = getAssetTypeString(claimableBalance.Asset.Type)
		asset.Amount = strconv.FormatInt(int64(claimableBalance.Amount), 10)
	}

	return nil
}

func (p *TransformToTickerAssetProcessor) enrichAssetData(asset *TickerAsset) *TickerAsset {
	// Set default values
	if asset.DisplayDecimals == 0 {
		asset.DisplayDecimals = 7
	}

	// Validate asset
	asset.IsValid = true
	if asset.Code == "" {
		asset.IsValid = false
		asset.ValidationError = "Missing asset code"
	}

	// Set last valid time if asset is valid
	if asset.IsValid {
		asset.LastValid = time.Now()
	}

	return asset
}

func (p *TransformToTickerAssetProcessor) forwardToProcessors(ctx context.Context, asset TickerAsset) error {
	// Marshal the asset to JSON bytes before forwarding
	jsonBytes, err := json.Marshal(asset)
	if err != nil {
		return fmt.Errorf("error marshaling ticker asset: %v", err)
	}

	for _, processor := range p.processors {
		if err := processor.Process(ctx, Message{Payload: jsonBytes}); err != nil {
			return fmt.Errorf("error in processor chain: %v", err)
		}
	}
	return nil
}

func cleanString(s string) string {
	// Remove null characters and trim spaces
	return strings.TrimRight(strings.TrimSpace(s), "\u0000")
}
