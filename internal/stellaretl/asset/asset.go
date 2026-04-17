package asset

import (
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/withObsrvr/cdp-pipeline-workflow/internal/stellaretl/toid"
	"github.com/withObsrvr/cdp-pipeline-workflow/internal/stellaretl/utils"
)

// AssetOutput is a representation of an asset that aligns with the BigQuery table history_assets
type AssetOutput struct {
	AssetCode      string    `json:"asset_code"`
	AssetIssuer    string    `json:"asset_issuer"`
	AssetType      string    `json:"asset_type"`
	AssetID        int64     `json:"asset_id"`
	ClosedAt       time.Time `json:"closed_at"`
	LedgerSequence uint32    `json:"ledger_sequence"`
}

// TransformAsset converts an asset-bearing operation into a form suitable for BigQuery
func TransformAsset(operation xdr.Operation, operationIndex int32, transactionIndex int32, ledgerSeq int32, lcm xdr.LedgerCloseMeta) (AssetOutput, error) {
	operationID := toid.New(ledgerSeq, int32(transactionIndex), operationIndex).ToInt64()

	var asset xdr.Asset
	switch operation.Body.Type {
	case xdr.OperationTypePayment:
		opPayment, ok := operation.Body.GetPaymentOp()
		if !ok {
			return AssetOutput{}, fmt.Errorf("could not access Payment info for this operation (id %d)", operationID)
		}
		asset = opPayment.Asset
	case xdr.OperationTypeManageSellOffer:
		opSellOffer, ok := operation.Body.GetManageSellOfferOp()
		if !ok {
			return AssetOutput{}, fmt.Errorf("could not access ManageSellOffer info for this operation (id %d)", operationID)
		}
		asset = opSellOffer.Selling
	case xdr.OperationTypeManageBuyOffer:
		opBuyOffer, ok := operation.Body.GetManageBuyOfferOp()
		if !ok {
			return AssetOutput{}, fmt.Errorf("could not access ManageBuyOffer info for this operation (id %d)", operationID)
		}
		asset = opBuyOffer.Buying
	case xdr.OperationTypeCreatePassiveSellOffer:
		opPassiveSellOffer, ok := operation.Body.GetCreatePassiveSellOfferOp()
		if !ok {
			return AssetOutput{}, fmt.Errorf("could not access CreatePassiveSellOffer info for this operation (id %d)", operationID)
		}
		asset = opPassiveSellOffer.Selling
	case xdr.OperationTypeChangeTrust:
		opChangeTrust, ok := operation.Body.GetChangeTrustOp()
		if !ok {
			return AssetOutput{}, fmt.Errorf("could not access ChangeTrust info for this operation (id %d)", operationID)
		}
		asset = opChangeTrust.Line.ToAsset()
	case xdr.OperationTypePathPaymentStrictReceive:
		opPathPayment, ok := operation.Body.GetPathPaymentStrictReceiveOp()
		if !ok {
			return AssetOutput{}, fmt.Errorf("could not access PathPaymentStrictReceive info for this operation (id %d)", operationID)
		}
		asset = opPathPayment.DestAsset
	case xdr.OperationTypePathPaymentStrictSend:
		opPathPayment, ok := operation.Body.GetPathPaymentStrictSendOp()
		if !ok {
			return AssetOutput{}, fmt.Errorf("could not access PathPaymentStrictSend info for this operation (id %d)", operationID)
		}
		asset = opPathPayment.DestAsset
	default:
		return AssetOutput{}, fmt.Errorf("operation of type %d does not contain a supported asset (id %d)", operation.Body.Type, operationID)
	}

	outputAsset, err := TransformSingleAsset(asset)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("%s (id %d)", err.Error(), operationID)
	}

	outputCloseTime, err := utils.GetCloseTime(lcm)
	if err != nil {
		return AssetOutput{}, err
	}
	outputAsset.ClosedAt = outputCloseTime
	outputAsset.LedgerSequence = utils.GetLedgerSequence(lcm)

	return outputAsset, nil
}

func TransformSingleAsset(asset xdr.Asset) (AssetOutput, error) {
	var outputAssetType, outputAssetCode, outputAssetIssuer string
	err := asset.Extract(&outputAssetType, &outputAssetCode, &outputAssetIssuer)
	if err != nil {
		return AssetOutput{}, fmt.Errorf("could not extract asset from this operation")
	}

	farmAssetID := utils.FarmHashAsset(outputAssetCode, outputAssetIssuer, outputAssetType)

	return AssetOutput{
		AssetCode:   outputAssetCode,
		AssetIssuer: outputAssetIssuer,
		AssetType:   outputAssetType,
		AssetID:     farmAssetID,
	}, nil
}
