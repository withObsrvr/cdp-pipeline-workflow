package processor

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// ============================================
// Ledger Extraction
// ============================================

// extractLedgerData extracts ledger data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractLedgerData(lcm *xdr.LedgerCloseMeta, ledgerRange uint32) *BronzeLedgerRecord {
	header := p.getLedgerHeader(lcm)

	record := &BronzeLedgerRecord{
		Sequence:           uint32(header.Header.LedgerSeq),
		LedgerHash:         hex.EncodeToString(header.Hash[:]),
		PreviousLedgerHash: hex.EncodeToString(header.Header.PreviousLedgerHash[:]),
		ClosedAt:           time.Unix(int64(header.Header.ScpValue.CloseTime), 0),
		ProtocolVersion:    uint32(header.Header.LedgerVersion),
		TotalCoins:         int64(header.Header.TotalCoins),
		FeePool:            int64(header.Header.FeePool),
		BaseFee:            uint32(header.Header.BaseFee),
		BaseReserve:        uint32(header.Header.BaseReserve),
		MaxTxSetSize:       uint32(header.Header.MaxTxSetSize),
		LedgerRange:        &ledgerRange,
	}

	// Count transactions and operations
	var txCount, failedCount, operationCount, txSetOperationCount uint32

	switch lcm.V {
	case 0:
		v0 := lcm.MustV0()
		txCount = uint32(len(v0.TxSet.Txs))
		for _, tx := range v0.TxSet.Txs {
			opCount := uint32(len(tx.Operations()))
			txSetOperationCount += opCount
			operationCount += opCount
		}
	case 1:
		v1 := lcm.MustV1()
		txCount = uint32(len(v1.TxProcessing))
		for _, txApply := range v1.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	case 2:
		v2 := lcm.MustV2()
		txCount = uint32(len(v2.TxProcessing))
		for _, txApply := range v2.TxProcessing {
			if opResults, ok := txApply.Result.Result.OperationResults(); ok {
				opCount := uint32(len(opResults))
				txSetOperationCount += opCount
				if txApply.Result.Result.Successful() {
					operationCount += opCount
				} else {
					failedCount++
				}
			} else {
				failedCount++
			}
		}
	}

	record.TransactionCount = &txCount
	record.SuccessfulTxCount = txCount - failedCount
	record.FailedTxCount = failedCount
	record.OperationCount = &operationCount
	record.TxSetOperationCount = &txSetOperationCount

	// Extract Soroban fields (Protocol 20+)
	if lcmV1, ok := lcm.GetV1(); ok {
		if extV1, ok := lcmV1.Ext.GetV1(); ok {
			sorobanFee := int64(extV1.SorobanFeeWrite1Kb)
			record.SorobanFeeWrite1KB = &sorobanFee
		}
	} else if lcmV2, ok := lcm.GetV2(); ok {
		if extV1, ok := lcmV2.Ext.GetV1(); ok {
			sorobanFee := int64(extV1.SorobanFeeWrite1Kb)
			record.SorobanFeeWrite1KB = &sorobanFee
		}
	}

	// Extract consensus metadata (node_id and signature)
	if lcValueSig, ok := header.Header.ScpValue.Ext.GetLcValueSignature(); ok {
		nodeIDBytes := lcValueSig.NodeId.Ed25519
		if nodeIDStr, err := strkey.Encode(strkey.VersionByteAccountID, nodeIDBytes[:]); err == nil {
			record.NodeID = &nodeIDStr
		}
		sig := base64.StdEncoding.EncodeToString(lcValueSig.Signature[:])
		record.Signature = &sig
	}

	// Full ledger header as XDR (base64 encoded)
	if ledgerHeaderBytes, err := header.Header.MarshalBinary(); err == nil {
		headerB64 := base64.StdEncoding.EncodeToString(ledgerHeaderBytes)
		record.LedgerHeader = &headerB64
	}

	// Extract state tracking (Protocol 20+)
	if lcmV1, ok := lcm.GetV1(); ok {
		sorobanStateSize := uint64(lcmV1.TotalByteSizeOfLiveSorobanState)
		record.BucketListSize = &sorobanStateSize
		record.LiveSorobanStateSize = &sorobanStateSize
	} else if lcmV2, ok := lcm.GetV2(); ok {
		sorobanStateSize := uint64(lcmV2.TotalByteSizeOfLiveSorobanState)
		record.BucketListSize = &sorobanStateSize
		record.LiveSorobanStateSize = &sorobanStateSize
	}

	// Extract Protocol 23 (CAP-62) eviction count
	if lcmV1, ok := lcm.GetV1(); ok {
		evictedCount := uint32(len(lcmV1.EvictedKeys))
		record.EvictedKeysCount = &evictedCount
	} else if lcmV2, ok := lcm.GetV2(); ok {
		evictedCount := uint32(len(lcmV2.EvictedKeys))
		record.EvictedKeysCount = &evictedCount
	}

	return record
}

// ============================================
// Transaction Extraction
// ============================================

// extractTransactions extracts transaction data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractTransactions(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeTransactionRecord {
	var transactions []*BronzeTransactionRecord

	reader, err := p.getTransactionReader(lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create transaction reader: %v", err)
		return transactions
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading transaction in ledger %d: %v", ledgerSeq, err)
			continue
		}

		acctSeq := int64(tx.Envelope.SeqNum())
		lr := ledgerRange
		memoType := "none"
		record := &BronzeTransactionRecord{
			LedgerSequence:        ledgerSeq,
			TransactionHash:       hex.EncodeToString(tx.Result.TransactionHash[:]),
			SourceAccount:         tx.Envelope.SourceAccount().ToAccountId().Address(),
			FeeCharged:            int64(tx.Result.Result.FeeCharged),
			MaxFee:                int64(tx.Envelope.Fee()),
			Successful:            tx.Result.Successful(),
			TransactionResultCode: tx.Result.Result.Result.Code.String(),
			OperationCount:        int32(len(tx.Envelope.Operations())),
			CreatedAt:             closedAt,
			AccountSequence:       &acctSeq,
			LedgerRange:           &lr,
			MemoType:              &memoType,
			SignaturesCount:       int32(len(tx.Envelope.Signatures())),
			NewAccount:            false,
		}

		// Extract memo
		memo := tx.Envelope.Memo()
		switch memo.Type {
		case xdr.MemoTypeMemoNone:
			mt := "none"
			record.MemoType = &mt
		case xdr.MemoTypeMemoText:
			mt := "text"
			record.MemoType = &mt
			if text, ok := memo.GetText(); ok {
				m := string(text)
				record.Memo = &m
			}
		case xdr.MemoTypeMemoId:
			mt := "id"
			record.MemoType = &mt
			if id, ok := memo.GetId(); ok {
				m := fmt.Sprintf("%d", id)
				record.Memo = &m
			}
		case xdr.MemoTypeMemoHash:
			mt := "hash"
			record.MemoType = &mt
			if hash, ok := memo.GetHash(); ok {
				m := hex.EncodeToString(hash[:])
				record.Memo = &m
			}
		case xdr.MemoTypeMemoReturn:
			mt := "return"
			record.MemoType = &mt
			if ret, ok := memo.GetRetHash(); ok {
				m := hex.EncodeToString(ret[:])
				record.Memo = &m
			}
		}

		// Extract time bounds if present
		if timeBounds := tx.Envelope.TimeBounds(); timeBounds != nil {
			minTime := int64(timeBounds.MinTime)
			maxTime := int64(timeBounds.MaxTime)
			record.TimeboundsMinTime = &minTime
			record.TimeboundsMaxTime = &maxTime
		}

		// Extract Soroban resources if present
		if tx.UnsafeMeta.V == 3 {
			if tx.UnsafeMeta.V3.SorobanMeta != nil {
				sorobanMeta := tx.UnsafeMeta.V3.SorobanMeta

				// Extract resource consumption
				instructions := int64(sorobanMeta.Ext.V1.TotalNonRefundableResourceFeeCharged)
				record.SorobanResourcesInstructions = &instructions

				// Events count
				eventsCount := int32(len(sorobanMeta.Events))
				record.SorobanContractEventsCount = &eventsCount
			}
		}

		// XDR fields (optional, can be expensive)
		if envBytes, err := tx.Envelope.MarshalBinary(); err == nil {
			envB64 := base64.StdEncoding.EncodeToString(envBytes)
			record.TxEnvelope = &envB64
		}
		if resultBytes, err := tx.Result.Result.MarshalBinary(); err == nil {
			resultB64 := base64.StdEncoding.EncodeToString(resultBytes)
			record.TxResult = &resultB64
		}

		transactions = append(transactions, record)
	}

	return transactions
}

// ============================================
// Operations Extraction
// ============================================

// extractOperations extracts operation data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractOperations(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeOperationRecord {
	var operations []*BronzeOperationRecord

	reader, err := p.getTransactionReader(lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create transaction reader for operations: %v", err)
		return operations
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading transaction for operations: %v", err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		txSuccessful := tx.Result.Successful()

		for i, op := range tx.Envelope.Operations() {
			lr := ledgerRange
			record := &BronzeOperationRecord{
				TransactionHash:       txHash,
				OperationIndex:        int32(i),
				LedgerSequence:        ledgerSeq,
				Type:                  int32(op.Body.Type),
				TypeString:            op.Body.Type.String(),
				CreatedAt:             closedAt,
				TransactionSuccessful: txSuccessful,
				LedgerRange:           &lr,
			}

			// Source account
			if op.SourceAccount != nil {
				record.SourceAccount = op.SourceAccount.ToAccountId().Address()
			} else {
				record.SourceAccount = tx.Envelope.SourceAccount().ToAccountId().Address()
			}

			// Extract operation-specific fields based on type
			extractBronzeOperationDetails(op, record)

			operations = append(operations, record)
		}
	}

	return operations
}

// extractBronzeOperationDetails extracts operation-specific fields for Bronze records
func extractBronzeOperationDetails(op xdr.Operation, record *BronzeOperationRecord) {
	switch op.Body.Type {
	case xdr.OperationTypeCreateAccount:
		if createAcct, ok := op.Body.GetCreateAccountOp(); ok {
			dest := createAcct.Destination.Address()
			record.Destination = &dest
			startingBalance := int64(createAcct.StartingBalance)
			record.StartingBalance = &startingBalance
		}
	case xdr.OperationTypePayment:
		if payment, ok := op.Body.GetPaymentOp(); ok {
			dest := payment.Destination.ToAccountId().Address()
			record.Destination = &dest
			amount := int64(payment.Amount)
			record.Amount = &amount

			canonical, assetType, code, issuer := extractAsset(payment.Asset)
			record.Asset = &canonical
			record.AssetType = &assetType
			record.AssetCode = code
			record.AssetIssuer = issuer
		}
	case xdr.OperationTypeChangeTrust:
		if changeTrust, ok := op.Body.GetChangeTrustOp(); ok {
			limit := int64(changeTrust.Limit)
			record.TrustlineLimit = &limit

			// Use extractChangeTrustAsset to handle all asset types including pool shares
			assetType, assetCode, assetIssuer := extractChangeTrustAsset(changeTrust.Line)
			record.AssetType = &assetType
			if assetCode != "" {
				record.AssetCode = &assetCode
				canonical := assetCode
				if assetIssuer != "" && assetIssuer != "pool" {
					canonical = fmt.Sprintf("%s:%s", assetCode, assetIssuer)
				}
				record.Asset = &canonical
			}
			if assetIssuer != "" {
				record.AssetIssuer = &assetIssuer
			}
		}
	case xdr.OperationTypeManageSellOffer, xdr.OperationTypeManageBuyOffer:
		if sellOffer, ok := op.Body.GetManageSellOfferOp(); ok {
			amount := int64(sellOffer.Amount)
			record.Amount = &amount

			offerID := int64(sellOffer.OfferId)
			record.OfferID = &offerID

			priceStr := fmt.Sprintf("%d/%d", sellOffer.Price.N, sellOffer.Price.D)
			record.Price = &priceStr

			sCanonical, sType, sCode, sIssuer := extractAsset(sellOffer.Selling)
			record.SellingAsset = &sCanonical
			record.SellingAssetType = &sType
			record.SellingAssetCode = sCode
			record.SellingAssetIssuer = sIssuer

			bCanonical, bType, bCode, bIssuer := extractAsset(sellOffer.Buying)
			record.BuyingAsset = &bCanonical
			record.BuyingAssetType = &bType
			record.BuyingAssetCode = bCode
			record.BuyingAssetIssuer = bIssuer
		}
	case xdr.OperationTypeInvokeHostFunction:
		sorobanOp := "invoke_host_function"
		record.SorobanOperation = &sorobanOp
	}
}

// ============================================
// Effects Extraction
// ============================================

// extractEffects extracts effect data from LedgerCloseMeta
// Note: Effects require additional processing - this is a simplified version
func (p *BronzeLedgerReaderProcessor) extractEffects(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeEffectRecord {
	var effects []*BronzeEffectRecord

	reader, err := p.getTransactionReader(lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create transaction reader for effects: %v", err)
		return effects
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading transaction for effects: %v", err)
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		lr := ledgerRange

		// Extract balance changes from fee changes (simplified effects)
		for effectIndex, change := range tx.FeeChanges {
			// Fee changes are LedgerEntryChanges with Type field for state/created/updated/removed
			if state, ok := change.GetState(); ok {
				if state.Data.Type == xdr.LedgerEntryTypeAccount {
					if account, ok := state.Data.GetAccount(); ok {
						accountID := account.AccountId.Address()
						record := &BronzeEffectRecord{
							LedgerSequence:   ledgerSeq,
							TransactionHash:  txHash,
							OperationIndex:   0,
							EffectIndex:      int32(effectIndex),
							EffectType:       0, // Account debited/credited
							EffectTypeString: "account_debited",
							AccountID:        &accountID,
							CreatedAt:        closedAt,
							LedgerRange:      &lr,
						}
						effects = append(effects, record)
					}
				}
			}
		}
	}

	return effects
}

// ============================================
// Trades Extraction
// ============================================

// extractTrades extracts trade data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractTrades(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeTradeRecord {
	var trades []*BronzeTradeRecord

	reader, err := p.getTransactionReader(lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create transaction reader for trades: %v", err)
		return trades
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading transaction for trades: %v", err)
			continue
		}

		if !tx.Result.Successful() {
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])

		// Look for trades in operation results
		opResults, _ := tx.Result.Result.OperationResults()
		for opIndex, opResult := range opResults {
			trades = append(trades, extractTradesFromOpResult(opResult, txHash, ledgerSeq, int32(opIndex), closedAt, ledgerRange)...)
		}
	}

	return trades
}

// extractTradesFromOpResult extracts trades from an operation result
func extractTradesFromOpResult(opResult xdr.OperationResult, txHash string, ledgerSeq uint32, opIndex int32, closedAt time.Time, ledgerRange uint32) []*BronzeTradeRecord {
	var trades []*BronzeTradeRecord

	tr, ok := opResult.GetTr()
	if !ok {
		return trades
	}

	var claimedOffers []xdr.ClaimAtom

	switch tr.Type {
	case xdr.OperationTypeManageSellOffer:
		if result, ok := tr.GetManageSellOfferResult(); ok {
			if success, ok := result.GetSuccess(); ok {
				claimedOffers = success.OffersClaimed
			}
		}
	case xdr.OperationTypeManageBuyOffer:
		if result, ok := tr.GetManageBuyOfferResult(); ok {
			if success, ok := result.GetSuccess(); ok {
				claimedOffers = success.OffersClaimed
			}
		}
	case xdr.OperationTypePathPaymentStrictReceive:
		if result, ok := tr.GetPathPaymentStrictReceiveResult(); ok {
			if success, ok := result.GetSuccess(); ok {
				claimedOffers = success.Offers
			}
		}
	case xdr.OperationTypePathPaymentStrictSend:
		if result, ok := tr.GetPathPaymentStrictSendResult(); ok {
			if success, ok := result.GetSuccess(); ok {
				claimedOffers = success.Offers
			}
		}
	}

	for tradeIndex, claim := range claimedOffers {
		lr := ledgerRange
		record := &BronzeTradeRecord{
			LedgerSequence:  ledgerSeq,
			TransactionHash: txHash,
			OperationIndex:  opIndex,
			TradeIndex:      int32(tradeIndex),
			CreatedAt:       closedAt,
			LedgerRange:     &lr,
		}

		// Handle different ClaimAtom types
		switch claim.Type {
		case xdr.ClaimAtomTypeClaimAtomTypeV0:
			// V0 claim atom (older format)
			v0 := claim.MustV0()
			record.TradeType = "orderbook"
			record.TradeTimestamp = closedAt
			record.SellerAccount = fmt.Sprintf("%x", v0.SellerEd25519)
			record.BuyerAccount = "unknown"
			record.SellingAmount = strconv.FormatInt(int64(v0.AmountSold), 10)
			record.BuyingAmount = strconv.FormatInt(int64(v0.AmountBought), 10)

			// Calculate price
			if v0.AmountSold > 0 {
				price := float64(v0.AmountBought) / float64(v0.AmountSold)
				record.Price = strconv.FormatFloat(price, 'f', 7, 64)
			} else {
				record.Price = "0"
			}

			// Assets
			_, _, code, issuer := extractAsset(v0.AssetSold)
			record.SellingAssetCode = code
			record.SellingAssetIssuer = issuer
			_, _, code, issuer = extractAsset(v0.AssetBought)
			record.BuyingAssetCode = code
			record.BuyingAssetIssuer = issuer

		case xdr.ClaimAtomTypeClaimAtomTypeOrderBook:
			// OrderBook claim atom (newer format)
			ob := claim.MustOrderBook()
			record.TradeType = "orderbook"
			record.TradeTimestamp = closedAt
			record.SellerAccount = ob.SellerId.Address()
			record.BuyerAccount = "unknown"
			record.SellingAmount = strconv.FormatInt(int64(ob.AmountSold), 10)
			record.BuyingAmount = strconv.FormatInt(int64(ob.AmountBought), 10)

			// Calculate price
			if ob.AmountSold > 0 {
				price := float64(ob.AmountBought) / float64(ob.AmountSold)
				record.Price = strconv.FormatFloat(price, 'f', 7, 64)
			} else {
				record.Price = "0"
			}

			// Assets
			_, _, code, issuer := extractAsset(ob.AssetSold)
			record.SellingAssetCode = code
			record.SellingAssetIssuer = issuer
			_, _, code, issuer = extractAsset(ob.AssetBought)
			record.BuyingAssetCode = code
			record.BuyingAssetIssuer = issuer

		case xdr.ClaimAtomTypeClaimAtomTypeLiquidityPool:
			// Liquidity pool trade (Protocol 18+)
			lp := claim.MustLiquidityPool()
			record.TradeType = "liquidity_pool"
			record.TradeTimestamp = closedAt
			record.SellerAccount = "unknown" // LP trades don't have a seller
			record.BuyerAccount = hex.EncodeToString(lp.LiquidityPoolId[:])

			// From pool's perspective:
			// - assetBought/amountBought: sent TO pool (user sold this)
			// - assetSold/amountSold: taken FROM pool (user bought this)
			record.SellingAmount = strconv.FormatInt(int64(lp.AmountBought), 10)
			record.BuyingAmount = strconv.FormatInt(int64(lp.AmountSold), 10)

			// Calculate price (selling / buying)
			if lp.AmountSold > 0 {
				price := float64(lp.AmountBought) / float64(lp.AmountSold)
				record.Price = strconv.FormatFloat(price, 'f', 7, 64)
			} else {
				record.Price = "0"
			}

			// Assets (note the swap - from user's perspective)
			_, _, code, issuer := extractAsset(lp.AssetBought)
			record.SellingAssetCode = code
			record.SellingAssetIssuer = issuer
			_, _, code, issuer = extractAsset(lp.AssetSold)
			record.BuyingAssetCode = code
			record.BuyingAssetIssuer = issuer

		default:
			// Unknown claim atom type, skip
			continue
		}

		trades = append(trades, record)
	}

	return trades
}

// ============================================
// Native Balances Extraction
// ============================================

// extractNativeBalances extracts native XLM balance data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractNativeBalances(lcm *xdr.LedgerCloseMeta, ledgerSeq uint32, ledgerRange uint32) []*BronzeNativeBalanceRecord {
	var balances []*BronzeNativeBalanceRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader: %v", err)
		return balances
	}
	defer reader.Close()

	accountMap := make(map[string]*BronzeNativeBalanceRecord)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		var accountEntry *xdr.AccountEntry
		if change.Post != nil {
			if ae, ok := change.Post.Data.GetAccount(); ok {
				accountEntry = &ae
			}
		}

		if accountEntry == nil {
			continue
		}

		accountID := accountEntry.AccountId.Address()
		seqNum := int64(accountEntry.SeqNum)
		lr := ledgerRange
		record := &BronzeNativeBalanceRecord{
			AccountID:          accountID,
			Balance:            int64(accountEntry.Balance),
			BuyingLiabilities:  0,
			SellingLiabilities: 0,
			NumSubentries:      int32(accountEntry.NumSubEntries),
			NumSponsoring:      0,
			NumSponsored:       0,
			SequenceNumber:     &seqNum,
			LastModifiedLedger: ledgerSeq,
			LedgerSequence:     ledgerSeq,
			LedgerRange:        &lr,
		}

		if ext, ok := accountEntry.Ext.GetV1(); ok {
			record.BuyingLiabilities = int64(ext.Liabilities.Buying)
			record.SellingLiabilities = int64(ext.Liabilities.Selling)

			if ext2, ok := ext.Ext.GetV2(); ok {
				record.NumSponsoring = int32(ext2.NumSponsoring)
				record.NumSponsored = int32(ext2.NumSponsored)
			}
		}

		accountMap[accountID] = record
	}

	for _, balance := range accountMap {
		balances = append(balances, balance)
	}

	return balances
}

// ============================================
// Accounts Extraction
// ============================================

// extractAccounts extracts account data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractAccounts(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeAccountRecord {
	var accounts []*BronzeAccountRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader for accounts: %v", err)
		return accounts
	}
	defer reader.Close()

	accountMap := make(map[string]*BronzeAccountRecord)
	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change for accounts: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		var accountEntry *xdr.AccountEntry
		if change.Post != nil {
			if ae, ok := change.Post.Data.GetAccount(); ok {
				accountEntry = &ae
			}
		}

		if accountEntry == nil {
			continue
		}

		accountID := accountEntry.AccountId.Address()
		flags := int32(accountEntry.Flags)

		record := &BronzeAccountRecord{
			AccountID:           accountID,
			LedgerSequence:      ledgerSeq,
			ClosedAt:            closedAt,
			Balance:             strconv.FormatInt(int64(accountEntry.Balance), 10),
			SequenceNumber:      int64(accountEntry.SeqNum),
			NumSubentries:       int32(accountEntry.NumSubEntries),
			NumSponsoring:       0,
			NumSponsored:        0,
			MasterWeight:        int32(accountEntry.Thresholds[0]),
			LowThreshold:        int32(accountEntry.Thresholds[1]),
			MedThreshold:        int32(accountEntry.Thresholds[2]),
			HighThreshold:       int32(accountEntry.Thresholds[3]),
			Flags:               flags,
			AuthRequired:        (flags & int32(xdr.AccountFlagsAuthRequiredFlag)) != 0,
			AuthRevocable:       (flags & int32(xdr.AccountFlagsAuthRevocableFlag)) != 0,
			AuthImmutable:       (flags & int32(xdr.AccountFlagsAuthImmutableFlag)) != 0,
			AuthClawbackEnabled: (flags & int32(xdr.AccountFlagsAuthClawbackEnabledFlag)) != 0,
			CreatedAt:           closedAt,
			UpdatedAt:           closedAt,
			LedgerRange:         ledgerRange,
		}

		// Home domain
		if accountEntry.HomeDomain != "" {
			hd := string(accountEntry.HomeDomain)
			record.HomeDomain = &hd
		}

		// Extract sponsorship info
		if ext, ok := accountEntry.Ext.GetV1(); ok {
			if ext2, ok := ext.Ext.GetV2(); ok {
				record.NumSponsoring = int32(ext2.NumSponsoring)
				record.NumSponsored = int32(ext2.NumSponsored)
			}
		}

		// Extract signers as JSON
		if len(accountEntry.Signers) > 0 {
			var signerStrs []string
			for _, signer := range accountEntry.Signers {
				signerStr := fmt.Sprintf("%s:%d", signer.Key.Address(), signer.Weight)
				signerStrs = append(signerStrs, signerStr)
			}
			if signersJSON, err := json.Marshal(signerStrs); err == nil {
				s := string(signersJSON)
				record.Signers = &s
			}
		}

		accountMap[accountID] = record
	}

	for _, account := range accountMap {
		accounts = append(accounts, account)
	}

	return accounts
}

// ============================================
// Trustlines Extraction
// ============================================

// extractTrustlines extracts trustline data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractTrustlines(lcm *xdr.LedgerCloseMeta, ledgerSeq uint32, ledgerRange uint32) []*BronzeTrustlineRecord {
	var trustlines []*BronzeTrustlineRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader for trustlines: %v", err)
		return trustlines
	}
	defer reader.Close()

	closedAt := p.getClosedAt(lcm)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change for trustlines: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeTrustline {
			continue
		}

		var tlEntry *xdr.TrustLineEntry
		if change.Post != nil {
			if tl, ok := change.Post.Data.GetTrustLine(); ok {
				tlEntry = &tl
			}
		}

		if tlEntry == nil {
			continue
		}

		// Extract asset info - handle TrustLineAsset directly (including pool shares)
		assetType, assetCode, assetIssuer := extractTrustLineAsset(tlEntry.Asset)

		flags := int32(tlEntry.Flags)

		record := &BronzeTrustlineRecord{
			AccountID:                       tlEntry.AccountId.Address(),
			AssetCode:                       assetCode,
			AssetIssuer:                     assetIssuer,
			AssetType:                       assetType,
			Balance:                         strconv.FormatInt(int64(tlEntry.Balance), 10),
			TrustLimit:                      strconv.FormatInt(int64(tlEntry.Limit), 10),
			BuyingLiabilities:               "0",
			SellingLiabilities:              "0",
			Authorized:                      (flags & int32(xdr.TrustLineFlagsAuthorizedFlag)) != 0,
			AuthorizedToMaintainLiabilities: (flags & int32(xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag)) != 0,
			ClawbackEnabled:                 (flags & int32(xdr.TrustLineFlagsTrustlineClawbackEnabledFlag)) != 0,
			LedgerSequence:                  ledgerSeq,
			CreatedAt:                       closedAt,
			LedgerRange:                     ledgerRange,
		}

		// Extract liabilities if available
		if ext, ok := tlEntry.Ext.GetV1(); ok {
			record.BuyingLiabilities = strconv.FormatInt(int64(ext.Liabilities.Buying), 10)
			record.SellingLiabilities = strconv.FormatInt(int64(ext.Liabilities.Selling), 10)
		}

		trustlines = append(trustlines, record)
	}

	return trustlines
}

// ============================================
// Offers Extraction
// ============================================

// extractOffers extracts offer data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractOffers(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeOfferRecord {
	var offers []*BronzeOfferRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader for offers: %v", err)
		return offers
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change for offers: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeOffer {
			continue
		}

		var offerEntry *xdr.OfferEntry
		if change.Post != nil {
			if offer, ok := change.Post.Data.GetOffer(); ok {
				offerEntry = &offer
			}
		}

		if offerEntry == nil {
			continue
		}

		// Selling asset
		_, sType, sCode, sIssuer := extractAsset(offerEntry.Selling)
		// Buying asset
		_, bType, bCode, bIssuer := extractAsset(offerEntry.Buying)

		// Calculate price as decimal string
		priceStr := "0"
		if offerEntry.Price.D > 0 {
			price := float64(offerEntry.Price.N) / float64(offerEntry.Price.D)
			priceStr = strconv.FormatFloat(price, 'f', 7, 64)
		}

		record := &BronzeOfferRecord{
			OfferID:            int64(offerEntry.OfferId),
			SellerAccount:      offerEntry.SellerId.Address(),
			LedgerSequence:     ledgerSeq,
			ClosedAt:           closedAt,
			SellingAssetType:   sType,
			SellingAssetCode:   sCode,
			SellingAssetIssuer: sIssuer,
			BuyingAssetType:    bType,
			BuyingAssetCode:    bCode,
			BuyingAssetIssuer:  bIssuer,
			Amount:             strconv.FormatInt(int64(offerEntry.Amount), 10),
			Price:              priceStr,
			Flags:              int32(offerEntry.Flags),
			CreatedAt:          closedAt,
			LedgerRange:        ledgerRange,
		}

		offers = append(offers, record)
	}

	return offers
}

// ============================================
// Claimable Balances Extraction
// ============================================

// extractClaimableBalances extracts claimable balance data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractClaimableBalances(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeClaimableBalanceRecord {
	var balances []*BronzeClaimableBalanceRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader for claimable balances: %v", err)
		return balances
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change for claimable balances: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeClaimableBalance {
			continue
		}

		var cbEntry *xdr.ClaimableBalanceEntry
		if change.Post != nil {
			if cb, ok := change.Post.Data.GetClaimableBalance(); ok {
				cbEntry = &cb
			}
		}

		if cbEntry == nil {
			continue
		}

		// Get balance ID
		balanceIDBytes, err := cbEntry.BalanceId.MarshalBinary()
		if err != nil {
			continue
		}

		// Asset
		_, assetType, code, issuer := extractAsset(cbEntry.Asset)

		// Get sponsor from extension (if claimants have sponsors)
		sponsor := ""
		if len(cbEntry.Claimants) > 0 {
			// Try to get sponsor from the first claimant's V0 destination
			if claimant, ok := cbEntry.Claimants[0].GetV0(); ok {
				sponsor = claimant.Destination.Address()
			}
		}

		record := &BronzeClaimableBalanceRecord{
			BalanceID:      hex.EncodeToString(balanceIDBytes),
			Sponsor:        sponsor,
			LedgerSequence: ledgerSeq,
			ClosedAt:       closedAt,
			AssetType:      assetType,
			AssetCode:      code,
			AssetIssuer:    issuer,
			Amount:         int64(cbEntry.Amount),
			ClaimantsCount: int32(len(cbEntry.Claimants)),
			Flags:          int32(cbEntry.Ext.V),
			CreatedAt:      closedAt,
			LedgerRange:    ledgerRange,
		}

		balances = append(balances, record)
	}

	return balances
}

// ============================================
// Liquidity Pools Extraction
// ============================================

// extractLiquidityPools extracts liquidity pool data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractLiquidityPools(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeLiquidityPoolRecord {
	var pools []*BronzeLiquidityPoolRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader for liquidity pools: %v", err)
		return pools
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change for liquidity pools: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeLiquidityPool {
			continue
		}

		var lpEntry *xdr.LiquidityPoolEntry
		if change.Post != nil {
			if lp, ok := change.Post.Data.GetLiquidityPool(); ok {
				lpEntry = &lp
			}
		}

		if lpEntry == nil {
			continue
		}

		record := &BronzeLiquidityPoolRecord{
			LiquidityPoolID: hex.EncodeToString(lpEntry.LiquidityPoolId[:]),
			LedgerSequence:  ledgerSeq,
			ClosedAt:        closedAt,
			PoolType:        lpEntry.Body.Type.String(),
			CreatedAt:       closedAt,
			LedgerRange:     ledgerRange,
		}

		// Extract constant product pool details
		if cp, ok := lpEntry.Body.GetConstantProduct(); ok {
			record.Fee = int32(cp.Params.Fee)
			record.TrustlineCount = int32(cp.PoolSharesTrustLineCount)
			record.TotalPoolShares = int64(cp.TotalPoolShares)

			// Asset A
			_, aType, aCode, aIssuer := extractAsset(cp.Params.AssetA)
			record.AssetAType = aType
			record.AssetACode = aCode
			record.AssetAIssuer = aIssuer
			record.AssetAAmount = int64(cp.ReserveA)

			// Asset B
			_, bType, bCode, bIssuer := extractAsset(cp.Params.AssetB)
			record.AssetBType = bType
			record.AssetBCode = bCode
			record.AssetBIssuer = bIssuer
			record.AssetBAmount = int64(cp.ReserveB)
		}

		pools = append(pools, record)
	}

	return pools
}

// ============================================
// Contract Events Extraction
// ============================================

// extractContractEvents extracts contract event data from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractContractEvents(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeContractEventRecord {
	var events []*BronzeContractEventRecord

	reader, err := p.getTransactionReader(lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create transaction reader for contract events: %v", err)
		return events
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading transaction for contract events: %v", err)
			continue
		}

		// Use SDK helper method to get transaction events
		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			// Not a Soroban transaction or no events
			continue
		}

		txHash := hex.EncodeToString(tx.Result.TransactionHash[:])
		eventIndex := 0

		// Iterate over operation events (per-operation contract events)
		for opIndex, opEvents := range txEvents.OperationEvents {
			for _, event := range opEvents {
				// Create unique event ID
				eventID := fmt.Sprintf("%s-%d-%d", txHash[:16], opIndex, eventIndex)

				record := &BronzeContractEventRecord{
					EventID:                  eventID,
					LedgerSequence:           ledgerSeq,
					TransactionHash:          txHash,
					ClosedAt:                 closedAt,
					EventType:                event.Type.String(),
					InSuccessfulContractCall: tx.Result.Successful(),
					TopicsJSON:               "[]",
					TopicsDecoded:            "[]",
					DataXDR:                  "",
					DataDecoded:              "",
					TopicCount:               0,
					OperationIndex:           int32(opIndex),
					EventIndex:               int32(eventIndex),
					CreatedAt:                closedAt,
					LedgerRange:              ledgerRange,
				}

				// Contract ID
				if event.ContractId != nil {
					contractID := hex.EncodeToString((*event.ContractId)[:])
					record.ContractID = &contractID
				}

				// Topics (as JSON array)
				if len(event.Body.V0.Topics) > 0 {
					record.TopicCount = int32(len(event.Body.V0.Topics))
					var topicStrs []string
					for _, topic := range event.Body.V0.Topics {
						if topicBytes, err := topic.MarshalBinary(); err == nil {
							topicStrs = append(topicStrs, base64.StdEncoding.EncodeToString(topicBytes))
						}
					}
					if topicsJSON, err := json.Marshal(topicStrs); err == nil {
						record.TopicsJSON = string(topicsJSON)
						record.TopicsDecoded = string(topicsJSON) // Simplified - same as raw for now
					}
				}

				// Data (base64 encoded XDR)
				if dataBytes, err := event.Body.V0.Data.MarshalBinary(); err == nil {
					record.DataXDR = base64.StdEncoding.EncodeToString(dataBytes)
					record.DataDecoded = record.DataXDR // Simplified - same as raw for now
				}

				events = append(events, record)
				eventIndex++
			}
		}
	}

	return events
}

// ============================================
// Contract Data Extraction
// ============================================

// extractContractData extracts contract data entries from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractContractData(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeContractDataRecord {
	var data []*BronzeContractDataRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader for contract data: %v", err)
		return data
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change for contract data: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeContractData {
			continue
		}

		var cdEntry *xdr.ContractDataEntry
		var lastModified uint32
		var deleted bool

		if change.Post != nil {
			if cd, ok := change.Post.Data.GetContractData(); ok {
				cdEntry = &cd
				lastModified = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			}
		} else if change.Pre != nil {
			// Entry was deleted
			if cd, ok := change.Pre.Data.GetContractData(); ok {
				cdEntry = &cd
				lastModified = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}
		}

		if cdEntry == nil {
			continue
		}

		// Contract ID
		contractID := ""
		if cdEntry.Contract.Type == xdr.ScAddressTypeScAddressTypeContract {
			if contractHash, ok := cdEntry.Contract.GetContractId(); ok {
				contractID = hex.EncodeToString(contractHash[:])
			}
		}

		// Ledger key hash
		keyHash := ""
		if keyBytes, err := cdEntry.Key.MarshalBinary(); err == nil {
			hash := sha256.Sum256(keyBytes)
			keyHash = hex.EncodeToString(hash[:])
		}

		// Full XDR
		contractDataXDR := ""
		if xdrBytes, err := cdEntry.MarshalBinary(); err == nil {
			contractDataXDR = base64.StdEncoding.EncodeToString(xdrBytes)
		}

		record := &BronzeContractDataRecord{
			ContractID:         contractID,
			LedgerSequence:     ledgerSeq,
			LedgerKeyHash:      keyHash,
			ContractKeyType:    cdEntry.Key.Type.String(),
			ContractDurability: cdEntry.Durability.String(),
			LastModifiedLedger: int32(lastModified),
			LedgerEntryChange:  0, // Would need to track change type
			Deleted:            deleted,
			ClosedAt:           closedAt,
			ContractDataXDR:    contractDataXDR,
			CreatedAt:          closedAt,
			LedgerRange:        ledgerRange,
		}

		data = append(data, record)
	}

	return data
}

// ============================================
// Contract Code Extraction
// ============================================

// extractContractCode extracts contract code entries from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractContractCode(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeContractCodeRecord {
	var codes []*BronzeContractCodeRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader for contract code: %v", err)
		return codes
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change for contract code: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeContractCode {
			continue
		}

		var ccEntry *xdr.ContractCodeEntry
		var lastModified uint32
		var deleted bool

		if change.Post != nil {
			if cc, ok := change.Post.Data.GetContractCode(); ok {
				ccEntry = &cc
				lastModified = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			}
		} else if change.Pre != nil {
			if cc, ok := change.Pre.Data.GetContractCode(); ok {
				ccEntry = &cc
				lastModified = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}
		}

		if ccEntry == nil {
			continue
		}

		// Ledger key hash
		keyHash := ""
		if keyBytes, err := ccEntry.Hash.MarshalBinary(); err == nil {
			hash := sha256.Sum256(keyBytes)
			keyHash = hex.EncodeToString(hash[:])
		}

		record := &BronzeContractCodeRecord{
			ContractCodeHash:   hex.EncodeToString(ccEntry.Hash[:]),
			LedgerKeyHash:      keyHash,
			ContractCodeExtV:   int32(ccEntry.Ext.V),
			LastModifiedLedger: int32(lastModified),
			LedgerEntryChange:  0,
			Deleted:            deleted,
			ClosedAt:           closedAt,
			LedgerSequence:     ledgerSeq,
			CreatedAt:          closedAt,
			LedgerRange:        ledgerRange,
		}

		codes = append(codes, record)
	}

	return codes
}

// ============================================
// Config Settings Extraction
// ============================================

// extractConfigSettings extracts config setting entries from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractConfigSettings(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeConfigSettingRecord {
	var settings []*BronzeConfigSettingRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader for config settings: %v", err)
		return settings
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change for config settings: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeConfigSetting {
			continue
		}

		var csEntry *xdr.ConfigSettingEntry
		var lastModified uint32
		var deleted bool

		if change.Post != nil {
			if cs, ok := change.Post.Data.GetConfigSetting(); ok {
				csEntry = &cs
				lastModified = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			}
		} else if change.Pre != nil {
			if cs, ok := change.Pre.Data.GetConfigSetting(); ok {
				csEntry = &cs
				lastModified = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}
		}

		if csEntry == nil {
			continue
		}

		// Full XDR
		configXDR := ""
		if xdrBytes, err := csEntry.MarshalBinary(); err == nil {
			configXDR = base64.StdEncoding.EncodeToString(xdrBytes)
		}

		record := &BronzeConfigSettingRecord{
			ConfigSettingID:    int32(csEntry.ConfigSettingId),
			LedgerSequence:     ledgerSeq,
			LastModifiedLedger: int32(lastModified),
			Deleted:            deleted,
			ClosedAt:           closedAt,
			ConfigSettingXDR:   configXDR,
			CreatedAt:          closedAt,
			LedgerRange:        ledgerRange,
		}

		settings = append(settings, record)
	}

	return settings
}

// ============================================
// TTL Extraction
// ============================================

// extractTTL extracts TTL entries from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractTTL(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeTTLRecord {
	var ttls []*BronzeTTLRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader for TTL: %v", err)
		return ttls
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change for TTL: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeTtl {
			continue
		}

		var ttlEntry *xdr.TtlEntry
		var lastModified uint32
		var deleted bool

		if change.Post != nil {
			if ttl, ok := change.Post.Data.GetTtl(); ok {
				ttlEntry = &ttl
				lastModified = uint32(change.Post.LastModifiedLedgerSeq)
				deleted = false
			}
		} else if change.Pre != nil {
			if ttl, ok := change.Pre.Data.GetTtl(); ok {
				ttlEntry = &ttl
				lastModified = uint32(change.Pre.LastModifiedLedgerSeq)
				deleted = true
			}
		}

		if ttlEntry == nil {
			continue
		}

		// Key hash
		keyHash := ""
		if keyBytes, err := ttlEntry.KeyHash.MarshalBinary(); err == nil {
			keyHash = hex.EncodeToString(keyBytes)
		}

		liveUntil := int64(ttlEntry.LiveUntilLedgerSeq)
		ttlRemaining := liveUntil - int64(ledgerSeq)
		expired := ttlRemaining <= 0

		record := &BronzeTTLRecord{
			KeyHash:            keyHash,
			LedgerSequence:     ledgerSeq,
			LiveUntilLedgerSeq: liveUntil,
			TTLRemaining:       ttlRemaining,
			Expired:            expired,
			LastModifiedLedger: int32(lastModified),
			Deleted:            deleted,
			ClosedAt:           closedAt,
			CreatedAt:          closedAt,
			LedgerRange:        ledgerRange,
		}

		ttls = append(ttls, record)
	}

	return ttls
}

// ============================================
// Evicted Keys Extraction
// ============================================

// extractEvictedKeys extracts evicted key entries from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractEvictedKeys(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeEvictedKeyRecord {
	var evictedKeys []*BronzeEvictedKeyRecord

	ledgerSeq := p.getLedgerSequence(lcm)

	// Get evicted keys from LCM
	var keys []xdr.LedgerKey
	if lcmV1, ok := lcm.GetV1(); ok {
		keys = lcmV1.EvictedKeys
	} else if lcmV2, ok := lcm.GetV2(); ok {
		keys = lcmV2.EvictedKeys
	}

	for _, key := range keys {
		keyHash := ""
		if keyBytes, err := key.MarshalBinary(); err == nil {
			hash := sha256.Sum256(keyBytes)
			keyHash = hex.EncodeToString(hash[:])
		}

		contractID := ""
		keyType := key.Type.String()
		durability := ""

		// Extract contract ID and durability based on key type
		if key.Type == xdr.LedgerEntryTypeContractData {
			if cd, ok := key.GetContractData(); ok {
				if cd.Contract.Type == xdr.ScAddressTypeScAddressTypeContract {
					if contractHash, ok := cd.Contract.GetContractId(); ok {
						contractID = hex.EncodeToString(contractHash[:])
					}
				}
				durability = cd.Durability.String()
			}
		} else if key.Type == xdr.LedgerEntryTypeContractCode {
			if cc, ok := key.GetContractCode(); ok {
				contractID = hex.EncodeToString(cc.Hash[:])
			}
			durability = "persistent" // Contract code is always persistent
		}

		record := &BronzeEvictedKeyRecord{
			KeyHash:        keyHash,
			LedgerSequence: ledgerSeq,
			ContractID:     contractID,
			KeyType:        keyType,
			Durability:     durability,
			ClosedAt:       closedAt,
			LedgerRange:    ledgerRange,
			CreatedAt:      closedAt,
		}

		evictedKeys = append(evictedKeys, record)
	}

	return evictedKeys
}

// ============================================
// Restored Keys Extraction
// ============================================

// extractRestoredKeys extracts restored key entries from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractRestoredKeys(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeRestoredKeyRecord {
	var restoredKeys []*BronzeRestoredKeyRecord

	// Note: Restored keys tracking requires analyzing RestoreFootprint operations
	// This is a simplified stub - full implementation would parse tx meta

	return restoredKeys
}

// ============================================
// Account Signers Extraction
// ============================================

// extractAccountSigners extracts account signer entries from LedgerCloseMeta
func (p *BronzeLedgerReaderProcessor) extractAccountSigners(lcm *xdr.LedgerCloseMeta, closedAt time.Time, ledgerRange uint32) []*BronzeAccountSignerRecord {
	var signers []*BronzeAccountSignerRecord

	reader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(p.networkPassphrase, *lcm)
	if err != nil {
		log.Printf("BronzeLedgerReader: Failed to create change reader for account signers: %v", err)
		return signers
	}
	defer reader.Close()

	ledgerSeq := p.getLedgerSequence(lcm)

	for {
		change, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("BronzeLedgerReader: Error reading change for account signers: %v", err)
			continue
		}

		if change.Type != xdr.LedgerEntryTypeAccount {
			continue
		}

		var accountEntry *xdr.AccountEntry
		var deleted bool

		if change.Post != nil {
			if ae, ok := change.Post.Data.GetAccount(); ok {
				accountEntry = &ae
				deleted = false
			}
		} else if change.Pre != nil {
			// Account deleted - all signers are deleted
			if ae, ok := change.Pre.Data.GetAccount(); ok {
				accountEntry = &ae
				deleted = true
			}
		}

		if accountEntry == nil {
			continue
		}

		accountID := accountEntry.AccountId.Address()

		// Extract all signers (including master key)
		// Master key
		record := &BronzeAccountSignerRecord{
			AccountID:      accountID,
			Signer:         accountID, // Master key is the account itself
			LedgerSequence: ledgerSeq,
			Weight:         int32(accountEntry.Thresholds[0]), // Master weight
			Deleted:        deleted,
			ClosedAt:       closedAt,
			LedgerRange:    ledgerRange,
			CreatedAt:      closedAt,
		}
		signers = append(signers, record)

		// Additional signers
		for _, signer := range accountEntry.Signers {
			record := &BronzeAccountSignerRecord{
				AccountID:      accountID,
				Signer:         signer.Key.Address(),
				LedgerSequence: ledgerSeq,
				Weight:         int32(signer.Weight),
				Deleted:        deleted,
				ClosedAt:       closedAt,
				LedgerRange:    ledgerRange,
				CreatedAt:      closedAt,
			}

			// Note: Signer sponsors are tracked separately in sponsorship ledger entries
			// The xdr.Signer struct doesn't have a Sponsor field directly

			signers = append(signers, record)
		}
	}

	return signers
}

// ============================================
// Helper Functions
// ============================================

// extractAsset extracts asset information from xdr.Asset
func extractAsset(asset xdr.Asset) (canonical string, assetType string, code *string, issuer *string) {
	assetType = asset.Type.String()
	if asset.Type == xdr.AssetTypeAssetTypeNative {
		return "native", assetType, nil, nil
	}

	switch asset.Type {
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		if alphaNum4, ok := asset.GetAlphaNum4(); ok {
			c := string(alphaNum4.AssetCode[:])
			c = trimNull(c)
			i := alphaNum4.Issuer.Address()
			return fmt.Sprintf("%s:%s", c, i), assetType, &c, &i
		}
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		if alphaNum12, ok := asset.GetAlphaNum12(); ok {
			c := string(alphaNum12.AssetCode[:])
			c = trimNull(c)
			i := alphaNum12.Issuer.Address()
			return fmt.Sprintf("%s:%s", c, i), assetType, &c, &i
		}
	}

	return "", assetType, nil, nil
}

// trimNull removes null characters from a string
func trimNull(s string) string {
	for i, r := range s {
		if r == 0 {
			return s[:i]
		}
	}
	return s
}

// extractTrustLineAsset extracts asset info from TrustLineAsset (handles pool shares)
func extractTrustLineAsset(asset xdr.TrustLineAsset) (assetType string, assetCode string, assetIssuer string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", "", ""
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a4 := asset.MustAlphaNum4()
		code := trimNull(string(a4.AssetCode[:]))
		issuer := a4.Issuer.Address()
		return "credit_alphanum4", code, issuer
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a12 := asset.MustAlphaNum12()
		code := trimNull(string(a12.AssetCode[:]))
		issuer := a12.Issuer.Address()
		return "credit_alphanum12", code, issuer
	case xdr.AssetTypeAssetTypePoolShare:
		// Liquidity pool shares (Protocol 18+)
		// For pool shares, use the liquidity pool ID as the "code"
		if lpID, ok := asset.GetLiquidityPoolId(); ok {
			poolID := hex.EncodeToString(lpID[:])
			return "liquidity_pool_shares", poolID, "pool"
		}
		return "liquidity_pool_shares", "POOL_SHARE", "pool"
	default:
		return "unknown", "", ""
	}
}

// extractChangeTrustAsset extracts asset info from ChangeTrustAsset (handles pool shares)
func extractChangeTrustAsset(asset xdr.ChangeTrustAsset) (assetType string, assetCode string, assetIssuer string) {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native", "", ""
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		a4 := asset.MustAlphaNum4()
		code := trimNull(string(a4.AssetCode[:]))
		issuer := a4.Issuer.Address()
		return "credit_alphanum4", code, issuer
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		a12 := asset.MustAlphaNum12()
		code := trimNull(string(a12.AssetCode[:]))
		issuer := a12.Issuer.Address()
		return "credit_alphanum12", code, issuer
	case xdr.AssetTypeAssetTypePoolShare:
		// Liquidity pool shares (Protocol 18+)
		// For ChangeTrustAsset, pool shares have LiquidityPoolParameters
		if lp, ok := asset.GetLiquidityPool(); ok {
			// Get the constant product parameters
			if cp, ok := lp.GetConstantProduct(); ok {
				// Compute the pool ID from the parameters
				poolID, err := xdr.NewPoolId(cp.AssetA, cp.AssetB, cp.Fee)
				if err == nil {
					return "liquidity_pool_shares", hex.EncodeToString(poolID[:]), "pool"
				}
			}
		}
		return "liquidity_pool_shares", "POOL_SHARE", "pool"
	default:
		return "unknown", "", ""
	}
}
