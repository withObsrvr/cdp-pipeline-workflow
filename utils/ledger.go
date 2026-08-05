package utils

import (
	"errors"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// LedgerSequence returns the sequence number of a ledger
func LedgerSequence(lcm xdr.LedgerCloseMeta) (*uint32, error) {
	ledgerSequence := lcm.LedgerSequence()
	return &ledgerSequence, nil
}

func CloseTime(lcm xdr.LedgerCloseMeta) (*time.Time, error) {
	ledgerHeader := lcm.LedgerHeaderHistoryEntry()
	closeTime, err := TimePointToUTCTimeStamp(ledgerHeader.Header.ScpValue.CloseTime)
	if err != nil {
		return nil, err
	}

	return &closeTime, nil
}

func BaseFee(lcm xdr.LedgerCloseMeta) (*uint32, error) {
	ledgerHeader := lcm.LedgerHeaderHistoryEntry()
	baseFee := uint32(ledgerHeader.Header.BaseFee)
	return &baseFee, nil
}

func BaseReserve(lcm xdr.LedgerCloseMeta) (*uint32, error) {
	ledgerHeader := lcm.LedgerHeaderHistoryEntry()
	baseReserve := uint32(ledgerHeader.Header.BaseReserve)
	return &baseReserve, nil
}

// SorobanFeeWrite1Kb returns the Soroban write fee per 1KB, or nil when the
// ledger carries no Soroban extension (pre-Protocol 20). The extension lives on
// both LedgerCloseMeta V1 and V2. Unrecognised versions return an error rather
// than panicking, so a future protocol bump degrades instead of crashing.
func SorobanFeeWrite1Kb(lcm xdr.LedgerCloseMeta) (*int64, error) {
	var ext xdr.LedgerCloseMetaExt
	switch {
	case lcm.V == 0:
		return nil, nil
	default:
		if lcmV1, ok := lcm.GetV1(); ok {
			ext = lcmV1.Ext
		} else if lcmV2, ok := lcm.GetV2(); ok {
			ext = lcmV2.Ext
		} else {
			return nil, fmt.Errorf("unsupported LedgerCloseMeta.V: %d", lcm.V)
		}
	}

	extV1, ok := ext.GetV1()
	if !ok {
		return nil, nil
	}
	sorobanFeeWrite1Kb := int64(extV1.SorobanFeeWrite1Kb)
	return &sorobanFeeWrite1Kb, nil
}

// TotalByteSizeOfBucketList returns the total byte size of the live Soroban
// state, or nil for V0 ledgers which do not track it.
func TotalByteSizeOfBucketList(lcm xdr.LedgerCloseMeta) (*uint64, error) {
	switch {
	case lcm.V == 0:
		return nil, nil
	default:
		var total uint64
		if lcmV1, ok := lcm.GetV1(); ok {
			total = uint64(lcmV1.TotalByteSizeOfLiveSorobanState)
		} else if lcmV2, ok := lcm.GetV2(); ok {
			total = uint64(lcmV2.TotalByteSizeOfLiveSorobanState)
		} else {
			return nil, fmt.Errorf("unsupported LedgerCloseMeta.V: %d", lcm.V)
		}
		return &total, nil
	}
}

// TimePointToUTCTimeStamp takes in an xdr TimePoint and converts it to a time.Time struct in UTC. It returns an error for negative timepoints
func TimePointToUTCTimeStamp(providedTime xdr.TimePoint) (time.Time, error) {
	intTime := int64(providedTime)
	if intTime < 0 {
		return time.Time{}, errors.New("the timepoint is negative")
	}
	return time.Unix(intTime, 0).UTC(), nil
}
