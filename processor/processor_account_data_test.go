package processor

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAccountAddress = "GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7"
	testSponsorAddress = "GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"
)

func testAccountID(t *testing.T, address string) xdr.AccountId {
	t.Helper()
	var accountID xdr.AccountId
	require.NoError(t, accountID.SetAddress(address))
	return accountID
}

// testAccountEntry builds a minimal but valid account ledger entry.
func testAccountEntry(t *testing.T, address string, balance xdr.Int64, seqNum xdr.SequenceNumber) *xdr.LedgerEntry {
	t.Helper()
	return &xdr.LedgerEntry{
		LastModifiedLedgerSeq: 4242,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId:     testAccountID(t, address),
				Balance:       balance,
				SeqNum:        seqNum,
				NumSubEntries: 3,
				Flags:         xdr.Uint32(xdr.AccountFlagsAuthRequiredFlag),
				HomeDomain:    "example.com",
				Thresholds:    xdr.Thresholds{1, 2, 3, 4},
			},
		},
	}
}

// testLedgerCloseMeta builds a V1 LedgerCloseMeta carrying just enough header
// for processAccountChange to read the sequence and close time.
func testLedgerCloseMeta(sequence uint32, closeTime int64) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(sequence),
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(closeTime),
					},
				},
			},
		},
	}
}

// TestProcessAccountChange_ChangeTypes covers the Pre/Post normalisation that
// replaced the raw xdr.LedgerEntryChange union handling.
func TestProcessAccountChange_ChangeTypes(t *testing.T) {
	meta := testLedgerCloseMeta(1000, 1700000000)

	t.Run("created maps to change type 1", func(t *testing.T) {
		change := ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  nil,
			Post: testAccountEntry(t, testAccountAddress, 5000000, 7),
		}

		record, err := processAccountChange(change, meta)

		require.NoError(t, err)
		assert.Equal(t, testAccountAddress, record.AccountID)
		assert.Equal(t, uint32(1), record.LedgerEntryChange)
		assert.False(t, record.Deleted)
		assert.Equal(t, "5000000", record.Balance)
		assert.Equal(t, uint64(7), record.Sequence)
		assert.Equal(t, uint32(1000), record.LedgerSequence)
	})

	t.Run("updated maps to change type 2 and reads the post state", func(t *testing.T) {
		change := ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  testAccountEntry(t, testAccountAddress, 1, 1),
			Post: testAccountEntry(t, testAccountAddress, 9000000, 12),
		}

		record, err := processAccountChange(change, meta)

		require.NoError(t, err)
		assert.Equal(t, uint32(2), record.LedgerEntryChange)
		assert.False(t, record.Deleted)
		// The post state wins: an update must not report the pre-image values.
		assert.Equal(t, "9000000", record.Balance)
		assert.Equal(t, uint64(12), record.Sequence)
	})

	t.Run("removed yields a minimal deleted record from the pre state", func(t *testing.T) {
		change := ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Pre:  testAccountEntry(t, testAccountAddress, 42, 3),
			Post: nil,
		}

		record, err := processAccountChange(change, meta)

		require.NoError(t, err)
		assert.Equal(t, testAccountAddress, record.AccountID)
		assert.True(t, record.Deleted)
		assert.Equal(t, uint32(1000), record.LedgerSequence)
		// Removals intentionally carry only identity plus deletion status.
		assert.Empty(t, record.Balance)
	})

	t.Run("change with neither pre nor post is rejected", func(t *testing.T) {
		change := ingest.Change{Type: xdr.LedgerEntryTypeAccount}

		_, err := processAccountChange(change, meta)

		require.Error(t, err)
	})

	t.Run("non-account entry is rejected", func(t *testing.T) {
		change := ingest.Change{
			Type: xdr.LedgerEntryTypeTrustline,
			Post: &xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type:      xdr.LedgerEntryTypeTrustline,
					TrustLine: &xdr.TrustLineEntry{},
				},
			},
		}

		_, err := processAccountChange(change, meta)

		require.Error(t, err)
	})

	t.Run("non-account removal is rejected", func(t *testing.T) {
		change := ingest.Change{
			Type: xdr.LedgerEntryTypeTrustline,
			Pre: &xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type:      xdr.LedgerEntryTypeTrustline,
					TrustLine: &xdr.TrustLineEntry{},
				},
			},
		}

		_, err := processAccountChange(change, meta)

		require.Error(t, err)
	})
}

// TestProcessAccountChange_Sponsor covers the sponsor extension read, which
// dereferences Ext.V1 and must tolerate its absence.
func TestProcessAccountChange_Sponsor(t *testing.T) {
	meta := testLedgerCloseMeta(1000, 1700000000)

	t.Run("no extension leaves sponsor empty", func(t *testing.T) {
		record, err := processAccountChange(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Post: testAccountEntry(t, testAccountAddress, 1, 1),
		}, meta)

		require.NoError(t, err)
		assert.Empty(t, record.Sponsor)
	})

	t.Run("V1 extension without sponsor leaves sponsor empty", func(t *testing.T) {
		entry := testAccountEntry(t, testAccountAddress, 1, 1)
		entry.Ext = xdr.LedgerEntryExt{
			V:  1,
			V1: &xdr.LedgerEntryExtensionV1{},
		}

		record, err := processAccountChange(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Post: entry,
		}, meta)

		require.NoError(t, err)
		assert.Empty(t, record.Sponsor)
	})

	t.Run("sponsored entry reports the sponsor address", func(t *testing.T) {
		sponsor := testAccountID(t, testSponsorAddress)
		entry := testAccountEntry(t, testAccountAddress, 1, 1)
		entry.Ext = xdr.LedgerEntryExt{
			V:  1,
			V1: &xdr.LedgerEntryExtensionV1{SponsoringId: &sponsor},
		}

		record, err := processAccountChange(ingest.Change{
			Type: xdr.LedgerEntryTypeAccount,
			Post: entry,
		}, meta)

		require.NoError(t, err)
		assert.Equal(t, testSponsorAddress, record.Sponsor)
	})
}
