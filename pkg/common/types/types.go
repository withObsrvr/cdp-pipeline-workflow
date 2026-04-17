package types

import (
	"context"
	"encoding/hex"
	"math/big"
	"strconv"
	"time"

	"github.com/stellar/go/xdr"
)

// Message encapsulates the payload to be processed.
type Message struct {
	Payload interface{}
}

// ProcessorConfig defines configuration for a processor
type ProcessorConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

// ConsumerConfig defines configuration for a consumer
type ConsumerConfig struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config"`
}

// Processor defines the interface for processing messages.
type Processor interface {
	Process(context.Context, Message) error
	Subscribe(Processor)
}

// Consumer defines the interface for consuming messages.
type Consumer interface {
	Process(context.Context, Message) error
	Subscribe(Processor)
}

// AssetDetails contains information about an asset
type AssetDetails struct {
	Code      string    `json:"code"`
	Issuer    string    `json:"issuer,omitempty"` // omitempty since native assets have no issuer
	Type      string    `json:"type"`             // native, credit_alphanum4, credit_alphanum12
	Timestamp time.Time `json:"timestamp"`
}

// AssetInfo contains detailed information about an asset
type AssetInfo struct {
	AssetDetails
	Stats      AssetStats `json:"stats"`
	HomeDomain string     `json:"home_domain,omitempty"`
	Anchor     struct {
		Name string `json:"name,omitempty"`
		URL  string `json:"url,omitempty"`
	} `json:"anchor,omitempty"`
	Verified    bool   `json:"verified"`
	DisplayName string `json:"display_name,omitempty"`
	Description string `json:"description,omitempty"`
}

// Helper functions for parsing data
func ParseTimestamp(ts string) (time.Time, error) {
	i, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(i, 0), nil
}

// AssetStats represents statistical information about an asset
type AssetStats struct {
	NumAccounts    int64      `json:"num_accounts"`
	Amount         string     `json:"amount"`
	Payments       int64      `json:"payments"`
	PaymentVolume  string     `json:"payment_volume"`
	Trades         int64      `json:"trades"`
	TradeVolume    string     `json:"trade_volume"`
	LastLedgerTime time.Time  `json:"last_ledger_time"`
	LastLedger     uint32     `json:"last_ledger"`
	Change24h      float64    `json:"change_24h,omitempty"`
	Volume24h      float64    `json:"volume_24h,omitempty"`
	TradeCount24h  int        `json:"trade_count_24h,omitempty"`
	SupplyMetrics  SupplyData `json:"supply_metrics,omitempty"`
}

// SupplyData represents supply information about an asset
type SupplyData struct {
	Issued         string  `json:"issued"`
	Max            string  `json:"max,omitempty"`
	Circulating    string  `json:"circulating,omitempty"`
	Locked         string  `json:"locked,omitempty"`
	CircPercent    float64 `json:"circ_percent,omitempty"`
	StakedPercent  float64 `json:"staked_percent,omitempty"`
	ActiveAccounts int64   `json:"active_accounts,omitempty"`
}

// Price represents the price of an asset as a fraction
type Price struct {
	Numerator   int32 `json:"n"`
	Denominator int32 `json:"d"`
}

// Path is a representation of an asset without an ID
type Path struct {
	AssetCode   string `json:"asset_code"`
	AssetIssuer string `json:"asset_issuer"`
	AssetType   string `json:"asset_type"`
}

// Asset represents a Stellar asset
type Asset struct {
	Code   string `json:"code"`
	Issuer string `json:"issuer,omitempty"`
	Type   string `json:"type"` // native, credit_alphanum4, credit_alphanum12
}

// ConvertStroopValueToReal converts stroop values to real XLM values
func ConvertStroopValueToReal(input xdr.Int64) (float64, error) {
	rat := big.NewRat(int64(input), int64(10000000))
	output, _ := rat.Float64()
	return output, nil
}

// HashToHexString converts an XDR Hash to a hex string
func HashToHexString(inputHash xdr.Hash) string {
	return hex.EncodeToString(inputHash[:])
}
