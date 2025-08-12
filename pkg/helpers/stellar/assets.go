package stellar

import (
	"fmt"
	"strings"

	"github.com/stellar/go/xdr"
)

// AssetToString converts an XDR asset to its string representation
func AssetToString(asset xdr.Asset) string {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "native"
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		code := strings.TrimRight(string(asset.AlphaNum4.AssetCode[:]), "\x00")
		issuer := asset.AlphaNum4.Issuer.Address()
		return fmt.Sprintf("%s:%s", code, issuer)
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		code := strings.TrimRight(string(asset.AlphaNum12.AssetCode[:]), "\x00")
		issuer := asset.AlphaNum12.Issuer.Address()
		return fmt.Sprintf("%s:%s", code, issuer)
	default:
		return "unknown"
	}
}

// CreateAsset creates an asset based on code and issuer
func CreateAsset(code, issuer string) (xdr.Asset, error) {
	if issuer == "" {
		// Native asset (XLM)
		return xdr.NewNativeAsset()
	}
	
	// Determine asset type based on code length
	if len(code) <= 4 {
		return xdr.NewCreditAsset(code, issuer)
	} else if len(code) <= 12 {
		return xdr.NewCreditAsset(code, issuer)
	}
	
	return xdr.Asset{}, fmt.Errorf("invalid asset code length: %d", len(code))
}

// IsNativeAsset checks if an asset is native (XLM)
func IsNativeAsset(asset xdr.Asset) bool {
	return asset.Type == xdr.AssetTypeAssetTypeNative
}

// GetAssetCode returns the asset code, handling all asset types
func GetAssetCode(asset xdr.Asset) string {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return "XLM"
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		return strings.TrimRight(string(asset.AlphaNum4.AssetCode[:]), "\x00")
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		return strings.TrimRight(string(asset.AlphaNum12.AssetCode[:]), "\x00")
	default:
		return ""
	}
}

// GetAssetIssuer returns the asset issuer address, or empty string for native
func GetAssetIssuer(asset xdr.Asset) string {
	switch asset.Type {
	case xdr.AssetTypeAssetTypeNative:
		return ""
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		return asset.AlphaNum4.Issuer.Address()
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		return asset.AlphaNum12.Issuer.Address()
	default:
		return ""
	}
}

// CompareAssets checks if two assets are equal
func CompareAssets(a, b xdr.Asset) bool {
	if a.Type != b.Type {
		return false
	}
	
	switch a.Type {
	case xdr.AssetTypeAssetTypeNative:
		return true
	case xdr.AssetTypeAssetTypeCreditAlphanum4:
		return a.AlphaNum4.AssetCode == b.AlphaNum4.AssetCode &&
			a.AlphaNum4.Issuer == b.AlphaNum4.Issuer
	case xdr.AssetTypeAssetTypeCreditAlphanum12:
		return a.AlphaNum12.AssetCode == b.AlphaNum12.AssetCode &&
			a.AlphaNum12.Issuer == b.AlphaNum12.Issuer
	default:
		return false
	}
}