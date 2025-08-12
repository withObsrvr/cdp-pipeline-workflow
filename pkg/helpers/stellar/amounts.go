package stellar

import (
	"fmt"
	"math"
	"strconv"

	"github.com/stellar/go/amount"
)

// FormatAmount converts a raw int64 amount to a decimal string
// Stellar amounts are stored as int64 with 7 decimal places
func FormatAmount(raw int64) string {
	return amount.String(raw)
}

// ParseAmount converts a decimal string to raw int64
func ParseAmount(s string) (int64, error) {
	return amount.ParseInt64(s)
}

// FormatAmountWithPrecision formats amount with specific decimal places
func FormatAmountWithPrecision(raw int64, decimals int) string {
	if decimals > 7 {
		decimals = 7
	}
	
	formatted := amount.String(raw)
	
	// Find decimal point
	dotIndex := -1
	for i, c := range formatted {
		if c == '.' {
			dotIndex = i
			break
		}
	}
	
	if dotIndex == -1 {
		return formatted
	}
	
	// Trim to requested decimals
	endIndex := dotIndex + decimals + 1
	if endIndex > len(formatted) {
		endIndex = len(formatted)
	}
	
	return formatted[:endIndex]
}

// AddAmounts safely adds two amounts, checking for overflow
func AddAmounts(a, b int64) (int64, error) {
	if a > 0 && b > math.MaxInt64-a {
		return 0, fmt.Errorf("amount overflow: %d + %d", a, b)
	}
	if a < 0 && b < math.MinInt64-a {
		return 0, fmt.Errorf("amount underflow: %d + %d", a, b)
	}
	return a + b, nil
}

// SubtractAmounts safely subtracts two amounts
func SubtractAmounts(a, b int64) (int64, error) {
	if b > 0 && a < math.MinInt64+b {
		return 0, fmt.Errorf("amount underflow: %d - %d", a, b)
	}
	if b < 0 && a > math.MaxInt64+b {
		return 0, fmt.Errorf("amount overflow: %d - %d", a, b)
	}
	return a - b, nil
}

// MultiplyAmount multiplies an amount by a factor
func MultiplyAmount(amount int64, factor float64) (int64, error) {
	result := float64(amount) * factor
	
	if result > math.MaxInt64 || result < math.MinInt64 {
		return 0, fmt.Errorf("multiplication overflow: %d * %f", amount, factor)
	}
	
	return int64(result), nil
}

// IsValidAmount checks if an amount is valid (non-negative)
func IsValidAmount(amount int64) bool {
	return amount >= 0
}

// CompareAmounts compares two amounts
// Returns -1 if a < b, 0 if a == b, 1 if a > b
func CompareAmounts(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// ParseStringAmount parses various amount formats
func ParseStringAmount(s string) (int64, error) {
	// Try standard parsing first
	amt, err := amount.ParseInt64(s)
	if err == nil {
		return amt, nil
	}
	
	// Try parsing as float and converting
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount format: %s", s)
	}
	
	// Convert to stroops (multiply by 10^7)
	stroops := f * 10000000
	
	// Check bounds
	if stroops > math.MaxInt64 || stroops < 0 {
		return 0, fmt.Errorf("amount out of range: %s", s)
	}
	
	return int64(stroops), nil
}

// AmountToFloat converts amount to float64 (useful for calculations)
func AmountToFloat(raw int64) float64 {
	return float64(raw) / 10000000.0
}

// FloatToAmount converts float64 to amount (with rounding)
func FloatToAmount(f float64) int64 {
	return int64(math.Round(f * 10000000))
}