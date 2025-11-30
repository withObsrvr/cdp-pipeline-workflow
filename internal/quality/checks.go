package quality

import (
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"time"
)

// QualityCheck defines the interface for all data quality checks
// Each check validates a specific aspect of data integrity
type QualityCheck interface {
	// Name returns the unique identifier for this check
	Name() string

	// Type returns the category of check (completeness, consistency, validity, etc.)
	Type() string

	// Run executes the check and returns a result
	Run() QualityCheckResult
}

// QualityCheckResult holds the outcome of a quality check
type QualityCheckResult struct {
	CheckName     string    // Name of the check that was run
	CheckType     string    // Type/category of check
	Passed        bool      // Whether the check passed
	Details       string    // Human-readable details about the result
	RowCount      int       // Number of rows examined
	NullAnomalies int       // Count of null/missing value issues found
	Dataset       string    // Dataset name (e.g., "core.ledgers_row_v2")
	Partition     string    // Partition identifier (optional)
	CreatedAt     time.Time // When the check was performed
}

// ==============================================================================
// LEDGERS TABLE QUALITY CHECKS
// ==============================================================================

// SequenceMonotonicityCheck verifies that ledger sequences are sequential
// with no gaps or duplicates
type SequenceMonotonicityCheck struct {
	ledgers []LedgerData
	dataset string
}

func NewSequenceMonotonicityCheck(ledgers []LedgerData, dataset string) *SequenceMonotonicityCheck {
	return &SequenceMonotonicityCheck{
		ledgers: ledgers,
		dataset: dataset,
	}
}

func (c *SequenceMonotonicityCheck) Name() string {
	return "sequence_monotonicity"
}

func (c *SequenceMonotonicityCheck) Type() string {
	return "consistency"
}

func (c *SequenceMonotonicityCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.ledgers),
	}

	if len(c.ledgers) == 0 {
		result.Passed = true
		result.Details = "No ledgers to check"
		return result
	}

	// Sort ledgers by sequence for checking
	sorted := make([]LedgerData, len(c.ledgers))
	copy(sorted, c.ledgers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Sequence < sorted[j].Sequence
	})

	// Check for gaps and duplicates
	gaps := 0
	duplicates := 0

	for i := 1; i < len(sorted); i++ {
		diff := sorted[i].Sequence - sorted[i-1].Sequence
		if diff == 0 {
			duplicates++
		} else if diff > 1 {
			gaps += int(diff - 1)
		}
	}

	if gaps > 0 || duplicates > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d gaps and %d duplicate sequences", gaps, duplicates)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d sequences are monotonic and contiguous", len(sorted))
	}

	return result
}

// LedgerHashFormatCheck validates that ledger hashes are properly formatted
// (64-character hex strings)
type LedgerHashFormatCheck struct {
	ledgers []LedgerData
	dataset string
}

func NewLedgerHashFormatCheck(ledgers []LedgerData, dataset string) *LedgerHashFormatCheck {
	return &LedgerHashFormatCheck{
		ledgers: ledgers,
		dataset: dataset,
	}
}

func (c *LedgerHashFormatCheck) Name() string {
	return "ledger_hash_format"
}

func (c *LedgerHashFormatCheck) Type() string {
	return "validity"
}

func (c *LedgerHashFormatCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.ledgers),
	}

	if len(c.ledgers) == 0 {
		result.Passed = true
		result.Details = "No ledgers to check"
		return result
	}

	// Stellar hashes are 64-character hex strings
	hashPattern := regexp.MustCompile(`^[a-f0-9]{64}$`)
	invalidHashes := 0

	for _, ledger := range c.ledgers {
		if !hashPattern.MatchString(ledger.LedgerHash) {
			invalidHashes++
		}
		if ledger.PreviousLedgerHash != "" && !hashPattern.MatchString(ledger.PreviousLedgerHash) {
			invalidHashes++
		}
	}

	if invalidHashes > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d invalid hash formats", invalidHashes)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d ledger hashes are properly formatted", len(c.ledgers)*2)
	}

	return result
}

// TransactionCountConsistencyCheck verifies that transaction_count field
// matches the actual number of transactions in the ledger
type TransactionCountConsistencyCheck struct {
	ledgers      []LedgerData
	transactions []TransactionData
	dataset      string
}

func NewTransactionCountConsistencyCheck(ledgers []LedgerData, transactions []TransactionData, dataset string) *TransactionCountConsistencyCheck {
	return &TransactionCountConsistencyCheck{
		ledgers:      ledgers,
		transactions: transactions,
		dataset:      dataset,
	}
}

func (c *TransactionCountConsistencyCheck) Name() string {
	return "transaction_count_consistency"
}

func (c *TransactionCountConsistencyCheck) Type() string {
	return "consistency"
}

func (c *TransactionCountConsistencyCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.ledgers),
	}

	if len(c.ledgers) == 0 {
		result.Passed = true
		result.Details = "No ledgers to check"
		return result
	}

	// Build a map of ledger sequence -> transaction count
	txCountByLedger := make(map[uint32]int)
	for _, tx := range c.transactions {
		txCountByLedger[tx.LedgerSequence]++
	}

	inconsistencies := 0
	for _, ledger := range c.ledgers {
		actualCount := txCountByLedger[ledger.Sequence]
		if int(ledger.TransactionCount) != actualCount {
			inconsistencies++
		}
	}

	if inconsistencies > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d ledgers with mismatched transaction counts", inconsistencies)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d ledgers have consistent transaction counts", len(c.ledgers))
	}

	return result
}

// TimestampOrderingCheck ensures that closed_at timestamps are monotonically increasing
type TimestampOrderingCheck struct {
	ledgers []LedgerData
	dataset string
}

func NewTimestampOrderingCheck(ledgers []LedgerData, dataset string) *TimestampOrderingCheck {
	return &TimestampOrderingCheck{
		ledgers: ledgers,
		dataset: dataset,
	}
}

func (c *TimestampOrderingCheck) Name() string {
	return "timestamp_ordering"
}

func (c *TimestampOrderingCheck) Type() string {
	return "consistency"
}

func (c *TimestampOrderingCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.ledgers),
	}

	if len(c.ledgers) == 0 {
		result.Passed = true
		result.Details = "No ledgers to check"
		return result
	}

	// Sort ledgers by sequence
	sorted := make([]LedgerData, len(c.ledgers))
	copy(sorted, c.ledgers)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Sequence < sorted[j].Sequence
	})

	outOfOrder := 0
	for i := 1; i < len(sorted); i++ {
		prevTime := sorted[i-1].ClosedAt
		currTime := sorted[i].ClosedAt

		if currTime.Before(prevTime) {
			outOfOrder++
		}
	}

	if outOfOrder > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d timestamps out of order", outOfOrder)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d timestamps are properly ordered", len(sorted))
	}

	return result
}

// RequiredFieldsCheck verifies that critical fields are non-null and non-empty
type RequiredFieldsCheck struct {
	ledgers []LedgerData
	dataset string
}

func NewRequiredFieldsCheck(ledgers []LedgerData, dataset string) *RequiredFieldsCheck {
	return &RequiredFieldsCheck{
		ledgers: ledgers,
		dataset: dataset,
	}
}

func (c *RequiredFieldsCheck) Name() string {
	return "required_fields"
}

func (c *RequiredFieldsCheck) Type() string {
	return "completeness"
}

func (c *RequiredFieldsCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.ledgers),
	}

	if len(c.ledgers) == 0 {
		result.Passed = true
		result.Details = "No ledgers to check"
		return result
	}

	nullFields := 0

	for _, ledger := range c.ledgers {
		// Check required fields
		if ledger.LedgerHash == "" {
			nullFields++
		}
		if ledger.ClosedAt.IsZero() {
			nullFields++
		}
		if ledger.Sequence == 0 {
			nullFields++
		}
	}

	result.NullAnomalies = nullFields

	if nullFields > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d missing required fields", nullFields)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d ledgers have required fields populated", len(c.ledgers))
	}

	return result
}

// ==============================================================================
// TRANSACTIONS TABLE QUALITY CHECKS
// ==============================================================================

// TransactionHashFormatCheck validates that transaction hashes are properly formatted
type TransactionHashFormatCheck struct {
	transactions []TransactionData
	dataset      string
}

func NewTransactionHashFormatCheck(transactions []TransactionData, dataset string) *TransactionHashFormatCheck {
	return &TransactionHashFormatCheck{
		transactions: transactions,
		dataset:      dataset,
	}
}

func (c *TransactionHashFormatCheck) Name() string {
	return "transaction_hash_format"
}

func (c *TransactionHashFormatCheck) Type() string {
	return "validity"
}

func (c *TransactionHashFormatCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.transactions),
	}

	if len(c.transactions) == 0 {
		result.Passed = true
		result.Details = "No transactions to check"
		return result
	}

	// Stellar transaction hashes are 64-character hex strings
	hashPattern := regexp.MustCompile(`^[a-f0-9]{64}$`)
	invalidHashes := 0

	for _, tx := range c.transactions {
		if !hashPattern.MatchString(tx.TransactionHash) {
			invalidHashes++
		}
	}

	if invalidHashes > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d invalid transaction hash formats", invalidHashes)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d transaction hashes are properly formatted", len(c.transactions))
	}

	return result
}

// SourceAccountFormatCheck validates that source accounts are valid Stellar addresses
type SourceAccountFormatCheck struct {
	transactions []TransactionData
	dataset      string
}

func NewSourceAccountFormatCheck(transactions []TransactionData, dataset string) *SourceAccountFormatCheck {
	return &SourceAccountFormatCheck{
		transactions: transactions,
		dataset:      dataset,
	}
}

func (c *SourceAccountFormatCheck) Name() string {
	return "source_account_format"
}

func (c *SourceAccountFormatCheck) Type() string {
	return "validity"
}

func (c *SourceAccountFormatCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.transactions),
	}

	if len(c.transactions) == 0 {
		result.Passed = true
		result.Details = "No transactions to check"
		return result
	}

	// Stellar addresses start with 'G' and are 56 characters
	accountPattern := regexp.MustCompile(`^G[A-Z2-7]{55}$`)
	invalidAccounts := 0

	for _, tx := range c.transactions {
		if tx.SourceAccount != "" && !accountPattern.MatchString(tx.SourceAccount) {
			invalidAccounts++
		}
	}

	if invalidAccounts > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d invalid source account formats", invalidAccounts)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d source accounts are properly formatted", len(c.transactions))
	}

	return result
}

// TransactionFeeRangeCheck validates that fees are within reasonable ranges
type TransactionFeeRangeCheck struct {
	transactions []TransactionData
	dataset      string
}

func NewTransactionFeeRangeCheck(transactions []TransactionData, dataset string) *TransactionFeeRangeCheck {
	return &TransactionFeeRangeCheck{
		transactions: transactions,
		dataset:      dataset,
	}
}

func (c *TransactionFeeRangeCheck) Name() string {
	return "transaction_fee_range"
}

func (c *TransactionFeeRangeCheck) Type() string {
	return "validity"
}

func (c *TransactionFeeRangeCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.transactions),
	}

	if len(c.transactions) == 0 {
		result.Passed = true
		result.Details = "No transactions to check"
		return result
	}

	// Stellar base fee is 100 stroops, reasonable max is 100 XLM (1 billion stroops)
	const minFee = 100
	const maxFee = 1000000000
	outOfRange := 0

	for _, tx := range c.transactions {
		if tx.FeeCharged < minFee || tx.FeeCharged > maxFee {
			outOfRange++
		}
	}

	if outOfRange > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d transactions with fees outside valid range [%d, %d]", outOfRange, minFee, maxFee)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d transaction fees are within valid range", len(c.transactions))
	}

	return result
}

// TransactionLedgerConsistencyCheck verifies transactions reference valid ledger sequences
type TransactionLedgerConsistencyCheck struct {
	transactions []TransactionData
	ledgers      []LedgerData
	dataset      string
}

func NewTransactionLedgerConsistencyCheck(transactions []TransactionData, ledgers []LedgerData, dataset string) *TransactionLedgerConsistencyCheck {
	return &TransactionLedgerConsistencyCheck{
		transactions: transactions,
		ledgers:      ledgers,
		dataset:      dataset,
	}
}

func (c *TransactionLedgerConsistencyCheck) Name() string {
	return "transaction_ledger_consistency"
}

func (c *TransactionLedgerConsistencyCheck) Type() string {
	return "consistency"
}

func (c *TransactionLedgerConsistencyCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.transactions),
	}

	if len(c.transactions) == 0 {
		result.Passed = true
		result.Details = "No transactions to check"
		return result
	}

	// Build map of valid ledger sequences
	validLedgers := make(map[uint32]bool)
	for _, ledger := range c.ledgers {
		validLedgers[ledger.Sequence] = true
	}

	invalidRefs := 0
	for _, tx := range c.transactions {
		if !validLedgers[tx.LedgerSequence] {
			invalidRefs++
		}
	}

	if invalidRefs > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d transactions referencing non-existent ledgers", invalidRefs)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d transactions reference valid ledgers", len(c.transactions))
	}

	return result
}

// ==============================================================================
// OPERATIONS TABLE QUALITY CHECKS
// ==============================================================================

// OperationIndexOrderingCheck verifies operation indices are sequential within transactions
type OperationIndexOrderingCheck struct {
	operations []OperationData
	dataset    string
}

func NewOperationIndexOrderingCheck(operations []OperationData, dataset string) *OperationIndexOrderingCheck {
	return &OperationIndexOrderingCheck{
		operations: operations,
		dataset:    dataset,
	}
}

func (c *OperationIndexOrderingCheck) Name() string {
	return "operation_index_ordering"
}

func (c *OperationIndexOrderingCheck) Type() string {
	return "consistency"
}

func (c *OperationIndexOrderingCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.operations),
	}

	if len(c.operations) == 0 {
		result.Passed = true
		result.Details = "No operations to check"
		return result
	}

	// Group operations by transaction hash
	opsByTx := make(map[string][]OperationData)
	for _, op := range c.operations {
		opsByTx[op.TransactionHash] = append(opsByTx[op.TransactionHash], op)
	}

	outOfOrder := 0
	for _, ops := range opsByTx {
		// Sort by index
		sort.Slice(ops, func(i, j int) bool {
			return ops[i].OperationIndex < ops[j].OperationIndex
		})

		// Check for sequential indices starting at 0 or 1
		expectedStart := ops[0].OperationIndex
		for i, op := range ops {
			if op.OperationIndex != expectedStart+int32(i) {
				outOfOrder++
				break
			}
		}
	}

	if outOfOrder > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d transactions with non-sequential operation indices", outOfOrder)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All operations have sequential indices across %d transactions", len(opsByTx))
	}

	return result
}

// OperationTransactionHashCheck verifies all operations have valid transaction hashes
type OperationTransactionHashCheck struct {
	operations []OperationData
	dataset    string
}

func NewOperationTransactionHashCheck(operations []OperationData, dataset string) *OperationTransactionHashCheck {
	return &OperationTransactionHashCheck{
		operations: operations,
		dataset:    dataset,
	}
}

func (c *OperationTransactionHashCheck) Name() string {
	return "operation_transaction_hash"
}

func (c *OperationTransactionHashCheck) Type() string {
	return "validity"
}

func (c *OperationTransactionHashCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.operations),
	}

	if len(c.operations) == 0 {
		result.Passed = true
		result.Details = "No operations to check"
		return result
	}

	// Stellar transaction hashes are 64-character hex strings
	hashPattern := regexp.MustCompile(`^[a-f0-9]{64}$`)
	invalidHashes := 0

	for _, op := range c.operations {
		if !hashPattern.MatchString(op.TransactionHash) {
			invalidHashes++
		}
	}

	if invalidHashes > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d operations with invalid transaction hashes", invalidHashes)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d operations have valid transaction hashes", len(c.operations))
	}

	return result
}

// OperationLedgerConsistencyCheck verifies operations reference valid ledger sequences
type OperationLedgerConsistencyCheck struct {
	operations []OperationData
	ledgers    []LedgerData
	dataset    string
}

func NewOperationLedgerConsistencyCheck(operations []OperationData, ledgers []LedgerData, dataset string) *OperationLedgerConsistencyCheck {
	return &OperationLedgerConsistencyCheck{
		operations: operations,
		ledgers:    ledgers,
		dataset:    dataset,
	}
}

func (c *OperationLedgerConsistencyCheck) Name() string {
	return "operation_ledger_consistency"
}

func (c *OperationLedgerConsistencyCheck) Type() string {
	return "consistency"
}

func (c *OperationLedgerConsistencyCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.operations),
	}

	if len(c.operations) == 0 {
		result.Passed = true
		result.Details = "No operations to check"
		return result
	}

	// Build map of valid ledger sequences
	validLedgers := make(map[uint32]bool)
	for _, ledger := range c.ledgers {
		validLedgers[ledger.Sequence] = true
	}

	invalidRefs := 0
	for _, op := range c.operations {
		if !validLedgers[op.LedgerSequence] {
			invalidRefs++
		}
	}

	if invalidRefs > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d operations referencing non-existent ledgers", invalidRefs)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d operations reference valid ledgers", len(c.operations))
	}

	return result
}

// OperationRequiredFieldsCheck verifies critical fields are non-null
type OperationRequiredFieldsCheck struct {
	operations []OperationData
	dataset    string
}

func NewOperationRequiredFieldsCheck(operations []OperationData, dataset string) *OperationRequiredFieldsCheck {
	return &OperationRequiredFieldsCheck{
		operations: operations,
		dataset:    dataset,
	}
}

func (c *OperationRequiredFieldsCheck) Name() string {
	return "operation_required_fields"
}

func (c *OperationRequiredFieldsCheck) Type() string {
	return "completeness"
}

func (c *OperationRequiredFieldsCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.operations),
	}

	if len(c.operations) == 0 {
		result.Passed = true
		result.Details = "No operations to check"
		return result
	}

	nullFields := 0

	for _, op := range c.operations {
		if op.TransactionHash == "" {
			nullFields++
		}
		if op.LedgerSequence == 0 {
			nullFields++
		}
	}

	result.NullAnomalies = nullFields

	if nullFields > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d missing required fields", nullFields)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d operations have required fields populated", len(c.operations))
	}

	return result
}

// ==============================================================================
// BALANCES TABLE QUALITY CHECKS
// ==============================================================================

// BalanceAccountIDFormatCheck validates that account IDs are valid Stellar addresses
type BalanceAccountIDFormatCheck struct {
	balances []BalanceData
	dataset  string
}

func NewBalanceAccountIDFormatCheck(balances []BalanceData, dataset string) *BalanceAccountIDFormatCheck {
	return &BalanceAccountIDFormatCheck{
		balances: balances,
		dataset:  dataset,
	}
}

func (c *BalanceAccountIDFormatCheck) Name() string {
	return "balance_account_id_format"
}

func (c *BalanceAccountIDFormatCheck) Type() string {
	return "validity"
}

func (c *BalanceAccountIDFormatCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.balances),
	}

	if len(c.balances) == 0 {
		result.Passed = true
		result.Details = "No balances to check"
		return result
	}

	// Stellar addresses start with 'G' and are 56 characters
	accountPattern := regexp.MustCompile(`^G[A-Z2-7]{55}$`)
	invalidAccounts := 0

	for _, bal := range c.balances {
		if bal.AccountID != "" && !accountPattern.MatchString(bal.AccountID) {
			invalidAccounts++
		}
	}

	if invalidAccounts > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d invalid account ID formats", invalidAccounts)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d account IDs are properly formatted", len(c.balances))
	}

	return result
}

// BalanceRangeCheck validates that balances are non-negative and within reasonable ranges
type BalanceRangeCheck struct {
	balances []BalanceData
	dataset  string
}

func NewBalanceRangeCheck(balances []BalanceData, dataset string) *BalanceRangeCheck {
	return &BalanceRangeCheck{
		balances: balances,
		dataset:  dataset,
	}
}

func (c *BalanceRangeCheck) Name() string {
	return "balance_range"
}

func (c *BalanceRangeCheck) Type() string {
	return "validity"
}

func (c *BalanceRangeCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.balances),
	}

	if len(c.balances) == 0 {
		result.Passed = true
		result.Details = "No balances to check"
		return result
	}

	// Balance must be non-negative, max reasonable is 100 billion XLM (10^18 stroops ~ total supply)
	const minBalance int64 = 0
	const maxBalance int64 = 1000000000000000000 // 10^18 stroops
	outOfRange := 0

	for _, bal := range c.balances {
		if bal.Balance < minBalance || bal.Balance > maxBalance {
			outOfRange++
		}
	}

	if outOfRange > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d balances outside valid range [%d, %d]", outOfRange, minBalance, maxBalance)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d balances are within valid range", len(c.balances))
	}

	return result
}

// LiabilitiesValidationCheck validates that liabilities have reasonable relationship to balance
type LiabilitiesValidationCheck struct {
	balances []BalanceData
	dataset  string
}

func NewLiabilitiesValidationCheck(balances []BalanceData, dataset string) *LiabilitiesValidationCheck {
	return &LiabilitiesValidationCheck{
		balances: balances,
		dataset:  dataset,
	}
}

func (c *LiabilitiesValidationCheck) Name() string {
	return "liabilities_validation"
}

func (c *LiabilitiesValidationCheck) Type() string {
	return "consistency"
}

func (c *LiabilitiesValidationCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.balances),
	}

	if len(c.balances) == 0 {
		result.Passed = true
		result.Details = "No balances to check"
		return result
	}

	invalidLiabilities := 0

	for _, bal := range c.balances {
		// Liabilities must be non-negative
		if bal.BuyingLiabilities < 0 || bal.SellingLiabilities < 0 {
			invalidLiabilities++
			continue
		}

		// Total liabilities should not exceed balance (with some tolerance for edge cases)
		totalLiabilities := bal.BuyingLiabilities + bal.SellingLiabilities
		if totalLiabilities > bal.Balance*2 { // Allow 2x for edge cases with reserves
			invalidLiabilities++
		}
	}

	if invalidLiabilities > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d accounts with invalid liabilities", invalidLiabilities)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d accounts have valid liabilities", len(c.balances))
	}

	return result
}

// BalanceLedgerConsistencyCheck verifies balances reference valid ledger sequences
type BalanceLedgerConsistencyCheck struct {
	balances []BalanceData
	ledgers  []LedgerData
	dataset  string
}

func NewBalanceLedgerConsistencyCheck(balances []BalanceData, ledgers []LedgerData, dataset string) *BalanceLedgerConsistencyCheck {
	return &BalanceLedgerConsistencyCheck{
		balances: balances,
		ledgers:  ledgers,
		dataset:  dataset,
	}
}

func (c *BalanceLedgerConsistencyCheck) Name() string {
	return "balance_ledger_consistency"
}

func (c *BalanceLedgerConsistencyCheck) Type() string {
	return "consistency"
}

func (c *BalanceLedgerConsistencyCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.balances),
	}

	if len(c.balances) == 0 {
		result.Passed = true
		result.Details = "No balances to check"
		return result
	}

	// Build map of valid ledger sequences
	validLedgers := make(map[uint32]bool)
	for _, ledger := range c.ledgers {
		validLedgers[ledger.Sequence] = true
	}

	invalidRefs := 0
	for _, bal := range c.balances {
		if !validLedgers[bal.LedgerSequence] {
			invalidRefs++
		}
	}

	if invalidRefs > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d balances referencing non-existent ledgers", invalidRefs)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d balances reference valid ledgers", len(c.balances))
	}

	return result
}

// BalanceRequiredFieldsCheck verifies critical fields are non-null
type BalanceRequiredFieldsCheck struct {
	balances []BalanceData
	dataset  string
}

func NewBalanceRequiredFieldsCheck(balances []BalanceData, dataset string) *BalanceRequiredFieldsCheck {
	return &BalanceRequiredFieldsCheck{
		balances: balances,
		dataset:  dataset,
	}
}

func (c *BalanceRequiredFieldsCheck) Name() string {
	return "balance_required_fields"
}

func (c *BalanceRequiredFieldsCheck) Type() string {
	return "completeness"
}

func (c *BalanceRequiredFieldsCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.balances),
	}

	if len(c.balances) == 0 {
		result.Passed = true
		result.Details = "No balances to check"
		return result
	}

	nullFields := 0

	for _, bal := range c.balances {
		if bal.AccountID == "" {
			nullFields++
		}
		if bal.LedgerSequence == 0 {
			nullFields++
		}
	}

	result.NullAnomalies = nullFields

	if nullFields > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d missing required fields", nullFields)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d balances have required fields populated", len(c.balances))
	}

	return result
}

// SequenceNumberValidityCheck validates that sequence numbers are non-negative
type SequenceNumberValidityCheck struct {
	balances []BalanceData
	dataset  string
}

func NewSequenceNumberValidityCheck(balances []BalanceData, dataset string) *SequenceNumberValidityCheck {
	return &SequenceNumberValidityCheck{
		balances: balances,
		dataset:  dataset,
	}
}

func (c *SequenceNumberValidityCheck) Name() string {
	return "sequence_number_validity"
}

func (c *SequenceNumberValidityCheck) Type() string {
	return "validity"
}

func (c *SequenceNumberValidityCheck) Run() QualityCheckResult {
	result := QualityCheckResult{
		CheckName: c.Name(),
		CheckType: c.Type(),
		Dataset:   c.dataset,
		CreatedAt: time.Now(),
		RowCount:  len(c.balances),
	}

	if len(c.balances) == 0 {
		result.Passed = true
		result.Details = "No balances to check"
		return result
	}

	invalidSeq := 0

	for _, bal := range c.balances {
		if bal.SequenceNumber < 0 {
			invalidSeq++
		}
	}

	if invalidSeq > 0 {
		result.Passed = false
		result.Details = fmt.Sprintf("Found %d negative sequence numbers", invalidSeq)
	} else {
		result.Passed = true
		result.Details = fmt.Sprintf("All %d sequence numbers are valid", len(c.balances))
	}

	return result
}

// ==============================================================================
// QUALITY CHECK RUNNER
// ==============================================================================

// RunLedgerQualityChecks executes all ledger quality checks and returns results
func (ing *Ingester) RunLedgerQualityChecks() []QualityCheckResult {
	dataset := "core.ledgers_row_v2"
	results := []QualityCheckResult{}

	// Run all 5 ledger checks
	checks := []QualityCheck{
		NewSequenceMonotonicityCheck(ing.buffers.ledgers, dataset),
		NewLedgerHashFormatCheck(ing.buffers.ledgers, dataset),
		NewTransactionCountConsistencyCheck(ing.buffers.ledgers, ing.buffers.transactions, dataset),
		NewTimestampOrderingCheck(ing.buffers.ledgers, dataset),
		NewRequiredFieldsCheck(ing.buffers.ledgers, dataset),
	}

	for _, check := range checks {
		result := check.Run()
		results = append(results, result)
	}

	return results
}

// RunTransactionQualityChecks executes all transaction quality checks and returns results
func (ing *Ingester) RunTransactionQualityChecks() []QualityCheckResult {
	dataset := "core.transactions_row_v1"
	results := []QualityCheckResult{}

	// Run all 4 transaction checks
	checks := []QualityCheck{
		NewTransactionHashFormatCheck(ing.buffers.transactions, dataset),
		NewSourceAccountFormatCheck(ing.buffers.transactions, dataset),
		NewTransactionFeeRangeCheck(ing.buffers.transactions, dataset),
		NewTransactionLedgerConsistencyCheck(ing.buffers.transactions, ing.buffers.ledgers, dataset),
	}

	for _, check := range checks {
		result := check.Run()
		results = append(results, result)
	}

	return results
}

// RunOperationQualityChecks executes all operation quality checks and returns results
func (ing *Ingester) RunOperationQualityChecks() []QualityCheckResult {
	dataset := "core.operations_row_v2"
	results := []QualityCheckResult{}

	// Run all 5 operation checks
	checks := []QualityCheck{
		NewOperationIndexOrderingCheck(ing.buffers.operations, dataset),
		NewOperationTransactionHashCheck(ing.buffers.operations, dataset),
		NewOperationLedgerConsistencyCheck(ing.buffers.operations, ing.buffers.ledgers, dataset),
		NewOperationRequiredFieldsCheck(ing.buffers.operations, dataset),
	}

	for _, check := range checks {
		result := check.Run()
		results = append(results, result)
	}

	return results
}

// RunBalanceQualityChecks executes all balance quality checks and returns results
func (ing *Ingester) RunBalanceQualityChecks() []QualityCheckResult {
	dataset := "core.native_balances_snapshot_v1"
	results := []QualityCheckResult{}

	// Run all 6 balance checks
	checks := []QualityCheck{
		NewBalanceAccountIDFormatCheck(ing.buffers.balances, dataset),
		NewBalanceRangeCheck(ing.buffers.balances, dataset),
		NewLiabilitiesValidationCheck(ing.buffers.balances, dataset),
		NewBalanceLedgerConsistencyCheck(ing.buffers.balances, ing.buffers.ledgers, dataset),
		NewBalanceRequiredFieldsCheck(ing.buffers.balances, dataset),
		NewSequenceNumberValidityCheck(ing.buffers.balances, dataset),
	}

	for _, check := range checks {
		result := check.Run()
		results = append(results, result)
	}

	return results
}

// RunAllQualityChecks executes all quality checks across all tables
func (ing *Ingester) RunAllQualityChecks() []QualityCheckResult {
	results := []QualityCheckResult{}

	// Run ledger checks
	results = append(results, ing.RunLedgerQualityChecks()...)

	// Run transaction checks (if we have transactions)
	if len(ing.buffers.transactions) > 0 {
		results = append(results, ing.RunTransactionQualityChecks()...)
	}

	// Run operation checks (if we have operations)
	if len(ing.buffers.operations) > 0 {
		results = append(results, ing.RunOperationQualityChecks()...)
	}

	// Run balance checks (if we have balances)
	if len(ing.buffers.balances) > 0 {
		results = append(results, ing.RunBalanceQualityChecks()...)
	}

	return results
}

// ==============================================================================
// QUALITY CHECK RECORDING
// ==============================================================================

// recordQualityChecks writes quality check results to the _meta_quality table
func (ing *Ingester) recordQualityChecks(results []QualityCheckResult) error {
	if len(results) == 0 {
		return nil
	}

	// Generate unique IDs for each result (timestamp-based)
	baseID := time.Now().UnixNano()

	// OPTIMIZED: Batched insert with single query instead of 19 individual inserts
	// This reduces network roundtrips from 19 to 1 (critical for remote PostgreSQL)
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s._meta_quality (
			id, dataset, partition, check_name, check_type,
			passed, details, row_count, null_anomalies, created_at
		) VALUES `,
		ing.config.DuckLake.CatalogName,
		ing.config.DuckLake.SchemaName,
	)

	// Build multi-row VALUES clause
	valuePlaceholders := make([]string, len(results))
	args := make([]interface{}, 0, len(results)*10) // 10 fields per result

	passed := 0
	failed := 0

	for i, result := range results {
		id := baseID + int64(i)
		valuePlaceholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

		partition := result.Partition
		if partition == "" {
			partition = "" // NULL partition for non-partitioned data
		}

		args = append(args,
			id,
			result.Dataset,
			partition,
			result.CheckName,
			result.CheckType,
			result.Passed,
			result.Details,
			result.RowCount,
			result.NullAnomalies,
			result.CreatedAt,
		)

		if result.Passed {
			passed++
		} else {
			failed++
		}
	}

	// Execute single batched insert
	insertSQL += strings.Join(valuePlaceholders, ",")
	_, err := ing.db.Exec(insertSQL, args...)
	if err != nil {
		return fmt.Errorf("failed to batch record %d quality checks: %w", len(results), err)
	}

	// Log summary
	if failed > 0 {
		log.Printf("⚠️  Quality Checks: %d passed, %d FAILED", passed, failed)
	} else {
		log.Printf("✅ Quality Checks: All %d checks passed", passed)
	}

	return nil
}
