package stellar

import (
	"fmt"
)

// ProcessingError provides rich context for errors during processing
type ProcessingError struct {
	Processor   string
	Ledger      uint32
	Transaction string
	Operation   int
	Err         error
}

func (e ProcessingError) Error() string {
	if e.Operation >= 0 {
		return fmt.Sprintf("%s: error at ledger %d, tx %s, op %d: %v",
			e.Processor, e.Ledger, e.Transaction, e.Operation, e.Err)
	}
	if e.Transaction != "" {
		return fmt.Sprintf("%s: error at ledger %d, tx %s: %v",
			e.Processor, e.Ledger, e.Transaction, e.Err)
	}
	return fmt.Sprintf("%s: error at ledger %d: %v",
		e.Processor, e.Ledger, e.Err)
}

func (e ProcessingError) Unwrap() error {
	return e.Err
}

// NewProcessingError creates a new processing error with context
func NewProcessingError(processor string, ledger uint32, tx string, op int, err error) error {
	return ProcessingError{
		Processor:   processor,
		Ledger:      ledger,
		Transaction: tx,
		Operation:   op,
		Err:         err,
	}
}

// ValidationError represents a validation failure
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
}

func (e ValidationError) Error() string {
	if e.Value != nil {
		return fmt.Sprintf("validation error for %s (value: %v): %s", e.Field, e.Value, e.Message)
	}
	return fmt.Sprintf("validation error for %s: %s", e.Field, e.Message)
}

// BatchError represents errors from batch processing
type BatchError struct {
	Errors []error
}

func (e BatchError) Error() string {
	return fmt.Sprintf("batch processing failed with %d errors", len(e.Errors))
}

// ErrorContext provides a fluent interface for building errors with context
type ErrorContext struct {
	processor string
	ledger    uint32
	tx        string
	op        int
}

// NewErrorContext creates a new error context builder
func NewErrorContext(processor string) *ErrorContext {
	return &ErrorContext{
		processor: processor,
		op:        -1,
	}
}

// WithLedger adds ledger context
func (c *ErrorContext) WithLedger(ledger uint32) *ErrorContext {
	c.ledger = ledger
	return c
}

// WithTransaction adds transaction context
func (c *ErrorContext) WithTransaction(tx string) *ErrorContext {
	c.tx = tx
	return c
}

// WithOperation adds operation context
func (c *ErrorContext) WithOperation(op int) *ErrorContext {
	c.op = op
	return c
}

// Wrap wraps an error with the context
func (c *ErrorContext) Wrap(err error) error {
	if err == nil {
		return nil
	}
	return NewProcessingError(c.processor, c.ledger, c.tx, c.op, err)
}