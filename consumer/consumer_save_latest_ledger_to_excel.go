package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"github.com/withObsrvr/cdp-pipeline-workflow/utils"
)

type SaveLatestLedgerToExcel struct {
	filePath   string
	writer     *utils.ExcelWriter
	processors []processor.Processor
}

type LedgerData struct {
	Sequence       uint32    `json:"sequence"`
	Hash           string    `json:"hash"`
	PreviousHash   string    `json:"previous_hash"`
	CloseTime      time.Time `json:"close_time"`
	TotalCoins     string    `json:"total_coins"`
	FeePool        string    `json:"fee_pool"`
	BaseFee        uint32    `json:"base_fee"`
	BaseReserve    uint32    `json:"base_reserve"`
	MaxTxSetSize   uint32    `json:"max_tx_set_size"`
	TxCount        int       `json:"tx_count"`
	OperationCount int       `json:"operation_count"`
}

func NewSaveLatestLedgerToExcel(config map[string]interface{}) (*SaveLatestLedgerToExcel, error) {
	filePath, ok := config["file_path"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration: missing 'file_path'")
	}

	headers := []string{
		"Sequence", "Hash", "PreviousHash", "CloseTime",
		"TotalCoins", "FeePool", "BaseFee", "BaseReserve",
		"MaxTxSetSize", "TxCount", "OperationCount",
	}

	writer, err := utils.NewExcelWriter(filePath, "Ledgers", headers)
	if err != nil {
		return nil, fmt.Errorf("failed to create Excel writer: %w", err)
	}

	return &SaveLatestLedgerToExcel{
		filePath: filePath,
		writer:   writer,
	}, nil
}

func (c *SaveLatestLedgerToExcel) Subscribe(processor processor.Processor) {
	c.processors = append(c.processors, processor)
}

func (c *SaveLatestLedgerToExcel) Process(ctx context.Context, msg processor.Message) error {
	log.Printf("Processing message in SaveLatestLedgerToExcel")

	var ledger LedgerData
	payloadBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte type for message.Payload, got %T", msg.Payload)
	}

	if err := json.Unmarshal(payloadBytes, &ledger); err != nil {
		return err
	}

	values := []interface{}{
		ledger.Sequence,
		ledger.Hash,
		ledger.PreviousHash,
		ledger.CloseTime,
		ledger.TotalCoins,
		ledger.FeePool,
		ledger.BaseFee,
		ledger.BaseReserve,
		ledger.MaxTxSetSize,
		ledger.TxCount,
		ledger.OperationCount,
	}

	if err := c.writer.AppendRow(values); err != nil {
		return fmt.Errorf("failed to append row: %w", err)
	}

	if err := c.writer.Save(); err != nil {
		return fmt.Errorf("failed to save Excel file: %w", err)
	}

	return nil
}

func (c *SaveLatestLedgerToExcel) Close() error {
	if c.writer != nil {
		return c.writer.Close()
	}
	return nil
}
