package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"github.com/withObsrvr/cdp-pipeline-workflow/utils"
)

type SaveToExcel struct {
	filePath   string
	writer     *utils.ExcelWriter
	processors []processor.Processor
}

func NewSaveToExcel(config map[string]interface{}) (*SaveToExcel, error) {
	filePath, ok := config["file_path"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration: missing 'file_path'")
	}

	headers := []string{"Timestamp", "BuyerAccountId", "SellerAccountId", "AssetCode", "Amount", "Memo"}
	writer, err := utils.NewExcelWriter(filePath, "Payments", headers)
	if err != nil {
		return nil, fmt.Errorf("failed to create Excel writer: %w", err)
	}

	return &SaveToExcel{
		filePath: filePath,
		writer:   writer,
	}, nil
}

func (c *SaveToExcel) Subscribe(processor processor.Processor) {
	c.processors = append(c.processors, processor)
}

func (c *SaveToExcel) Process(ctx context.Context, msg processor.Message) error {
	log.Printf("Processing message in SaveToExcel")

	var payment AppPayment
	payloadBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte type for message.Payload, got %T", msg.Payload)
	}

	if err := json.Unmarshal(payloadBytes, &payment); err != nil {
		return err
	}

	values := []interface{}{
		payment.Timestamp,
		payment.BuyerAccountId,
		payment.SellerAccountId,
		payment.AssetCode,
		payment.Amount,
		payment.Memo,
	}

	if err := c.writer.AppendRow(values); err != nil {
		return fmt.Errorf("failed to append row: %w", err)
	}

	if err := c.writer.Save(); err != nil {
		return fmt.Errorf("failed to save Excel file: %w", err)
	}

	return nil
}

func (c *SaveToExcel) Close() error {
	if c.writer != nil {
		return c.writer.Close()
	}
	return nil
}

type AppPayment struct {
	Timestamp       string `json:"timestamp"`
	BuyerAccountId  string `json:"buyer_account_id"`
	SellerAccountId string `json:"seller_account_id"`
	AssetCode       string `json:"asset_code"`
	Amount          string `json:"amount"`
	Type            string `json:"type"`
	Memo            string `json:"memo"`
}
