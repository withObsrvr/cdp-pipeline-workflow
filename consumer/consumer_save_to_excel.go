package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/withObsrvr/cdp-pipeline-workflow/processor"
	"github.com/xuri/excelize/v2"
)

type SaveToExcel struct {
	filePath   string
	processors []processor.Processor
}

func NewSaveToExcel(config map[string]interface{}) (*SaveToExcel, error) {
	filePath, ok := config["file_path"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration for SaveToExcel: missing 'file_path'")
	}

	return &SaveToExcel{filePath: filePath}, nil
}

func (c *SaveToExcel) Subscribe(processor processor.Processor) {
	c.processors = append(c.processors, processor)
}

func (c *SaveToExcel) Process(ctx context.Context, msg processor.Message) error {
	log.Printf("Processing message in SaveToExcel")
	f, err := excelize.OpenFile(c.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			f = excelize.NewFile()
			f.SetSheetName("Sheet1", "Payments")
			headers := []string{"Timestamp", "BuyerAccountId", "SellerAccountId", "AssetCode", "Amount"}
			for i, h := range headers {
				cell, _ := excelize.CoordinatesToCellName(i+1, 1)
				f.SetCellValue("Payments", cell, h)
			}
		} else {
			return fmt.Errorf("error opening Excel file: %w", err)
		}
	}

	var payment AppPayment
	payloadBytes, ok := msg.Payload.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte type for message.Payload, got %T", msg.Payload)
	}

	if err := json.Unmarshal(payloadBytes, &payment); err != nil {
		return err
	}

	rows, err := f.GetRows("Payments")
	if err != nil {
		return err
	}
	rowNum := len(rows) + 1

	values := []interface{}{
		payment.Timestamp,
		payment.BuyerAccountId,
		payment.SellerAccountId,
		payment.AssetCode,
		payment.Amount,
		payment.Memo,
	}
	for i, v := range values {
		cell, _ := excelize.CoordinatesToCellName(i+1, rowNum)
		f.SetCellValue("Payments", cell, v)
	}

	if err := f.SaveAs(c.filePath); err != nil {
		return err
	}

	// log.Printf("Payment saved to Excel: %v", payment)
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
