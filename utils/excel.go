package utils

import (
	"fmt"
	"os"

	"github.com/xuri/excelize/v2"
)

type ExcelWriter struct {
	filePath  string
	sheetName string
	headers   []string
	file      *excelize.File
}

func NewExcelWriter(filePath, sheetName string, headers []string) (*ExcelWriter, error) {
	writer := &ExcelWriter{
		filePath:  filePath,
		sheetName: sheetName,
		headers:   headers,
	}

	f, err := excelize.OpenFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			f = excelize.NewFile()
			f.SetSheetName("Sheet1", sheetName)

			// Write headers
			for i, h := range headers {
				cell, _ := excelize.CoordinatesToCellName(i+1, 1)
				f.SetCellValue(sheetName, cell, h)
			}
		} else {
			return nil, fmt.Errorf("error opening Excel file: %w", err)
		}
	}
	writer.file = f

	return writer, nil
}

func (w *ExcelWriter) AppendRow(values []interface{}) error {
	rows, err := w.file.GetRows(w.sheetName)
	if err != nil {
		return fmt.Errorf("error getting rows: %w", err)
	}
	rowNum := len(rows) + 1

	for i, v := range values {
		cell, _ := excelize.CoordinatesToCellName(i+1, rowNum)
		w.file.SetCellValue(w.sheetName, cell, v)
	}

	return nil
}

func (w *ExcelWriter) Save() error {
	if err := w.file.SaveAs(w.filePath); err != nil {
		return fmt.Errorf("error saving Excel file: %w", err)
	}
	return nil
}

func (w *ExcelWriter) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
