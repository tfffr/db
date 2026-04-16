package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xuri/excelize/v2"
)

const QueryString = `
SELECT
	json_data,
	dt_inclusion,
	dt_exclusion
FROM %s
`

type ApplyFunc func(row map[string]any) any
type FilterFunc func(row map[string]any) bool

func (conn *DBConnection) ExportViewToExcel(
	viewName string,
	fileName string,
	extractKeys []string,
	apply map[string]ApplyFunc,
	filter FilterFunc,
) error {
	// query view data
	query := fmt.Sprintf(QueryString, viewName)
	rows, err := conn.db.Query(query)
	if err != nil {
		return fmt.Errorf("view query error: %w", err)
	}
	defer rows.Close()

	// new xlsx workbook
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	// create worksheet
	err = createNewSheet(f)
	if err != nil {
		return err
	}

	// xlsx header row
	writeHeaders(f, extractKeys)

	// fill xlsx from rows
	if err := writeData(f, rows, extractKeys, apply, filter); err != nil {
		return err
	}

	// save workbook to disk
	if err := saveFile(f, fileName); err != nil {
		return err
	}

	return nil
}

// create worksheet
func createNewSheet(f *excelize.File) error {
	sheet, err := f.NewSheet(SheetName)
	if err != nil {
		return fmt.Errorf("worksheet create error: %w", err)
	}
	f.SetActiveSheet(sheet)
	if SheetName != "Sheet1" {
		f.DeleteSheet("Sheet1")
	}
	return nil
}

// xlsx header row
func writeHeaders(f *excelize.File, extractKeys []string) {
	headers := []string{HeadersData, HeadersInclusionDate, HeadersExclusionDate}
	headers = append(headers, extractKeys...)
	for i, header := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		f.SetCellValue(SheetName, cell, header)
	}
}

// write row data into xlsx
func writeData(
	f *excelize.File,
	rows *sql.Rows,
	extractKeys []string,
	apply map[string]ApplyFunc,
	filter FilterFunc,
) error {
	rowIdx := 2 // first data row below headers

	for rows.Next() {
		var jsonData []byte
		var dtInclusion any
		var dtExclusion any

		if err := rows.Scan(&jsonData, &dtInclusion, &dtExclusion); err != nil {
			return fmt.Errorf("row scan error: %w", err)
		}

		// apply filter before writing row
		if filter != nil {
			var parsed map[string]any
			if err := json.Unmarshal(jsonData, &parsed); err == nil {
				// skip rows excluded by filter
				if !filter(parsed) {
					continue
				}
			}
		}

		// base columns
		f.SetCellValue(SheetName, fmt.Sprintf("B%d", rowIdx), dtInclusion)
		f.SetCellValue(SheetName, fmt.Sprintf("C%d", rowIdx), dtExclusion)
		f.SetCellValue(SheetName, fmt.Sprintf("A%d", rowIdx), string(jsonData))

		// extra json columns
		if len(extractKeys) > 0 {
			writeExtraColumns(f, rowIdx, jsonData, extractKeys, apply)
		}

		rowIdx++
	}

	// rows iteration error check
	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %w", err)
	}

	return nil
}

func writeExtraColumns(
	f *excelize.File,
	rowIdx int,
	jsonData []byte,
	extraKeys []string,
	apply map[string]ApplyFunc,
) {
	var parsed map[string]interface{}

	// skip invalid json
	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		return
	}

	for i, key := range extraKeys {
		colIdx := 4 + i // columns 1-3 are base; extras start at column 4
		cell, _ := excelize.CoordinatesToCellName(colIdx, rowIdx)

		// optional apply transform
		if fn, exists := apply[key]; exists {
			f.SetCellValue(SheetName, cell, fn(parsed))
		} else if val, ok := parsed[key]; ok {
			cell, _ := excelize.CoordinatesToCellName(colIdx, rowIdx)
			f.SetCellValue(SheetName, cell, val)
		}
	}
}

func saveFile(f *excelize.File, fileName string) error {
	dir := filepath.Dir(fileName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("mkdir error %s: %w", dir, err)
	}

	if err := f.SaveAs(fileName); err != nil {
		return fmt.Errorf("file save error: %w", err)
	}

	return nil
}
