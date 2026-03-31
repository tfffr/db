package db

import (
	"fmt"

	"github.com/xuri/excelize/v2"
)

const QueryString = `
SELECT
	json_data,
	dt_inclusion,
	dt_exclusion
FROM %s
`

func (conn *DBConnection) ExportViewToExcel(viewName string, fileName string) error {
	// Запрос данных из view

	query := fmt.Sprintf(QueryString, viewName)
	rows, err := conn.db.Query(query)
	if err != nil {
		return fmt.Errorf("Ошибка запроса к view: %w", err)
	}
	defer rows.Close()

	// Новый XLSX файл
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	// Создание нового листа
	sheet, err := f.NewSheet(SheetName)
	if err != nil {
		return fmt.Errorf("Ошибка создания листа: %w", err)
	}
	f.SetActiveSheet(sheet)
	if SheetName != "Sheet1" {
		f.DeleteSheet("Sheet1")
	}

	// Заголовки для XLSX
	headers := []string{HeadersData, HeadersInclusionDate, HeadersExclusionDate}
	for i, header := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		f.SetCellValue(SheetName, cell, header)
	}

	// Заполнение XLSX данными
	rowIdx := 2
	for rows.Next() {
		var jsonData []byte
		var dtInclusion any
		var dtExclusion any

		if err := rows.Scan(&jsonData, &dtInclusion, &dtExclusion); err != nil {
			return fmt.Errorf("Ошибка сканирования строки: %w", err)
		}

		f.SetCellValue(SheetName, fmt.Sprintf("B%d", rowIdx), dtInclusion)
		f.SetCellValue(SheetName, fmt.Sprintf("C%d", rowIdx), dtExclusion)
		f.SetCellValue(SheetName, fmt.Sprintf("A%d", rowIdx), string(jsonData))

		rowIdx++
	}

	// Сохранение файла
	if err := f.SaveAs(fileName); err != nil {
		return fmt.Errorf("Ошибка сохранения файла: %w", err)
	}

	return nil
}
