package db

import (
	"database/sql"
	"encoding/json"
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

type Transformer func(row map[string]interface{}) interface{}

func (conn *DBConnection) ExportViewToExcel(
	viewName string,
	fileName string,
	extractKeys []string,
	apply map[string]Transformer,
) error {
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
	err = createNewSheet(f)
	if err != nil {
		return err
	}

	// Заголовки для XLSX
	writeHeaders(f, extractKeys)

	// Заполнение XLSX данными
	if err := writeData(f, rows, extractKeys, apply); err != nil {
		return err
	}

	// Сохранение файла
	if err := f.SaveAs(fileName); err != nil {
		return fmt.Errorf("Ошибка сохранения файла: %w", err)
	}

	return nil
}

// Создание нового листа
func createNewSheet(f *excelize.File) error {
	sheet, err := f.NewSheet(SheetName)
	if err != nil {
		return fmt.Errorf("Ошибка создания листа: %w", err)
	}
	f.SetActiveSheet(sheet)
	if SheetName != "Sheet1" {
		f.DeleteSheet("Sheet1")
	}
	return nil
}

// Заголовки для XLSX
func writeHeaders(f *excelize.File, extractKeys []string) {
	headers := []string{HeadersData, HeadersInclusionDate, HeadersExclusionDate}
	headers = append(headers, extractKeys...)
	for i, header := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		f.SetCellValue(SheetName, cell, header)
	}
}

// Заполнение XLSX данными
func writeData(
	f *excelize.File,
	rows *sql.Rows,
	extractKeys []string,
	apply map[string]Transformer,
) error {
	rowIdx := 2 // Запись после заголовков

	for rows.Next() {
		var jsonData []byte
		var dtInclusion any
		var dtExclusion any

		if err := rows.Scan(&jsonData, &dtInclusion, &dtExclusion); err != nil {
			return fmt.Errorf("Ошибка сканирования строки: %w", err)
		}

		// Базовые столбцы
		f.SetCellValue(SheetName, fmt.Sprintf("B%d", rowIdx), dtInclusion)
		f.SetCellValue(SheetName, fmt.Sprintf("C%d", rowIdx), dtExclusion)
		f.SetCellValue(SheetName, fmt.Sprintf("A%d", rowIdx), string(jsonData))

		// Дополнительные столбцы
		if len(extractKeys) > 0 {
			writeExtraColumns(f, rowIdx, jsonData, extractKeys, apply)
		}

		rowIdx++
	}

	// Проверка на ошибки
	if err := rows.Err(); err != nil {
		return fmt.Errorf("Ошибка при итерировании строк: %w", err)
	}

	return nil
}

func writeExtraColumns(
	f *excelize.File,
	rowIdx int,
	jsonData []byte,
	extraKeys []string,
	apply map[string]Transformer,
) {
	var parsed map[string]interface{}

	// Пропуск, если невалидный JSON
	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		return
	}

	for i, key := range extraKeys {
		colIdx := 4 + i // Первые 3 столбца - базовые. Начало с 4 столбца
		cell, _ := excelize.CoordinatesToCellName(colIdx, rowIdx)

		// Есть ли функция apply
		if fn, exists := apply[key]; exists {
			f.SetCellValue(SheetName, cell, fn(parsed))
		} else if val, ok := parsed[key]; ok {
			cell, _ := excelize.CoordinatesToCellName(colIdx, rowIdx)
			f.SetCellValue(SheetName, cell, val)
		}
	}
}
