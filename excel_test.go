package db

import (
	"testing"
)

func TestExportToExcel(t *testing.T) {
	// Подготовка тестовой БД
	conn := SetupTestDB(t)
	defer conn.Close()

	// Наполнение тестовой БД данными
	FillTestData(t, conn)

	// Создание view
	viewName := "v_test_output"
	err := conn.CreateOutputView(viewName)
	if err != nil {
		t.Fatalf("Ошибка создания view: %v", err)
	}

	exportKeys := []string{
		"foo",
		"genre",
	}
	err = conn.ExportViewToExcel(viewName, "report.xlsx", exportKeys)
	if err != nil {
		t.Fatalf("Ошибка экспорта в XLSX: %v", err)
	}
}
