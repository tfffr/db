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
	err = conn.ExportViewToExcel(viewName, "report.xlsx", exportKeys, nil, nil)
	if err != nil {
		t.Fatalf("Ошибка экспорта в XLSX: %v", err)
	}
}

func TestExportToExcelWithApply(t *testing.T) {
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

	genreIdealTrack := map[string]string{
		"jazz":  "Miles Davis - So What",
		"blues": "Robert Johnson - Cross Road Blues",
		"disco": "Donna Summer - I Feel Love",
	}

	applyIdealTrackToGenre := func(genre string) string {
		return genreIdealTrack[genre]
	}

	applyFunc := map[string]ApplyFunc{
		"ideal_track": func(row map[string]interface{}) interface{} {
			if genre, ok := row["genre"].(string); ok {
				return applyIdealTrackToGenre(genre)
			}
			return ""
		},
	}

	exportKeys := []string{
		"foo",
		"genre",
		"ideal_track",
	}

	err = conn.ExportViewToExcel(viewName, "report.xlsx", exportKeys, applyFunc, nil)
	if err != nil {
		t.Fatalf("Ошибка экспорта в XLSX: %v", err)
	}
}

func TestExportToExcelWithFilter(t *testing.T) {
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

	filterFunc := func(row map[string]any) bool {
		if genre, ok := row["genre"].(string); ok && genre == "disco" {
			return false
		}
		return true
	}

	exportKeys := []string{
		"foo",
		"genre",
		"ideal_track",
	}

	err = conn.ExportViewToExcel(viewName, "report.xlsx", exportKeys, nil, filterFunc)
	if err != nil {
		t.Fatalf("Ошибка экспорта в XLSX: %v", err)
	}
}
