package db

import (
	"encoding/json"
	"fmt"
	"testing"
)

const (
	InsertQueryString = `
INSERT INTO %s (json_data, hash, page_number)
VALUES ($1::jsonb, md5($1::text), 1)
ON CONFLICT (hash) DO UPDATE SET hash = EXCLUDED.hash
RETURNING id
`
	InsertDateQueryString = `
INSERT INTO %s (main_id, dt)
VALUES ($1, $2)
`
	SelectQueryString = `
SELECT json_data->>'record', dt_inclusion, dt_exclusion FROM %s
`
)

// Подготовка тестовой БД
func SetupTestDB(t *testing.T) *DBConnection {
	t.Helper()

	// Соединение с БД
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s",
		DBHost, DBPort, DBUser, DBPswd, DBName,
	)
	conn, err := NewConnection(dsn, "test_table", "test_dt")
	if err != nil {
		t.Fatalf("Не удалось подключиться к БД: %v", err)
	}

	// Предварительная очистка БД
	dropQueryString := "DROP TABLE IF EXISTS %s, %s CASCADE;"
	dropQuery := fmt.Sprintf(dropQueryString, conn.DateTable, conn.Table)
	_, err = conn.db.Exec(dropQuery)
	if err != nil {
		t.Fatalf("Ошибка очистки таблиц: %v", err)
	}

	// Инициализация таблиц
	err = conn.InitTables()
	if err != nil {
		t.Fatalf("Ошибка инициализации: %v", err)
	}

	return conn
}

// Наполнение тестовой БД данными
func FillTestData(t *testing.T, conn *DBConnection) {
	t.Helper()

	// Вставка тестовых данных
	record1, _ := json.Marshal(map[string]string{"record": "record 1"})
	record2, _ := json.Marshal(map[string]string{"record": "record 2"})
	record3, _ := json.Marshal(map[string]string{"record": "record 3"})

	insertRecord(t, conn, record1, "2026-02-01 10:00:00Z")
	insertRecord(t, conn, record2, "2026-02-01 10:00:00Z")
	insertRecord(t, conn, record3, "2026-02-01 10:00:00Z")

	insertRecord(t, conn, record3, "2026-03-01 10:00:00Z") // 1 & 2 пропали

	insertRecord(t, conn, record2, "2026-04-01 10:00:00Z") // 2 вновь появляется
	insertRecord(t, conn, record3, "2026-04-01 10:00:00Z")
}

// Запись данных в БД
func insertRecord(
	t *testing.T,
	conn *DBConnection,
	data json.RawMessage,
	dt string,
) {
	t.Helper()

	var id int

	// Таблица данных
	err := conn.db.QueryRow(
		fmt.Sprintf(InsertQueryString, conn.Table), data,
	).Scan(&id)

	if err != nil {
		t.Fatalf("Ошибка вставки данных: %v", err)
	}

	// Таблица дат
	_, err = conn.db.Exec(
		fmt.Sprintf(InsertDateQueryString, conn.DateTable), id, dt,
	)

	if err != nil {
		t.Fatalf("Ошибка вставки дат: %v", err)
	}
}
