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

// set up test database
func SetupTestDB(t *testing.T) *DBConnection {
	t.Helper()

	// database connection dsn
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s",
		GetEnv("DB_HOST", "localhost"),
		GetEnv("DB_PORT", "5432"),
		GetEnv("DB_USER", "user"),
		GetEnv("DB_PSWD", "pswd"),
		GetEnv("DB_NAME", "db"),
	)
	conn, err := NewConnection(dsn, "test_table", false)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	// drop existing tables
	dropQueryString := "DROP TABLE IF EXISTS %s, %s CASCADE;"
	dropQuery := fmt.Sprintf(dropQueryString, conn.DateTable, conn.Table)
	_, err = conn.db.Exec(dropQuery)
	if err != nil {
		t.Fatalf("table cleanup error: %v", err)
	}

	// initialize tables
	err = conn.InitTables()
	if err != nil {
		t.Fatalf("initialization error: %v", err)
	}

	return conn
}

// seed test database
func FillTestData(t *testing.T, conn *DBConnection) {
	t.Helper()

	// insert seed rows
	record1, _ := json.Marshal(map[string]string{"record": "record 1", "foo": "bar", "genre": "jazz"})
	record2, _ := json.Marshal(map[string]string{"record": "record 2", "foo": "baz", "genre": "blues"})
	record3, _ := json.Marshal(map[string]string{"record": "record 3", "foo": "bal", "genre": "disco"})

	insertRecord(t, conn, record1, "2026-02-01 10:00:00Z")
	insertRecord(t, conn, record2, "2026-02-01 10:00:00Z")
	insertRecord(t, conn, record3, "2026-02-01 10:00:00Z")

	insertRecord(t, conn, record3, "2026-03-01 10:00:00Z") // records 1 and 2 drop out of latest snapshot

	insertRecord(t, conn, record2, "2026-04-01 10:00:00Z") // record 2 reappears in snapshot
	insertRecord(t, conn, record3, "2026-04-01 10:00:00Z")
}

// insert a row into test tables
func insertRecord(
	t *testing.T,
	conn *DBConnection,
	data json.RawMessage,
	dt string,
) {
	t.Helper()

	var id int

	// main data table insert
	err := conn.db.QueryRow(
		fmt.Sprintf(InsertQueryString, conn.Table), data,
	).Scan(&id)

	if err != nil {
		t.Fatalf("data insert error: %v", err)
	}

	// date table insert
	_, err = conn.db.Exec(
		fmt.Sprintf(InsertDateQueryString, conn.DateTable), id, dt,
	)

	if err != nil {
		t.Fatalf("date insert error: %v", err)
	}
}
