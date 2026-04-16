package db

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"

	_ "github.com/lib/pq"
)

const (
	SQLCreateTable = `
CREATE TABLE IF NOT EXISTS %s (
	id SERIAL PRIMARY KEY,
	json_data JSONB,
	hash VARCHAR(64) UNIQUE,
	page_number INT
);
CREATE TABLE IF NOT EXISTS %s (
	id SERIAL PRIMARY KEY,
	main_id INT REFERENCES %s(id) ON DELETE CASCADE,
	dt TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
	`

	SQLCreateAICacheTable = `
CREATE TABLE IF NOT EXISTS %s (
hash VARCHAR(64) PRIMARY KEY,
response JSONB
);
	`

	SQLQueryTable = `
INSERT INTO %s (json_data, hash, page_number)
VALUES ($1, $2, $3)
ON CONFLICT (hash) DO UPDATE SET hash = EXCLUDED.hash
RETURNING id
	`

	SQLQueryDateTable = `
INSERT INTO %s (main_id)
VALUES ($1)
	`
)

type DBConnection struct {
	db           *sql.DB
	Table        string
	DateTable    string
	AICacheTable string
	WithAICache  bool
}

// new database connection
func NewConnection(dsn, table string, WithAICache bool) (*DBConnection, error) {
	db, err := sql.Open("postgres", dsn)

	if err != nil {
		return nil, fmt.Errorf("database driver error: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("database unreachable: %w", err)
	}

	return &DBConnection{
		db:           db,
		Table:        table,
		DateTable:    table + "_dt",
		AICacheTable: table + "_ai_cache",
		WithAICache:  WithAICache,
	}, nil
}

// close connection
func (conn *DBConnection) Close() error {
	return conn.db.Close()
}

// create tables
func (conn *DBConnection) InitTables() error {
	query := fmt.Sprintf(
		SQLCreateTable, // query
		conn.Table,     // table 1
		conn.DateTable, // table 2
		conn.Table,     // referencing table
	)
	if _, err := conn.db.Exec(query); err != nil {
		return err
	}

	if conn.WithAICache {
		query = fmt.Sprintf(SQLCreateAICacheTable, conn.AICacheTable)
		if _, err := conn.db.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

// save data to main and date tables
func (conn *DBConnection) SaveBatch(items []json.RawMessage, pageNumber int) error {
	if len(items) == 0 {
		return nil
	}

	// begin transaction
	tx, err := conn.db.Begin()
	if err != nil {
		return fmt.Errorf("transaction begin error: %w", err)
	}

	defer tx.Rollback()

	// query main data table
	queryTable := fmt.Sprintf(SQLQueryTable, conn.Table)

	// query date table
	queryDateTable := fmt.Sprintf(SQLQueryDateTable, conn.DateTable)

	for _, item := range items {
		hashSum := sha256.Sum256(item)
		hashStr := hex.EncodeToString(hashSum[:])

		var dataId int // inserted row id

		err = tx.QueryRow(queryTable, item, hashStr, pageNumber).Scan(&dataId)
		if err != nil {
			return fmt.Errorf(
				"write or scan id error (hash %s): %w",
				hashStr, err,
			)
		}

		_, err = tx.Exec(queryDateTable, dataId)
		if err != nil {
			return fmt.Errorf(
				"date table insert error (id %d): %w",
				dataId, err,
			)
		}
	}

	// commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit error: %w", err)
	}

	return nil
}

// run custom queries
func (conn *DBConnection) Exec(query string, args ...any) (sql.Result, error) {
	return conn.db.Exec(query, args...)
}

func (conn *DBConnection) GetAIResponse(hash string) ([]byte, error) {
	if !conn.WithAICache {
		return nil, fmt.Errorf("ai cache is disabled")
	}
	var response []byte
	query := fmt.Sprintf("SELECT response FROM %s WHERE hash = $1", conn.AICacheTable)
	err := conn.db.QueryRow(query, hash).Scan(&response)
	return response, err
}

func (conn *DBConnection) SaveAIResponse(hash string, response []byte) error {
	if !conn.WithAICache {
		return fmt.Errorf("ai cache is disabled")
	}
	query := fmt.Sprintf(`
INSERT INTO %s (hash, response)
VALUES ($1, $2::jsonb)
ON CONFLICT (hash) DO UPDATE SET response = EXCLUDED.response
	`, conn.AICacheTable)
	_, err := conn.db.Exec(query, hash, string(response))
	return err
}
