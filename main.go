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
}

// Новое соединение
func NewConnection(dsn, table string) (*DBConnection, error) {
	db, err := sql.Open("postgres", dsn)

	if err != nil {
		return nil, fmt.Errorf("Ошибка драйвера БД: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("БД недоступна: %w", err)
	}

	return &DBConnection{
		db:           db,
		Table:        table,
		DateTable:    table + "_dt",
		AICacheTable: table + "_ai_cache",
	}, nil
}

// Закрытие соединения
func (conn *DBConnection) Close() error {
	return conn.db.Close()
}

// Создание таблиц
func (conn *DBConnection) InitTables() error {
	query := fmt.Sprintf(SQLCreateTable, conn.Table, conn.DateTable, conn.Table, conn.AICacheTable)
	_, err := conn.db.Exec(query)
	return err
}

// Запись данных в таблицы данных и дат
func (conn *DBConnection) SaveBatch(items []json.RawMessage, pageNumber int) error {
	if len(items) == 0 {
		return nil
	}

	// Начало транзакции
	tx, err := conn.db.Begin()
	if err != nil {
		return fmt.Errorf("Ошибка начала транзакции: %w", err)
	}

	defer tx.Rollback()

	// Запрос по таблице данных
	queryTable := fmt.Sprintf(SQLQueryTable, conn.Table)

	// Запрос по таблице дат
	queryDateTable := fmt.Sprintf(SQLQueryDateTable, conn.DateTable)

	for _, item := range items {
		hashSum := sha256.Sum256(item)
		hashStr := hex.EncodeToString(hashSum[:])

		var dataId int // Переменная для хранения ID записи

		err = tx.QueryRow(queryTable, item, hashStr, pageNumber).Scan(&dataId)
		if err != nil {
			return fmt.Errorf(
				"Ошибка при записи/получении ID (для хэша %s): %w",
				hashStr, err,
			)
		}

		_, err = tx.Exec(queryDateTable, dataId)
		if err != nil {
			return fmt.Errorf(
				"Ошибка при записи в таблицу дат (для id %d): %w",
				dataId, err,
			)
		}
	}

	// Подтверждение транзакции
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("Ошибка подтверждения транзакции: %w", err)
	}

	return nil
}

// Выполнение кастомных запросов
func (conn *DBConnection) Exec(query string, args ...any) (sql.Result, error) {
	return conn.db.Exec(query, args...)
}

func (conn *DBConnection) GetAIResponse(hash string) ([]byte, error) {
	var response []byte
	query := fmt.Sprintf("SELECT response FROM %s WHERE hash = $1", conn.AICacheTable)
	err := conn.db.QueryRow(query, hash).Scan(&response)
	return response, err
}

func (conn *DBConnection) SaveAIResponse(hash string, response []byte) error {
	query := fmt.Sprintf(`
INSERT INTO %s (hash, response)
VALUES ($1, $2::jsonb)
ON CONFLICT (hash) DO UPDATE SET response = EXCLUDED.response
	`, conn.AICacheTable)
	_, err := conn.db.Exec(query, hash, string(response))
	return err
}
