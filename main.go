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
	db        *sql.DB
	Table     string
	DateTable string
}

// Новое соединение
func NewConnection(dsn, table, dateTable string) (*DBConnection, error) {
	db, err := sql.Open("postgres", dsn)

	if err != nil {
		return nil, fmt.Errorf("Ошибка драйвера БД: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("БД недоступна: %w", err)
	}

	return &DBConnection{
		db:        db,
		Table:     table,
		DateTable: dateTable,
	}, nil
}

// Закрытие соединения
func (conn *DBConnection) Close() error {
	return conn.db.Close()
}

// Создание таблиц
func (conn *DBConnection) InitTables() error {
	query := fmt.Sprintf(SQLCreateTable, conn.Table, conn.DateTable, conn.Table)
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
