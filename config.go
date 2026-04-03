package db

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

const (
	// PostgreSQL
	Timeout = 60

	// XLSX
	SheetName            = "Данные"
	HeadersData          = "JSON"
	HeadersInclusionDate = "Дата включения записи"
	HeadersExclusionDate = "Дата исключения записи"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Предупреждение: файл .env не найден, используются системные переменные")
	}
}

func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
