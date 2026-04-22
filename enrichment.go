package db

import (
	"fmt"
	"log"
)

func (conn *DBConnection) EnrichPersonHashes(
	processFunc func(jsonData []byte) string,
) error {
	q := fmt.Sprintf(
		"SELECT id, json_data FROM %s WHERE person_hash IS NULL",
		conn.Table,
	)
	rows, err := conn.db.Query(q)
	if err != nil {
		return err
	}
	defer rows.Close()

	updateQ := fmt.Sprintf(
		"UPDATE %s SET person_hash = $1 WHERE id = $2",
		conn.Table,
	)
	stmt, err := conn.db.Prepare(updateQ)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for rows.Next() {
		var id int
		var jsonData []byte
		if err := rows.Scan(&id, &jsonData); err != nil {
			continue
		}

		personHash := processFunc(jsonData)
		if personHash != "" {
			_, err = stmt.Exec(personHash, id)
			if err != nil {
				log.Printf(
					"ошибка обновления person_hash для id %d: %v",
					id, err,
				)
			}
		}
	}
	return nil
}
