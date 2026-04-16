package db

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

type Result struct {
	Inclusion time.Time
	Exclusion sql.NullTime
}

func TestCreateOutputView(t *testing.T) {
	// set up test database
	conn := SetupTestDB(t)
	defer conn.Close()

	// seed test data
	FillTestData(t, conn)

	// create view
	viewName := "v_test_output"
	err := conn.CreateOutputView(viewName)
	if err != nil {
		t.Fatalf("create view error: %v", err)
	}

	// query view results
	query := fmt.Sprintf(SelectQueryString, viewName)
	rows, err := conn.db.Query(query)
	if err != nil {
		t.Fatalf("view query error: %v", err)
	}
	defer rows.Close()

	results := make(map[string]Result)

	for rows.Next() {
		var record string
		var inc time.Time
		var exc sql.NullTime
		if err := rows.Scan(&record, &inc, &exc); err != nil {
			t.Fatalf("row scan error: %v", err)
		}
		results[record] = Result{Inclusion: inc, Exclusion: exc}
	}

	// Asserts
	checkAssert := func(record string, expectedInc string, expectedExc string) {
		res, ok := results[record]
		if !ok {
			t.Errorf("record %s not found in view", record)
			return
		}

		incStr := res.Inclusion.Format("2006-01-02")
		if incStr != expectedInc {
			t.Errorf(
				"[%s] expected inclusion date %s, got %s",
				record, expectedInc, incStr,
			)
		}

		var excStr string
		if res.Exclusion.Valid {
			excStr = res.Exclusion.Time.Format("2006-01-02")
		} else {
			excStr = "NULL"
		}

		if excStr != expectedExc {
			t.Errorf(
				"[%s] expected exclusion date %s, got %s",
				record, expectedExc, excStr,
			)
		}
	}

	checkAssert("record 1", "2026-02-01", "2026-03-01")
	checkAssert("record 2", "2026-04-01", "NULL")
	checkAssert("record 3", "2026-02-01", "NULL")
}
