package db

import "fmt"

const OutputViewQueryString = `
CREATE OR REPLACE VIEW %s AS
WITH global_dumps AS (
	SELECT DISTINCT dt::date AS dump_dt FROM %s
),
ordered_dumps AS (
	SELECT
		dump_dt,
		LEAD(dump_dt) OVER (ORDER BY dump_dt) AS next_dump_dt,
		DENSE_RANK() OVER (ORDER BY dump_dt) AS dump_id
	FROM global_dumps
),
presence AS (
	SELECT
		m.json_data,
		m.id AS main_id,
		d.dt::date AS dump_dt
	FROM %s m
	JOIN %s d ON m.id = d.main_id
	GROUP BY m.json_data, m.id, d.dt::date
),
grouped_presence AS (
	SELECT
		p.main_id,
		p.dump_dt,
		od.dump_id - ROW_NUMBER() OVER (PARTITION BY p.main_id ORDER BY p.dump_dt) AS grp
	FROM presence p
	JOIN ordered_dumps od ON p.dump_dt = od.dump_dt
),
islands AS (
	SELECT
		main_id,
		MIN(dump_dt) AS island_start_dt,
		MAX(dump_dt) AS island_end_dt,
		ROW_NUMBER() OVER (PARTITION BY main_id ORDER BY MAX(dump_dt) DESC) AS rn
	FROM grouped_presence
	GROUP BY main_id, grp
)
SELECT
	m.json_data,
	i.island_start_dt AS dt_inclusion,
	od.next_dump_dt AS dt_exclusion
FROM islands i
JOIN %s m ON i.main_id = m.id
JOIN ordered_dumps od ON i.island_end_dt = od.dump_dt
WHERE i.rn = 1;
`

func (conn *DBConnection) CreateOutputView(viewName string) error {
	query := fmt.Sprintf(
		OutputViewQueryString,
		viewName,
		conn.DateTable,
		conn.Table,
		conn.DateTable,
		conn.Table,
	)

	_, err := conn.db.Exec(query)
	if err != nil {
		return fmt.Errorf("create view error %s: %w", viewName, err)
	}
	return nil
}
