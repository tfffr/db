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
		COALESCE(m.person_hash, m.hash) as entity_id,
		m.id AS main_id,
		d.dt::date AS dump_dt
	FROM %s m
	JOIN %s d ON m.id = d.main_id
	GROUP BY COALESCE(m.person_hash, m.hash), d.dt::date
),
grouped_presence AS (
	SELECT
		p.entity_id,
		p.dump_dt,
		od.dump_id - ROW_NUMBER() OVER (PARTITION BY p.entity_id ORDER BY p.dump_dt) AS grp
	FROM presence p
	JOIN ordered_dumps od ON p.dump_dt = od.dump_dt
),
islands AS (
	SELECT
		entity_id,
		MIN(dump_dt) AS island_start_dt,
		MAX(dump_dt) AS island_end_dt,
		ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY MAX(dump_dt) DESC) AS rn
	FROM grouped_presence
	GROUP BY entity_id, grp
),
latest_records AS (
	SELECT DISTINCT ON (i.entity_id, i.island_end_dt)
		i.entity_id,
		i.island_start_dt,
		i.island_end_dt,
		i.rn,
		m.json_data
	FROM islands i
	JOIN %s m ON COALESCE(m.person_hash, m.hash) = i.entity_id
	JOIN %s ON m.id = d.main_id
	WHERE d.dt::date <= i.island_end_dt
	ORDER BY i.entity_id, i.island_end_dt, d.dt DESC
)
SELECT
	m.json_data,
	i.island_start_dt AS dt_inclusion,
	od.next_dump_dt AS dt_exclusion
FROM latest_records lr
JOIN ordered_dumps od ON lr.island_end_dt = od.dump_dt
WHERE i.rn = 1;
`

func (conn *DBConnection) CreateOutputView(viewName string) error {
	query := fmt.Sprintf(
		OutputViewQueryString,
		viewName,       // 1. Имя вьюхи: CREATE OR REPLACE VIEW %s
		conn.DateTable, // 2. global_dumps: SELECT ... FROM %s
		conn.Table,     // 3. presence: FROM %s m
		conn.DateTable, // 4. presence: JOIN %s d
		conn.Table,     // 5. latest_records: JOIN %s m
		conn.DateTable, // 6. latest_records: JOIN %s d
	)

	_, err := conn.db.Exec(query)
	if err != nil {
		return fmt.Errorf("create view error %s: %w", viewName, err)
	}
	return nil
}
