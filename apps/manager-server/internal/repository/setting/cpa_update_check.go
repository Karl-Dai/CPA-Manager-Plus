package setting

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const cpaUpdateCheckKey = "cpa_update_check_v1"

func LoadCPAUpdateCheck(ctx context.Context, db *sql.DB) ([]byte, error) {
	var data []byte
	err := db.QueryRowContext(ctx, "select value from settings where key = ?", cpaUpdateCheckKey).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return data, err
}

func SaveCPAUpdateCheck(ctx context.Context, db *sql.DB, data []byte) error {
	_, err := db.ExecContext(
		ctx,
		"insert into settings(key,value,updated_at_ms) values(?,?,?) on conflict(key) do update set value=excluded.value,updated_at_ms=excluded.updated_at_ms",
		cpaUpdateCheckKey,
		string(data),
		time.Now().UnixMilli(),
	)
	return err
}
