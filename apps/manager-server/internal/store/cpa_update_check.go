package store

import (
	"context"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/setting"
)

func (s *Store) LoadCPAUpdateCheck(ctx context.Context) ([]byte, error) {
	return setting.LoadCPAUpdateCheck(ctx, s.db)
}

func (s *Store) SaveCPAUpdateCheck(ctx context.Context, data []byte) error {
	return setting.SaveCPAUpdateCheck(ctx, s.db, data)
}
