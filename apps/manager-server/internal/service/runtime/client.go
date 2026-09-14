// Package runtime defines the Manager application boundary for observing CPA
// runtime state.
package runtime

import (
	"context"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
)

// RuntimeClient reports observed Runtime state without exposing an adapter or
// transport to application code. Status returns an error when the adapter
// cannot obtain reliable observed state. RuntimeStateOffline is reserved for
// authoritative observation that CPA is unavailable; transport and
// authentication failures are errors.
type RuntimeClient interface {
	Status(ctx context.Context) (model.RuntimeObservedStatus, error)
}
