//go:build pglive

package voting

import (
	"context"
	"testing"

	dbtest "github.com/kwilteam/kwil-db/internal/sql/pg/test"

	"github.com/stretchr/testify/require"
)

func Test_VotingLive(t *testing.T) {
	for _, tt := range votingTestCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			db, err := dbtest.NewTestDB(t)
			require.NoError(t, err)
			defer db.Close()

			dbTx, err := db.BeginTx(ctx)
			require.NoError(t, err)
			defer dbTx.Rollback(ctx) // always rollback to ensure cleanup

			err = InitializeVoteStore(ctx, dbTx)
			require.NoError(t, err)

			for addr, power := range tt.startingPower {
				err = SetValidatorPower(ctx, dbTx, []byte(addr), power)
				require.NoError(t, err)
			}

			tt.fn(t, dbTx)
		})
	}
}
