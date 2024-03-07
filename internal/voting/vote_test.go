package voting

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"slices"
	"testing"

	"github.com/kwilteam/kwil-db/common/sql"
	"github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/extensions/resolutions"

	"github.com/stretchr/testify/require"
)

type mockTx struct {
	*mockDB
}

func (m *mockTx) Commit(ctx context.Context) error {
	return nil
}

func (m *mockTx) Rollback(ctx context.Context) error {
	return nil
}

type mockDB struct {
	resTypes    map[string]string // id BYTEA => name TEXT
	resolutions map[string]*resolutions.Resolution
	processed   map[string]bool
	voters      map[string]*types.Validator // uuid => validator
}

func newDB() *mockDB {
	return &mockDB{
		resTypes:    make(map[string]string),
		resolutions: make(map[string]*resolutions.Resolution),
		voters:      make(map[string]*types.Validator),
	}
}

func (m *mockDB) AccessMode() sql.AccessMode {
	return sql.ReadWrite // not use in these tests
}

func (m *mockDB) BeginTx(ctx context.Context) (sql.Tx, error) {
	return &mockTx{m}, nil
}

func (m *mockDB) Execute(ctx context.Context, stmt string, args ...any) (*sql.ResultSet, error) {
	// mock some expected queries from internal functions
	switch stmt {
	case createVotingSchema, tableVoters, tableResolutionTypes, tableResolutions,
		resolutionsTypeIndex, tableProcessed, tableVotes: // InitializeVoteStore
		return &sql.ResultSet{}, nil

	case createResolutionType: // from resolutions.ListResolutions() in InitializeVoteStore
		// insert into resolution_types
		m.resTypes[string(args[0].([]byte))] = args[1].(string)
		return &sql.ResultSet{Status: sql.CommandTag{
			RowsAffected: 1,
		}}, nil

	case upsertVoter: // SetValidatorPower, for resolutions
		uuid, pubKey := args[0].([]byte), args[1].([]byte)
		m.voters[string(uuid)] = &types.Validator{
			PubKey: pubKey,
			Power:  args[0].(int64),
		}
		return &sql.ResultSet{Status: sql.CommandTag{
			RowsAffected: 1,
		}}, nil

	case removeVoter: // SetValidatorPower, for resolutions
		uuid := args[0].([]byte)
		var r int64
		if _, have := m.voters[string(uuid)]; have {
			r = 1
		}
		delete(m.voters, string(uuid))
		return &sql.ResultSet{
			Status: sql.CommandTag{
				RowsAffected: r,
			},
		}, nil

	case alreadyProcessed: // IsProcessed
		uuid := args[0].([]byte)
		_, have := m.processed[string(uuid)] // value ignored by IsProcessed
		var res sql.ResultSet
		if have {
			res.Rows = append(res.Rows, []any{uuid}) // value doesn't matter though
			res.Columns = []string{"id"}
		}

		return &res, nil

	case allVoters: // GetValidators
		res := &sql.ResultSet{
			Columns: []string{"name", "power"},
			Rows:    make([][]any, 0, len(m.voters)),
		}
		for _ /* uuid */, val := range m.voters {
			res.Rows = append(res.Rows, []any{val.PubKey, val.Power})
		}
		return res, nil

	case getVoterPower: // GetValidatorPower
		res := &sql.ResultSet{
			Columns: []string{"power"},
		}
		uuid := args[0].([]byte)
		if val, ok := m.voters[string(uuid)]; ok {
			res.Rows = [][]any{{val.Power}}
		}
		return res, nil

	case totalPower: // RequiredPower
		var total int64
		for _ /* uuid */, val := range m.voters {
			total += val.Power
		}
		res := &sql.ResultSet{
			Columns: []string{"sum"},
			Rows:    [][]any{{total}},
		}
		return res, nil

	case getResolutionByTypeAndProposer: // GetResolutionIDsByTypeAndProposer
		// join resolutions and resolution_types on type (r.type=t.id),
		// args are type name (TEXT) and vote_body_proposer (BYTEA),
		// order by resolution id
		resTypeName := args[0].(string)
		// var resTypeID []byte // type uuid
		// for rID, rName := range m.resTypes {
		// 	if rName == resTypeName {
		// 		resTypeID = []byte(rID)
		// 	}
		// }
		// if len(resTypeID) == 0 {
		// 	return &sql.ResultSet{}, nil
		// }

		res := &sql.ResultSet{
			Columns: []string{"sum"},
		}
		voteBodyProposer := args[1].([]byte)
		for _, resol := range m.resolutions {
			if resol.Type == resTypeName && bytes.EqualFold(resol.Proposer, voteBodyProposer) {
				resID := slices.Clone(resol.ID[:])
				res.Rows = append(res.Rows, []any{resID})
			}
		}

		slices.SortFunc(res.Rows, func(a, b []any) int {
			return bytes.Compare(a[0].([]byte), b[0].([]byte))
		})

		return res, nil

	case getResolutionsFullInfoByType: // GetResolutionsByType
		resTypeName := args[0].(string)
		var resolns []*resolutions.Resolution
		for _, resoln := range m.resolutions {
			if resoln.Type == resTypeName {
				resolns = append(resolns, resoln)
			}
		}

		slices.SortFunc(resolns, func(a, b *resolutions.Resolution) int {
			return bytes.Compare(a.ID[:], b.ID[:])
		})

		res := &sql.ResultSet{
			Columns: []string{"sum"},
		}
		for _, resoln := range resolns {
			res.Rows = append(res.Rows, toRow(resoln))
		}

		return res, nil

	case hasVoted:
		return nil, nil
	default:
		return nil, errors.New("bad query")
	}
}

const testType = "testResolution"

func init() {
	err := resolutions.RegisterResolution(testType, resolutions.ResolutionConfig{})
	if err != nil {
		panic(err)
	}
}

type votingTestCase struct {
	name          string
	startingPower map[string]int64 // starting power for any validators
	fn            func(t *testing.T, db sql.DB)
}

var votingTestCases = []votingTestCase{
	{
		name: "successful creationg and voting",
		startingPower: map[string]int64{
			"a": 100,
			"b": 100,
		},
		fn: func(t *testing.T, db sql.DB) {
			ctx := context.Background()

			err := CreateResolution(ctx, db, testEvent, 10, []byte("a"))
			require.NoError(t, err)

			err = ApproveResolution(ctx, db, testEvent.ID(), 10, []byte("a"))
			require.NoError(t, err)

			err = ApproveResolution(ctx, db, testEvent.ID(), 10, []byte("b"))
			require.NoError(t, err)

			events, err := GetResolutionsByThresholdAndType(ctx, db, testConfirmationThreshold, testType)
			require.NoError(t, err)

			require.Len(t, events, 1)

			require.Equal(t, testEvent.Body, events[0].Body)
			require.Equal(t, testEvent.Type, events[0].Type)
			require.Equal(t, testEvent.ID(), events[0].ID)
			require.Equal(t, int64(10), events[0].ExpirationHeight)
			require.False(t, events[0].DoubleProposerVote)
			require.Equal(t, int64(200), events[0].ApprovedPower)
		},
	},
	{
		name: "validator management",
		fn: func(t *testing.T, db sql.DB) {
			// I add power here because this is part of the domain of validator management
			// if test setup changes, this test will still be valid
			err := SetValidatorPower(context.Background(), db, []byte("a"), 100)
			require.NoError(t, err)

			err = SetValidatorPower(context.Background(), db, []byte("b"), 100)
			require.NoError(t, err)

			voters, err := GetValidators(context.Background(), db)
			require.NoError(t, err)

			require.Len(t, voters, 2)

			voterAPower, err := GetValidatorPower(context.Background(), db, []byte("a"))
			require.NoError(t, err)

			require.Equal(t, int64(100), voterAPower)
		},
	},
	{
		name: "deletion and processed",
		startingPower: map[string]int64{
			"a": 100,
		},
		fn: func(t *testing.T, db sql.DB) {
			ctx := context.Background()

			err := CreateResolution(ctx, db, testEvent, 10, []byte("a"))
			require.NoError(t, err)

			err = DeleteResolutions(ctx, db, testEvent.ID())
			require.NoError(t, err)

			processed, err := IsProcessed(ctx, db, testEvent.ID())
			require.NoError(t, err)

			require.False(t, processed)

			err = MarkProcessed(ctx, db, testEvent.ID())
			require.NoError(t, err)

			processed, err = IsProcessed(ctx, db, testEvent.ID())
			require.NoError(t, err)

			require.True(t, processed)
		},
	},
	{
		name: "reading resolution info",
		startingPower: map[string]int64{
			"a": 100,
			"b": 100,
		},
		fn: func(t *testing.T, db sql.DB) {
			ctx := context.Background()

			// validator 1 will approve first here, to test that it is properly ordered

			err := ApproveResolution(ctx, db, testEvent.ID(), 10, []byte("a"))
			require.NoError(t, err)

			err = CreateResolution(ctx, db, testEvent, 10, []byte("a"))
			require.NoError(t, err)

			err = ApproveResolution(ctx, db, testEvent.ID(), 10, []byte("b"))
			require.NoError(t, err)

			info, err := GetResolutionInfo(ctx, db, testEvent.ID())
			require.NoError(t, err)

			infoSlice, err := GetResolutionsByType(ctx, db, testType)
			require.NoError(t, err)
			require.Len(t, infoSlice, 1)

			require.EqualValues(t, testEvent.ID(), infoSlice[0].ID)

			info2Slice, err := GetResolutionIDsByTypeAndProposer(ctx, db, testType, []byte("a"))
			require.NoError(t, err)
			require.Len(t, info2Slice, 1)

			require.Equal(t, infoSlice[0].ID, info2Slice[0])

			require.Equal(t, testEvent.Body, info.Body)
			require.Equal(t, testEvent.Type, info.Type)
			require.Equal(t, testEvent.ID(), info.ID)
			require.Equal(t, int64(10), info.ExpirationHeight)
			require.True(t, info.DoubleProposerVote)
			require.Equal(t, int64(200), info.ApprovedPower)

			hasValidator1Info := false
			hasValidator2Info := false

			for _, voter := range info.Voters {
				if string(voter.PubKey) == "a" && voter.Power == 100 {
					hasValidator1Info = true
				}

				if string(voter.PubKey) == "b" && voter.Power == 100 {
					hasValidator2Info = true
				}
			}
			if !hasValidator1Info || !hasValidator2Info {
				t.Errorf("expected to find both validators in the voters list")
			}
		},
	},
	{
		name: "test expiration",
		startingPower: map[string]int64{
			"a": 100,
		},
		fn: func(t *testing.T, db sql.DB) {
			ctx := context.Background()

			err := CreateResolution(ctx, db, testEvent, 10, []byte("a"))
			require.NoError(t, err)

			expired, err := GetExpired(ctx, db, 10)
			require.NoError(t, err)
			require.Equal(t, 1, len(expired))

			resolutionInfo, err := GetResolutionInfo(ctx, db, testEvent.ID())
			require.NoError(t, err)

			require.EqualValues(t, resolutionInfo, expired[0])
		},
	},
}

func Test_Voting(t *testing.T) {
	for _, tt := range votingTestCases {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			db := newDB()

			dbTx, err := db.BeginTx(ctx)
			require.NoError(t, err)
			defer dbTx.Rollback(ctx) // always rollback to ensure cleanup

			err = InitializeVoteStore(ctx, db)
			require.NoError(t, err)

			for addr, power := range tt.startingPower {
				err = SetValidatorPower(ctx, db, []byte(addr), power)
				require.NoError(t, err)
			}

			tt.fn(t, dbTx)
		})
	}
}

var testEvent = &types.VotableEvent{
	Body: []byte("test"),
	Type: testType,
}

var testConfirmationThreshold = big.NewRat(2, 3)
