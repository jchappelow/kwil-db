package optimizer

import (
	"fmt"
	dt "github.com/kwilteam/kwil-db/internal/engine/cost/datatypes"

	"github.com/kwilteam/kwil-db/internal/engine/cost/datasource"
	"github.com/kwilteam/kwil-db/internal/engine/cost/logical_plan"
)

func ExampleProjectionRule_optimize_pushDown() {
	ds := datasource.NewMemDataSource(nil, nil)
	tUser := &dt.TableRef{Table: "users"}
	aop := logical_plan.NewDataFrame(logical_plan.Scan(tUser, ds, nil))
	plan := aop.
		Project(logical_plan.ColumnUnqualified("state"),
			logical_plan.Alias(logical_plan.ColumnUnqualified("username"), "name")).
		LogicalPlan()

	fmt.Println(logical_plan.Format(plan, 0))

	r := &ProjectionRule{}
	got := r.Optimize(plan)

	fmt.Printf("---After optimization---\n\n")
	fmt.Println(logical_plan.Format(got, 0))

	// Output:
	// Projection: state, username AS name
	//   Scan: users; projection=[]
	//
	// ---After optimization---
	//
	// Projection: state, username AS name
	//   Scan: users; projection=[state username]
	//
}

func ExampleProjectionRule_optimize_pushDown_with_selection() {
	ds := datasource.NewMemDataSource(nil, nil)
	tUser := &dt.TableRef{Table: "users"}
	aop := logical_plan.NewDataFrame(logical_plan.Scan(tUser, ds, nil))
	plan := aop.
		Filter(logical_plan.Eq(logical_plan.ColumnUnqualified("age"),
			logical_plan.LiteralInt(20))).
		Project(logical_plan.ColumnUnqualified("state"),
			logical_plan.Alias(logical_plan.ColumnUnqualified("username"), "name")).
		LogicalPlan()

	fmt.Println(logical_plan.Format(plan, 0))

	r := &ProjectionRule{}
	got := r.Optimize(plan)

	fmt.Printf("---After optimization---\n\n")
	fmt.Println(logical_plan.Format(got, 0))

	// Output:
	// Projection: state, username AS name
	//   Filter: age = 20
	//     Scan: users; projection=[]
	//
	// ---After optimization---
	//
	// Projection: state, username AS name
	//   Filter: age = 20
	//     Scan: users; projection=[age state username]
}

func ExampleProjectionRule_optimize_pushDown_with_aggregate() {
	ds := datasource.NewMemDataSource(nil, nil)
	tUser := &dt.TableRef{Table: "users"}
	aop := logical_plan.NewDataFrame(logical_plan.Scan(tUser, ds, nil))
	plan := aop.
		Aggregate(
			[]logical_plan.LogicalExpr{logical_plan.ColumnUnqualified("state")},
			[]logical_plan.LogicalExpr{logical_plan.Count(logical_plan.ColumnUnqualified("username"))}).
		// NOTE: the alias for aggregate result is a bit weird
		Project(logical_plan.ColumnUnqualified("state"),
			logical_plan.Alias(logical_plan.Count(logical_plan.ColumnUnqualified("username")), "num")).
		LogicalPlan()

	fmt.Println(logical_plan.Format(plan, 0))

	r := &ProjectionRule{}
	got := r.Optimize(plan)

	fmt.Printf("---After optimization---\n\n")
	fmt.Println(logical_plan.Format(got, 0))

	// Output:
	// Projection: state, COUNT(username) AS num
	//   Aggregate: [state], [COUNT(username)]
	//     Scan: users; projection=[]
	//
	// ---After optimization---
	//
	// Projection: state, COUNT(username) AS num
	//   Aggregate: [state], [COUNT(username)]
	//     Scan: users; projection=[state username]
	//
}

func ExampleProjectionRule_optimize_pushDown_all_operators() {
	ds := datasource.NewMemDataSource(nil, nil)
	tUser := &dt.TableRef{Table: "users"}
	aop := logical_plan.NewDataFrame(logical_plan.Scan(tUser, ds, nil))
	plan := aop.
		Filter(logical_plan.Eq(logical_plan.ColumnUnqualified("age"),
			logical_plan.LiteralInt(20))).
		Aggregate(
			[]logical_plan.LogicalExpr{logical_plan.ColumnUnqualified("state")},
			[]logical_plan.LogicalExpr{logical_plan.Count(logical_plan.ColumnUnqualified("username"))}).
		// the alias for aggregate result is bit weird
		Project(logical_plan.ColumnUnqualified("state"),
			logical_plan.Alias(logical_plan.Count(logical_plan.ColumnUnqualified("username")), "num")).
		LogicalPlan()

	fmt.Println(logical_plan.Format(plan, 0))

	r := &ProjectionRule{}
	got := r.Optimize(plan)

	fmt.Printf("---After optimization---\n\n")
	fmt.Println(logical_plan.Format(got, 0))

	// Output:
	// Projection: state, COUNT(username) AS num
	//   Aggregate: [state], [COUNT(username)]
	//     Filter: [age = 20]
	//       Scan: users; projection=[]
	//
	// ---After optimization---
	//
	// Projection: state, COUNT(username) AS num
	//   Aggregate: [state], [COUNT(username)]
	//     Filter: [age = 20]
	//       Scan: users; projection=[age state username]
	//
}
