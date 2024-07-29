package query_planner

import (
	"fmt"
	"github.com/kwilteam/kwil-db/core/types"
	"github.com/kwilteam/kwil-db/internal/engine/cost/catalog"
	ds "github.com/kwilteam/kwil-db/internal/engine/cost/datatypes"
	lp "github.com/kwilteam/kwil-db/internal/engine/cost/logical_plan"
	pt "github.com/kwilteam/kwil-db/internal/engine/cost/plantree"
	tree "github.com/kwilteam/kwil-db/parse"

	"slices"
)

type LogicalPlanner interface {
	ToExpr(expr tree.Expression, schema *ds.Schema) lp.LogicalExpr
	ToPlan(node tree.SQLStatement) lp.LogicalPlan
}

type queryPlanner struct {
	catalog catalog.Catalog
}

func NewPlanner(catalog catalog.Catalog) *queryPlanner {
	return &queryPlanner{
		catalog: catalog,
	}
}

// ToExpr converts a tree.Expression to a logical expression.
// TODO: use iterator or stack to traverse the tree, instead of recursive, to avoid stack overflow.
func (q *queryPlanner) ToExpr(expr tree.Expression, schema *ds.Schema) lp.LogicalExpr {
	switch e := expr.(type) {
	case *tree.ExpressionLiteral:
		switch e.Type {
		case types.IntType:
			// NOTE: I think planner will need to acknowledge the types that engine supports
			v, _ := e.Value.(int64)
			return lp.LiteralNumeric(v)
		case types.TextType:
			v, _ := e.Value.(string)
			return lp.LiteralText(v)
		case types.BlobType:
			v, _ := e.Value.([]byte)
			return lp.LiteralBlob(v)
		case types.BoolType:
			v, _ := e.Value.(bool)
			return lp.LiteralBool(v)
		case types.NullType:
			return lp.LiteralNull()
		}
	case *tree.ExpressionColumn:
		// TODO: handle relation
		return lp.ColumnUnqualified(e.Column)
	//case *tree.ExpressionFunction:
	case *tree.ExpressionUnary:
		switch e.Operator {
		//case tree.UnaryOperatorMinus:
		//case tree.UnaryOperatorPlus:
		case tree.UnaryOperatorNot:
			return lp.Not(q.ToExpr(e.Expression, schema))
		default:
			panic("unknown unary operator")
		}
	case *tree.ExpressionArithmetic:
		l := q.ToExpr(e.Left, schema)
		r := q.ToExpr(e.Right, schema)
		switch e.Operator {
		case tree.ArithmeticOperatorAdd:
			return lp.Add(l, r)
		case tree.ArithmeticOperatorSubtract:
			return lp.Sub(l, r)
		case tree.ArithmeticOperatorMultiply:
			return lp.Mul(l, r)
		case tree.ArithmeticOperatorDivide:
			return lp.Div(l, r)
		//case tree.ArithmeticOperatorModulus:
		default:
			panic("unknown arithmetic operator")
		}
	case *tree.ExpressionComparison:
		l := q.ToExpr(e.Left, schema)
		r := q.ToExpr(e.Right, schema)
		switch e.Operator {
		case tree.ComparisonOperatorEqual:
			return lp.Eq(l, r)
		case tree.ComparisonOperatorNotEqual:
			return lp.Neq(l, r)
		case tree.ComparisonOperatorGreaterThan:
			return lp.Gt(l, r)
		case tree.ComparisonOperatorLessThan:
			return lp.Lt(l, r)
		case tree.ComparisonOperatorGreaterThanOrEqual:
			return lp.Gte(l, r)
		case tree.ComparisonOperatorLessThanOrEqual:
			return lp.Lte(l, r)
		// TODO: make this a separate LogicalExpression?

		default:
			panic("unknown comparison operator")
		}
	case *tree.ExpressionLogical:
		l := q.ToExpr(e.Left, schema)
		r := q.ToExpr(e.Right, schema)
		switch e.Operator {
		case tree.LogicalOperatorAnd:
			return lp.And(l, r)
		case tree.LogicalOperatorOr:
			return lp.Or(l, r)
		default:
			panic("unknown logical operator")
		}
	case *tree.ExpressionFunctionCall:
		var inputs []lp.LogicalExpr
		for _, arg := range e.Args {
			inputs = append(inputs, q.ToExpr(arg, schema))
		}

		// use catalog? since there will be user-defined/kwil-defined functions
		fn, ok := tree.Functions[e.FunctionName()]
		if !ok {
			panic(fmt.Sprintf("function %s not found", e.FunctionName()))
		}

		if fn.IsAggregate {
			return lp.AggregateFunc(e, inputs, e.Distinct, nil)
		} else {
			return lp.ScalarFunc(e, inputs...)
		}
	//case *tree.ExpressionStringCompare:
	//	switch e.Operator {
	//	case tree.StringOperatorNotLike:
	//	}
	//case *tree.ExpressionBindParameter:
	//case *tree.ExpressionCollate:
	//case *tree.ExpressionIs:
	//case *tree.ExpressionList:
	//case *tree.ExpressionSelect:
	//case *tree.ExpressionBetween:
	//case *tree.ExpressionCase:
	default:
		panic("ToExpr: unknown expression type")
	}

	panic("unreachable")
}

func (q *queryPlanner) ToPlan(node *tree.SQLStatement) lp.LogicalPlan {
	return q.planStatement(node)
}

func (q *queryPlanner) planStatement(node *tree.SQLStatement) lp.LogicalPlan {
	return q.planStatementWithContext(node, NewPlannerContext())
}

func (q *queryPlanner) planStatementWithContext(node *tree.SQLStatement, ctx *PlannerContext) lp.LogicalPlan {
	if len(node.CTEs) > 0 {
		q.buildCTEs(node.CTEs, ctx)
	}

	switch n := node.SQL.(type) {
	case *tree.SelectStatement:
		return q.buildSelectStmt(n, ctx)
		//case *tree.Insert:
		//case *tree.Update:
		//case *tree.Delete:
	}
	return nil
}

// buildSelectStmt build a logical plan from a select statement.
// NOTE: we don't support nested select with CTE.
func (q *queryPlanner) buildSelectStmt(node *tree.SelectStatement, ctx *PlannerContext) lp.LogicalPlan {
	//if len(node.CTEs) > 0 {
	//	q.buildCTEs(node.CTEs, ctx)
	//}

	//return q.buildSelect(node.Stmt, ctx)

	var plan lp.LogicalPlan
	if len(node.SelectCores) > 1 { // set operation
		left := q.buildSelectCore(node.SelectCores[0], ctx)
		for i, rSelect := range node.SelectCores[1:] {
			// TODO: change AST tree to represent as left and right?
			//setOp := rSelect.Compound.Operator
			setOP := node.CompoundOperators[i]
			right := q.buildSelectCore(rSelect, ctx)
			switch setOP {
			case tree.CompoundOperatorUnion:
				plan = lp.Builder.From(left).Union(right).Distinct().Build()
			case tree.CompoundOperatorUnionAll:
				plan = lp.Builder.From(left).Union(right).Build()
			case tree.CompoundOperatorIntersect:
				plan = lp.Builder.From(left).Intersect(right).Build()
			case tree.CompoundOperatorExcept:
				plan = lp.Builder.From(left).Except(right).Build()
			default:
				panic(fmt.Sprintf("unknown set operation %s", setOP))
			}
			left = plan
		}
	} else { // plain select
		plan = q.buildSelectCore(node.SelectCores[0], ctx)
	}

	// NOTE: we don't support use index of an output column as sort_expression
	// only support column name or alias
	// actually, it's allowed in parser
	// TODO: support this? @brennan: thought?
	plan = q.buildOrderBy(plan, node.Ordering, nil, ctx)

	// TODO: change/unwrap tree.OrderBy,use []*tree.OrderingTerm directly ?
	plan = q.buildLimit(plan, node.Limit, node.Offset)
	return plan
}

func (q *queryPlanner) buildOrderBy(plan lp.LogicalPlan, nodes []*tree.OrderingTerm, schema *ds.Schema, ctx *PlannerContext) lp.LogicalPlan {
	if nodes == nil {
		return plan
	}

	// handle (select) distinct?

	//sortExprs := q.orderByToExprs(nodes, nil, ctx)

	sortExprs := make([]lp.LogicalExpr, 0, len(nodes))

	for _, order := range nodes {
		asc := order.Order != tree.OrderTypeDesc
		nullsFirst := order.Nulls == tree.NullOrderFirst
		sortExprs = append(sortExprs, lp.SortExpr(
			q.ToExpr(order.Expression, schema),
			asc, nullsFirst))
	}

	return lp.Builder.From(plan).Sort(sortExprs...).Build()
}

func (q *queryPlanner) buildLimit(plan lp.LogicalPlan, limit tree.Expression, offset tree.Expression) lp.LogicalPlan {
	if limit == nil {
		return plan
	}

	// TODO: change tree.Limit, use skip and fetch?

	var skip, fetch int64

	if offset != nil {
		switch t := offset.(type) {
		case *tree.ExpressionLiteral:
			offsetExpr := q.ToExpr(t, plan.Schema())
			e, ok := offsetExpr.(*lp.LiteralNumericExpr)
			if !ok {
				panic(fmt.Sprintf("unexpected offset expr %T", offsetExpr))
			}

			skip = e.Value

			if skip < 0 {
				panic(fmt.Sprintf("invalid offset value %v", skip))
			}
		default:
			panic(fmt.Sprintf("unexpected skip type %T", t))
		}
	}

	switch t := limit.(type) {
	case *tree.ExpressionLiteral:
		limitExpr := q.ToExpr(t, plan.Schema())
		e, ok := limitExpr.(*lp.LiteralNumericExpr)
		if !ok {
			panic(fmt.Sprintf("unexpected limit expr %T", limitExpr))
		}

		fetch = e.Value
	default:
		panic(fmt.Sprintf("unexpected limit type %T", t))
	}

	return lp.Builder.From(plan).Limit(skip, fetch).Build()
}

// buildSelectCore builds a logical plan for a simple select statement.
// The order of building is:
// 1. from
// 2. where
// 3. group by(can use reference from select)
// 4. having(can use reference from select)
// 5. select
// 6. distinct
// 7. order by, done in buildSelect
// 8. limit, done in buildSelect
func (q *queryPlanner) buildSelectCore(node *tree.SelectCore, ctx *PlannerContext) lp.LogicalPlan {
	var plan lp.LogicalPlan

	// from clause
	plan = q.buildFrom(node.From, node.Joins, ctx)

	noFrom := false
	if _, ok := plan.(*lp.NoRelationOp); ok {
		noFrom = true
	}

	// where clause
	// after this step, we got a schema(maybe combined from different tables) to work with
	sourcePlan := q.buildFilter(plan, node.Where, ctx)

	// try to qualify expr, also expand `*`
	projectExprs := q.prepareProjectionExprs(sourcePlan, node.Columns, noFrom, ctx)

	// aggregation
	planAfterAggr, projectedExpsAfterAggr, err :=
		q.buildAggregation(node.GroupBy, node.Having, sourcePlan, projectExprs, ctx)
	if err != nil {
		panic(err)
	}

	// projection after aggregate
	plan = q.buildProjection(planAfterAggr, projectedExpsAfterAggr)

	// distinct
	if node.Distinct {
		plan = lp.Builder.From(plan).Distinct().Build()
	}

	return plan
}

// buildFrom builds a logical plan for a from clause.
// NOTE: our AST uses `From` and `Joins` together represent a relation.
func (q *queryPlanner) buildFrom(node tree.Table, joins []*tree.Join, ctx *PlannerContext) lp.LogicalPlan {
	if node == nil {
		return lp.Builder.NoRelation().Build()
	}

	left := q.buildRelation(node, ctx)
	for _, join := range joins {
		right := q.buildRelation(join.Relation, ctx)
		joinSchema := left.Schema().Join(right.Schema())
		expr := q.ToExpr(join.On, joinSchema)
		left = lp.Builder.JoinOn(string(join.Type), left, expr).Build()
	}

	return left
}

func (q *queryPlanner) buildRelation(relation tree.Table, ctx *PlannerContext) lp.LogicalPlan {
	var left lp.LogicalPlan

	switch t := relation.(type) {
	case *tree.RelationTable:
		left = q.buildTableSource(t, ctx)
	case *tree.RelationSubquery:
		left = q.buildSelectStmt(t.Subquery, ctx)
	default:
		panic(fmt.Sprintf("unknown relation type %T", t))
	}

	return left
}

func (q *queryPlanner) buildCTEs(ctes []*tree.CommonTableExpression, ctx *PlannerContext) lp.LogicalPlan {
	for _, cte := range ctes {
		q.buildCTE(cte, ctx)
	}
	return nil
}

func (q *queryPlanner) buildCTE(cte *tree.CommonTableExpression, ctx *PlannerContext) lp.LogicalPlan {
	return nil
}

func (q *queryPlanner) buildTableSource(node *tree.RelationTable, ctx *PlannerContext) lp.LogicalPlan {
	//tableRef := ds.TableRefQualified(node.Schema, node.Name)   // TODO: handle schema
	tableRef := ds.TableRefQualified("", node.Table)

	// TODO: handle cte
	//relName := tableRef.String()
	//cte := ctx.GetCTE(relName)
	// return ctePlan

	schemaProvider, err := q.catalog.GetDataSource(tableRef)
	if err != nil {
		panic(err)
	}

	return lp.Builder.Scan(tableRef, schemaProvider).Build()
}

func (q *queryPlanner) buildFilter(plan lp.LogicalPlan, node tree.Expression, ctx *PlannerContext) lp.LogicalPlan {
	if node == nil {
		return plan
	}

	// TODO: handle parent schema

	expr := q.ToExpr(node, plan.Schema())
	expr = qualifyExpr(expr, plan.Schema())
	return lp.Builder.From(plan).Filter(expr).Build()
}

func (q *queryPlanner) buildProjection(plan lp.LogicalPlan, exprs []lp.LogicalExpr) lp.LogicalPlan {
	return lp.Builder.From(plan).Project(exprs...).Build()
}

func (q *queryPlanner) buildHaving(node tree.Expression, schema *ds.Schema,
	aliasMap map[string]lp.LogicalExpr, ctx *PlannerContext) lp.LogicalExpr {
	if node == nil {
		return nil
	}

	expr := q.ToExpr(node, schema)
	expr = resolveAlias(expr, aliasMap)
	expr = qualifyExpr(expr, schema)
	return expr
}

// buildAggregation builds a logical plan for an aggregate, returns the new plan
// and projected exprs.
func (q *queryPlanner) buildAggregation(groupBy []tree.Expression,
	having tree.Expression,
	sourcePlan lp.LogicalPlan, projectExprs []lp.LogicalExpr,
	ctx *PlannerContext) (lp.LogicalPlan, []lp.LogicalExpr, error) {
	if groupBy == nil {
		return sourcePlan, projectExprs, nil
	}

	aliasMap := extractAliases(projectExprs)
	// having/group_by may refer to the alias in select
	projectedPlan := q.buildProjection(sourcePlan, projectExprs)
	combinedSchema := sourcePlan.Schema().Clone().Merge(projectedPlan.Schema())

	havingExpr := q.buildHaving(having, combinedSchema, aliasMap, ctx)

	aggrExprs := slices.Clone(projectExprs) // shallow copy
	if havingExpr != nil {
		aggrExprs = append(aggrExprs, havingExpr)
	}
	aggrExprs = extractAggrExprs(aggrExprs)

	var groupByExprs []lp.LogicalExpr
	for _, gbExpr := range groupBy {
		groupByExpr := q.ToExpr(gbExpr, combinedSchema)

		// avoid conflict
		aliasMapClone := cloneAliases(aliasMap)
		for _, f := range sourcePlan.Schema().Fields {
			delete(aliasMapClone, f.Name)
		}

		groupByExpr = resolveAlias(groupByExpr, aliasMapClone)
		if err := ensureSchemaSatifiesExprs(combinedSchema, []lp.LogicalExpr{groupByExpr}); err != nil {
			panic(err)
			return nil, nil, fmt.Errorf("build aggregation: %w", err)
		}

		groupByExprs = append(groupByExprs, groupByExpr)
	}

	if len(groupByExprs) > 0 || len(aggrExprs) > 0 {
		planAfterAggr, projectedExpsAfterAggr := q.buildAggregate(
			sourcePlan, projectExprs, havingExpr, groupByExprs, aggrExprs)
		return planAfterAggr, projectedExpsAfterAggr, nil
	} else {
		if havingExpr != nil {
			return nil, nil, fmt.Errorf("build aggregation: having expression without group by")
		}
		return sourcePlan, projectExprs, nil
	}
}

// buildAggregate builds a logical plan for an aggregate.
// A typical aggregate plan has group by, having, and aggregate expressions.
func (q *queryPlanner) buildAggregate(input lp.LogicalPlan,
	projectedExprs []lp.LogicalExpr, havingExpr lp.LogicalExpr,
	groupByExprs, aggrExprs []lp.LogicalExpr) (lp.LogicalPlan, []lp.LogicalExpr) {
	plan := lp.Builder.From(input).Aggregate(groupByExprs, aggrExprs).Build()
	if p, ok := plan.(*lp.AggregateOp); ok {
		// rewrite projection to refer to columns that are output of aggregate plan.
		plan = p
		groupByExprs = p.GroupBy()
	} else {
		panic(fmt.Sprintf("unexpected plan type %T", plan))
	}

	// rewrite projection to refer to columns that are output of aggregate plan.
	// like replace a function call with a column
	aggrProjectionExprs := slices.Clone(groupByExprs)
	aggrProjectionExprs = append(aggrProjectionExprs, aggrExprs...)
	// resolve the columns in projection to qualified columns
	resolvedAggrProjectionExprs := make([]lp.LogicalExpr, len(aggrProjectionExprs))
	for i, expr := range aggrProjectionExprs {
		e := pt.TransformPostOrder(expr, func(n pt.TreeNode) pt.TreeNode {
			if c, ok := n.(*lp.ColumnExpr); ok {
				field := c.Resolve(plan.Schema())
				return lp.ColumnFromDefToExpr(field.QualifiedColumn())
			}
			return n
		})

		resolvedAggrProjectionExprs[i] = e.(lp.LogicalExpr)
	}
	// replace any expressions that are not a column with a column
	// like `1+2` or `group by a+b`(a,b are alias)
	var columnsAfterAggr []lp.LogicalExpr
	for _, expr := range resolvedAggrProjectionExprs {
		columnsAfterAggr = append(columnsAfterAggr, exprAsColumn(expr, plan))
	}
	fmt.Println("==================columnsAfterAggr: ", columnsAfterAggr)

	//
	// rewrite projection
	var projectedExprsAfterAggr []lp.LogicalExpr
	for _, expr := range projectedExprs {
		projectedExprsAfterAggr = append(projectedExprsAfterAggr,
			rebaseExprs(expr, resolvedAggrProjectionExprs, plan))
	}
	// make sure projection exprs can be resolved from columns

	if err := checkExprsProjectFromColumns(projectedExprsAfterAggr,
		columnsAfterAggr); err != nil {
		panic(fmt.Sprintf("build aggregation: %s", err))
	}

	if havingExpr != nil {
		havingExpr = rebaseExprs(havingExpr, resolvedAggrProjectionExprs, plan)
		if err := checkExprsProjectFromColumns(
			[]lp.LogicalExpr{havingExpr}, columnsAfterAggr); err != nil {
			panic(fmt.Sprintf("build aggregation: %s", err))
		}

		plan = lp.Builder.From(plan).Project(havingExpr).Build()
	}

	return plan, projectedExprsAfterAggr
}

func (q *queryPlanner) prepareProjectionExprs(plan lp.LogicalPlan, node []tree.ResultColumn, noFrom bool, ctx *PlannerContext) []lp.LogicalExpr {
	var exprs []lp.LogicalExpr
	for _, col := range node {
		exprs = append(exprs, q.projectColumnToExpr(col, plan, noFrom, ctx)...)
	}
	return exprs
}

func (q *queryPlanner) projectColumnToExpr(col tree.ResultColumn,
	plan lp.LogicalPlan, noFrom bool, ctx *PlannerContext) []lp.LogicalExpr {
	localSchema := plan.Schema()
	switch t := col.(type) {
	case *tree.ResultColumnExpression: // single column
		expr := q.ToExpr(t.Expression, localSchema)
		column := qualifyExpr(expr, localSchema)
		if t.Alias != "" { // only add alias if it's not the same as column name
			if c, ok := column.(*lp.ColumnExpr); ok {
				if c.Name != t.Alias {
					column = lp.Alias(column, t.Alias)
				}
			}
		}
		return []lp.LogicalExpr{column}
	case *tree.ResultColumnWildcard: // expand *
		if noFrom {
			panic("cannot use * in select list without FROM clause")
		}

		if t.Table == "" { // *
			return expandStar(localSchema)
		} else { // table.*
			return expandQualifiedStar(localSchema, t.Table)
		}
	default:
		panic(fmt.Sprintf("unknown result column type %T", t))
	}
}
