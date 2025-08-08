/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.tree;

public abstract class DefaultTraversalVisitor<R, C>
        extends AstVisitor<R, C>
{
    @Override
    protected R visitExtract(Extract node, C context)
    {
        return process(node.getExpression(), context);
    }

    @Override
    protected R visitCast(Cast node, C context)
    {
        return process(node.getExpression(), context);
    }

    @Override
    protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitBetweenPredicate(BetweenPredicate node, C context)
    {
        process(node.getValue(), context);
        process(node.getMin(), context);
        process(node.getMax(), context);

        return null;
    }

    @Override
    protected R visitCoalesceExpression(CoalesceExpression node, C context)
    {
        node.getOperands().forEach(operand -> process(operand, context));

        return null;
    }

    @Override
    protected R visitAtTimeZone(AtTimeZone node, C context)
    {
        process(node.getValue(), context);
        process(node.getTimeZone(), context);

        return null;
    }

    @Override
    protected R visitArrayConstructor(ArrayConstructor node, C context)
    {
        node.getValues().forEach(expression -> process(expression, context));

        return null;
    }

    @Override
    protected R visitSubscriptExpression(SubscriptExpression node, C context)
    {
        process(node.getBase(), context);
        process(node.getIndex(), context);

        return null;
    }

    @Override
    protected R visitComparisonExpression(ComparisonExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitQuery(Query node, C context)
    {
        node.getWith().ifPresent(with -> process(with, context));

        process(node.getQueryBody(), context);

        node.getOrderBy().ifPresent(orderBy -> process(orderBy, context));

        return null;
    }

    @Override
    protected R visitWith(With node, C context)
    {
        node.getQueries().forEach(query -> process(query, context));

        return null;
    }

    @Override
    protected R visitWithQuery(WithQuery node, C context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitSelect(Select node, C context)
    {
        node.getSelectItems().forEach(item -> process(item, context));

        return null;
    }

    @Override
    protected R visitSingleColumn(SingleColumn node, C context)
    {
        process(node.getExpression(), context);

        return null;
    }

    @Override
    protected R visitWhenClause(WhenClause node, C context)
    {
        process(node.getOperand(), context);
        process(node.getResult(), context);

        return null;
    }

    @Override
    protected R visitInPredicate(InPredicate node, C context)
    {
        process(node.getValue(), context);
        process(node.getValueList(), context);

        return null;
    }

    @Override
    protected R visitFunctionCall(FunctionCall node, C context)
    {
        node.getArguments().forEach(argument -> process(argument, context));

        node.getOrderBy().ifPresent(orderBy -> process(orderBy, context));
        node.getWindow().ifPresent(window -> process(window, context));
        node.getFilter().ifPresent(filter -> process(filter, context));

        return null;
    }

    @Override
    protected R visitGroupingOperation(GroupingOperation node, C context)
    {
        node.getGroupingColumns().forEach(columnArgument -> process(columnArgument, context));

        return null;
    }

    @Override
    protected R visitDereferenceExpression(DereferenceExpression node, C context)
    {
        process(node.getBase(), context);
        return null;
    }

    @Override
    public R visitWindow(Window node, C context)
    {
        node.getPartitionBy().forEach(expression -> process(expression, context));

        node.getOrderBy().ifPresent(orderBy -> process(orderBy, context));
        node.getFrame().ifPresent(frame -> process(frame, context));

        return null;
    }

    @Override
    public R visitWindowFrame(WindowFrame node, C context)
    {
        process(node.getStart(), context);
        node.getEnd().ifPresent(end -> process(end, context));

        return null;
    }

    @Override
    public R visitFrameBound(FrameBound node, C context)
    {
        node.getValue().ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context)
    {
        process(node.getOperand(), context);

        node.getWhenClauses().forEach(clause -> process(clause, context));

        node.getDefaultValue()
                .ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected R visitInListExpression(InListExpression node, C context)
    {
        node.getValues().forEach(value -> process(value, context));

        return null;
    }

    @Override
    protected R visitNullIfExpression(NullIfExpression node, C context)
    {
        process(node.getFirst(), context);
        process(node.getSecond(), context);

        return null;
    }

    @Override
    protected R visitIfExpression(IfExpression node, C context)
    {
        process(node.getCondition(), context);
        process(node.getTrueValue(), context);
        node.getFalseValue().ifPresent(falseValue -> process(falseValue, context));

        return null;
    }

    @Override
    protected R visitTryExpression(TryExpression node, C context)
    {
        process(node.getInnerExpression(), context);
        return null;
    }

    @Override
    protected R visitBindExpression(BindExpression node, C context)
    {
        node.getValues().forEach(value -> process(value, context));
        process(node.getFunction(), context);

        return null;
    }

    @Override
    protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitNotExpression(NotExpression node, C context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context)
    {
        node.getWhenClauses().forEach(clause -> process(clause, context));
        node.getDefaultValue()
                .ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected R visitLikePredicate(LikePredicate node, C context)
    {
        process(node.getValue(), context);
        process(node.getPattern(), context);
        node.getEscape().ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitIsNullPredicate(IsNullPredicate node, C context)
    {
        return process(node.getValue(), context);
    }

    @Override
    protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected R visitSubqueryExpression(SubqueryExpression node, C context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitOrderBy(OrderBy node, C context)
    {
        node.getSortItems().forEach(sortItem -> process(sortItem, context));
        return null;
    }

    @Override
    protected R visitSortItem(SortItem node, C context)
    {
        return process(node.getSortKey(), context);
    }

    @Override
    protected R visitQuerySpecification(QuerySpecification node, C context)
    {
        process(node.getSelect(), context);
        node.getFrom().ifPresent(from -> process(from, context));
        node.getWhere().ifPresent(where -> process(where, context));
        node.getGroupBy().ifPresent(groupBy -> process(groupBy, context));
        node.getHaving().ifPresent(having -> process(having, context));
        node.getOrderBy().ifPresent(orderBy -> process(orderBy, context));

        return null;
    }

    @Override
    protected R visitSetOperation(SetOperation node, C context)
    {
        node.getRelations().forEach(relation -> process(relation, context));
        return null;
    }

    @Override
    protected R visitValues(Values node, C context)
    {
        node.getRows().forEach(row -> process(row, context));
        return null;
    }

    @Override
    protected R visitRow(Row node, C context)
    {
        node.getItems().forEach(expression -> process(expression, context));
        return null;
    }

    @Override
    protected R visitTableSubquery(TableSubquery node, C context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitAliasedRelation(AliasedRelation node, C context)
    {
        return process(node.getRelation(), context);
    }

    @Override
    protected R visitSampledRelation(SampledRelation node, C context)
    {
        process(node.getRelation(), context);
        process(node.getSamplePercentage(), context);
        return null;
    }

    @Override
    protected R visitJoin(Join node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        node.getCriteria()
                .filter(criteria -> criteria instanceof JoinOn)
                .map(criteria -> process(((JoinOn) criteria).getExpression(), context));

        return null;
    }

    @Override
    protected R visitUnnest(Unnest node, C context)
    {
        node.getExpressions().forEach(expression -> process(expression, context));

        return null;
    }

    @Override
    protected R visitGroupBy(GroupBy node, C context)
    {
        node.getGroupingElements().forEach(groupingElement -> process(groupingElement, context));

        return null;
    }

    @Override
    protected R visitCube(Cube node, C context)
    {
        return null;
    }

    @Override
    protected R visitRollup(Rollup node, C context)
    {
        return null;
    }

    @Override
    protected R visitSimpleGroupBy(SimpleGroupBy node, C context)
    {
        node.getExpressions().forEach(expression -> process(expression, context));

        return null;
    }

    @Override
    protected R visitGroupingSets(GroupingSets node, C context)
    {
        return null;
    }

    @Override
    protected R visitInsert(Insert node, C context)
    {
        process(node.getQuery(), context);

        return null;
    }

    @Override
    protected R visitDelete(Delete node, C context)
    {
        process(node.getTable(), context);
        node.getWhere().ifPresent(where -> process(where, context));

        return null;
    }

    protected R visitUpdate(Update node, C context)
    {
        process(node.getTable(), context);
        node.getAssignments().forEach(value -> process(value, context));
        node.getWhere().ifPresent(where -> process(where, context));

        return null;
    }

    protected R visitUpdateAssignment(UpdateAssignment node, C context)
    {
        process(node.getName(), context);
        process(node.getValue(), context);
        return null;
    }

    @Override
    protected R visitMerge(Merge node, C context)
    {
        process(node.getTarget(), context);
        process(node.getSource(), context);
        process(node.getPredicate(), context);
        node.getMergeCases().forEach(mergeCase -> process(mergeCase, context));
        return null;
    }

    @Override
    protected R visitMergeInsert(MergeInsert node, C context)
    {
        node.getColumns().forEach(column -> process(column, context));
        node.getValues().forEach(expression -> process(expression, context));
        return null;
    }

    @Override
    protected R visitMergeUpdate(MergeUpdate node, C context)
    {
        node.getAssignments().forEach(assignment -> {
            process(assignment.getTarget(), context);
            process(assignment.getValue(), context);
        });
        return null;
    }

    @Override
    protected R visitCreateTableAsSelect(CreateTableAsSelect node, C context)
    {
        process(node.getQuery(), context);
        node.getProperties().forEach(property -> process(property, context));

        return null;
    }

    @Override
    protected R visitProperty(Property node, C context)
    {
        process(node.getName(), context);
        process(node.getValue(), context);

        return null;
    }

    @Override
    protected R visitAnalyze(Analyze node, C context)
    {
        node.getProperties().forEach(property -> process(property, context));

        return null;
    }

    @Override
    protected R visitCreateView(CreateView node, C context)
    {
        process(node.getQuery(), context);

        return null;
    }

    @Override
    protected R visitCreateMaterializedView(CreateMaterializedView node, C context)
    {
        process(node.getQuery(), context);
        node.getProperties().forEach(property -> process(property, context));

        return null;
    }

    @Override
    protected R visitRefreshMaterializedView(RefreshMaterializedView node, C context)
    {
        process(node.getTarget(), context);
        process(node.getWhere(), context);

        return null;
    }

    @Override
    protected R visitSetSession(SetSession node, C context)
    {
        process(node.getValue(), context);

        return null;
    }

    @Override
    protected R visitAddColumn(AddColumn node, C context)
    {
        process(node.getColumn(), context);

        return null;
    }

    @Override
    protected R visitCreateTable(CreateTable node, C context)
    {
        node.getElements().forEach(tableElement -> process(tableElement, context));
        node.getProperties().forEach(property -> process(property, context));

        return null;
    }

    @Override
    protected R visitStartTransaction(StartTransaction node, C context)
    {
        node.getTransactionModes().forEach(transactionMode -> process(transactionMode, context));

        return null;
    }

    @Override
    protected R visitExplain(Explain node, C context)
    {
        process(node.getStatement(), context);

        node.getOptions().forEach(option -> process(option, context));

        return null;
    }

    @Override
    protected R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context)
    {
        process(node.getValue(), context);
        process(node.getSubquery(), context);

        return null;
    }

    @Override
    protected R visitExists(ExistsPredicate node, C context)
    {
        process(node.getSubquery(), context);

        return null;
    }

    @Override
    protected R visitLateral(Lateral node, C context)
    {
        process(node.getQuery(), context);

        return super.visitLateral(node, context);
    }

    @Override
    protected R visitRevokeRoles(RevokeRoles node, C context)
    {
        node.getRoles().forEach(property -> process(property, context));
        return null;
    }

    @Override
    protected R visitCreateType(CreateType node, C context)
    {
        return null;
    }
}
