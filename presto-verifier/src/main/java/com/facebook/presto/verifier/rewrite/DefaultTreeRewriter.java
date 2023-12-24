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
package com.facebook.presto.verifier.rewrite;

import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.Analyze;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.CallArgument;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.RefreshMaterializedView;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Return;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableElement;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * A default implementation of {@link AstVisitor} that reconstructs a node if any of its children is reconstructed.
 * Expression node reconstruction is not supported and left to the users. At the moment it is only used for presto.verifier package.
 * Generalize it with caution.
 */
public class DefaultTreeRewriter<C>
        extends AstVisitor<Node, C>
{
    @Override
    protected Node visitNode(Node node, C context)
    {
        return node;
    }

    @Override
    protected Node visitExpression(Expression node, C context)
    {
        throw new UnsupportedOperationException("Not yet implemented: " + getClass().getSimpleName() + " for " + node.getClass().getName());
    }

    @Override
    protected Node visitAddColumn(AddColumn node, C context)
    {
        Node column = process(node.getColumn(), context);
        if (node.getColumn() == column) {
            return node;
        }

        return new AddColumn(node.getName(), (ColumnDefinition) column, node.isTableExists(), node.isColumnNotExists());
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, C context)
    {
        Node relation = process(node.getRelation(), context);
        Node alias = process(node.getAlias(), context);
        List<Identifier> columnNames = process(node.getColumnNames(), context);
        if (node.getRelation() == relation && node.getAlias() == alias && sameElements(node.getColumnNames(), columnNames)) {
            return node;
        }

        return new AliasedRelation((Relation) relation, (Identifier) alias, columnNames);
    }

    @Override
    protected Node visitAnalyze(Analyze node, C context)
    {
        List<Property> properties = process(node.getProperties(), context);
        if (sameElements(node.getProperties(), properties)) {
            return node;
        }

        return new Analyze(node.getTableName(), properties);
    }

    @Override
    protected Node visitCall(Call node, C context)
    {
        List<CallArgument> arguments = process(node.getArguments(), context);
        if (sameElements(node.getArguments(), arguments)) {
            return node;
        }

        return new Call(node.getName(), arguments);
    }

    @Override
    protected Node visitCallArgument(CallArgument node, C context)
    {
        Node value = process(node.getValue(), context);
        if (node.getValue() == value) {
            return node;
        }

        return node.getName().isPresent() ? new CallArgument(node.getName().get(), (Expression) value) : new CallArgument((Expression) value);
    }

    @Override
    protected Node visitColumnDefinition(ColumnDefinition node, C context)
    {
        Node name = process(node.getName(), context);
        List<Property> properties = process(node.getProperties(), context);
        if (node.getName() == name && sameElements(node.getProperties(), properties)) {
            return node;
        }

        return new ColumnDefinition((Identifier) name, node.getType(), node.isNullable(), properties, node.getComment());
    }

    @Override
    protected Node visitCreateMaterializedView(CreateMaterializedView node, C context)
    {
        Node query = process(node.getQuery(), context);
        List<Property> properties = process(node.getProperties(), context);
        if (node.getQuery() == query && node.getProperties() == properties) {
            return node;
        }

        return new CreateMaterializedView(node.getName(), (Query) query, node.isNotExists(), properties, node.getComment());
    }

    @Override
    protected Node visitCreateSchema(CreateSchema node, C context)
    {
        List<Property> properties = process(node.getProperties(), context);
        if (sameElements(node.getProperties(), properties)) {
            return node;
        }

        return new CreateSchema(node.getSchemaName(), node.isNotExists(), properties);
    }

    @Override
    protected Node visitCreateTable(CreateTable node, C context)
    {
        List<TableElement> elements = process(node.getElements(), context);
        List<Property> properties = process(node.getProperties(), context);
        if (sameElements(node.getElements(), elements) && sameElements(node.getProperties(), properties)) {
            return node;
        }

        return new CreateTable(node.getName(), elements, node.isNotExists(), properties, node.getComment());
    }

    @Override
    protected Node visitCreateTableAsSelect(CreateTableAsSelect node, C context)
    {
        Node query = process(node.getQuery(), context);
        List<Property> properties = process(node.getProperties(), context);
        Optional<List<Identifier>> columnAliases = node.getColumnAliases().map(aliases -> process(aliases, context));
        if (node.getQuery() == query && node.getProperties() == properties && (!columnAliases.isPresent() || sameElements(node.getColumnAliases().get(), columnAliases.get()))) {
            return node;
        }

        return new CreateTableAsSelect(node.getName(), (Query) query, node.isNotExists(), properties, node.isWithData(), node.getColumnAliases(), node.getComment());
    }

    @Override
    protected Node visitCreateView(CreateView node, C context)
    {
        Node query = process(node.getQuery(), context);
        if (node.getQuery() == query) {
            return node;
        }

        return new CreateView(node.getName(), (Query) query, node.isReplace(), node.getSecurity());
    }

    @Override
    protected Node visitCube(Cube node, C context)
    {
        List<Expression> expressions = process(node.getExpressions(), context);
        if (sameElements(node.getExpressions(), expressions)) {
            return node;
        }

        return new Cube(expressions);
    }

    @Override
    protected Node visitDeallocate(Deallocate node, C context)
    {
        Node name = process(node.getName(), context);
        if (node.getName() == name) {
            return node;
        }

        return new Deallocate((Identifier) name);
    }

    @Override
    protected Node visitDelete(Delete node, C context)
    {
        Node table = process(node.getTable(), context);
        Optional<Expression> where = process(node.getWhere(), context);
        if (node.getTable() == table && sameElement(node.getWhere(), where)) {
            return node;
        }

        return new Delete((Table) table, where);
    }

    @Override
    protected Node visitExcept(Except node, C context)
    {
        Node left = process(node.getLeft(), context);
        Node right = process(node.getRight(), context);
        if (node.getLeft() == left && node.getRight() == right) {
            return node;
        }

        return new Except((Relation) left, (Relation) right, node.isDistinct());
    }

    @Override
    protected Node visitExecute(Execute node, C context)
    {
        Node name = process(node.getName(), context);
        List<Expression> parameters = process(node.getParameters(), context);
        if (node.getName() == name && sameElements(node.getParameters(), parameters)) {
            return node;
        }

        return new Execute(node.getName(), parameters);
    }

    @Override
    protected Node visitExplain(Explain node, C context)
    {
        Node statement = process(node.getStatement(), context);
        if (node.getStatement() == statement) {
            return node;
        }

        return new Explain((Statement) statement, node.isAnalyze(), node.isVerbose(), node.getOptions());
    }

    @Override
    protected Node visitFrameBound(FrameBound node, C context)
    {
        Optional<Expression> value = process(node.getValue(), context);
        if (sameElement(node.getValue(), value)) {
            return node;
        }

        return value.isPresent() ? new FrameBound(node.getType(), value.get()) : new FrameBound(node.getType());
    }

    @Override
    protected Node visitGroupBy(GroupBy node, C context)
    {
        List<GroupingElement> groupingElements = process(node.getGroupingElements(), context);
        if (sameElements(node.getGroupingElements(), groupingElements)) {
            return node;
        }

        return new GroupBy(node.isDistinct(), groupingElements);
    }

    @Override
    protected Node visitGroupingSets(GroupingSets node, C context)
    {
        List<List<Expression>> sets = node.getSets().stream().map(expressionList -> process(expressionList, context)).collect(ImmutableList.toImmutableList());

        if (sameElements(node.getSets(), sets)) {
            return node;
        }

        return new GroupingSets(sets);
    }

    @Override
    protected Node visitInsert(Insert node, C context)
    {
        Node query = process(node.getQuery(), context);
        Optional<List<Identifier>> columns = node.getColumns().map(columnList -> process(columnList, context));
        if (node.getQuery() == query && (!columns.isPresent() || sameElements(node.getColumns().get(), columns.get()))) {
            return node;
        }

        return new Insert(node.getTarget(), columns, (Query) query);
    }

    @Override
    protected Node visitIntersect(Intersect node, C context)
    {
        List<Relation> relations = process(node.getRelations(), context);
        if (sameElements(node.getRelations(), relations)) {
            return node;
        }

        return new Intersect(relations, node.isDistinct());
    }

    @Override
    protected Node visitJoin(Join node, C context)
    {
        Node left = process(node.getLeft(), context);
        Node right = process(node.getRight(), context);
        Optional<JoinCriteria> joinCriteria = node.getCriteria()
                .map(criteria -> {
                    if (criteria instanceof JoinOn) {
                        Node expression = process(((JoinOn) criteria).getExpression(), context);
                        if (((JoinOn) criteria).getExpression() == expression) {
                            return criteria;
                        }
                        return new JoinOn((Expression) expression);
                    }
                    return criteria;
                });
        if (node.getLeft() == left && node.getRight() == right && node.getCriteria() == joinCriteria) {
            return node;
        }

        return new Join(node.getType(), (Relation) left, (Relation) right, joinCriteria);
    }

    @Override
    protected Node visitLateral(Lateral node, C context)
    {
        Node query = process(node.getQuery(), context);
        if (node.getQuery() == query) {
            return node;
        }

        return new Lateral((Query) query);
    }

    @Override
    protected Node visitOrderBy(OrderBy node, C context)
    {
        List<SortItem> sortItems = process(node.getSortItems(), context);
        if (sameElements(node.getSortItems(), sortItems)) {
            return node;
        }

        return new OrderBy(sortItems);
    }

    @Override
    protected Node visitPrepare(Prepare node, C context)
    {
        Node statement = process(node.getStatement(), context);
        if (node.getStatement() == statement) {
            return node;
        }

        return new Prepare(node.getName(), (Statement) statement);
    }

    @Override
    protected Node visitProperty(Property node, C context)
    {
        Node name = process(node.getName(), context);
        Node value = process(node.getValue(), context);
        if (node.getName() == name && node.getValue() == value) {
            return node;
        }

        return new Property((Identifier) name, (Expression) value);
    }

    @Override
    protected Node visitQuery(Query node, C context)
    {
        Optional<With> with = process(node.getWith(), context);
        Node queryBody = process(node.getQueryBody(), context);
        Optional<OrderBy> orderBy = process(node.getOrderBy(), context);
        if (node.getQueryBody() == queryBody && sameElement(node.getWith(), with) && sameElement(node.getOrderBy(), orderBy)) {
            return node;
        }

        return new Query(with, (QueryBody) queryBody, orderBy, node.getOffset(), node.getLimit());
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, C context)
    {
        Node select = process(node.getSelect(), context);
        Optional<Relation> from = process(node.getFrom(), context);
        Optional<Expression> where = process(node.getWhere(), context);
        Optional<GroupBy> groupBy = process(node.getGroupBy(), context);
        Optional<Expression> having = process(node.getHaving(), context);
        Optional<OrderBy> orderBy = process(node.getOrderBy(), context);
        if (node.getSelect() ==
                select && sameElement(node.getFrom(), from) && sameElement(node.getWhere(), where) && sameElement(node.getGroupBy(), groupBy) && sameElement(node.getHaving(),
                having) && sameElement(node.getOrderBy(), orderBy)) {
            return node;
        }

        return new QuerySpecification(
                (Select) select,
                from,
                where,
                groupBy,
                having,
                orderBy,
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitRefreshMaterializedView(RefreshMaterializedView node, C context)
    {
        Node table = process(node.getTarget(), context);
        Node where = process(node.getWhere(), context);
        if (node.getTarget() == table && node.getWhere() == where) {
            return node;
        }

        return new RefreshMaterializedView((Table) table, (Expression) where);
    }

    @Override
    protected Node visitReturn(Return node, C context)
    {
        Node expression = process(node.getExpression(), context);
        if (node.getExpression() == expression) {
            return node;
        }

        return new Return((Expression) expression);
    }

    @Override
    protected Node visitRollup(Rollup node, C context)
    {
        List<Expression> expressions = process(node.getExpressions(), context);
        if (sameElements(node.getExpressions(), expressions)) {
            return node;
        }

        return new Rollup(expressions);
    }

    @Override
    protected Node visitRow(Row node, C context)
    {
        List<Expression> items = process(node.getItems(), context);
        if (sameElements(node.getItems(), items)) {
            return node;
        }

        return new Row(items);
    }

    @Override
    protected Node visitSampledRelation(SampledRelation node, C context)
    {
        Node relation = process(node.getRelation(), context);
        Node samplePercentage = process(node.getSamplePercentage(), context);
        if (node.getRelation() == relation && node.getSamplePercentage() == samplePercentage) {
            return node;
        }

        return new SampledRelation((Relation) relation, node.getType(), (Expression) samplePercentage);
    }

    @Override
    protected Node visitSelect(Select node, C context)
    {
        List<SelectItem> selectItems = process(node.getSelectItems(), context);
        if (sameElements(node.getSelectItems(), selectItems)) {
            return node;
        }

        return new Select(node.isDistinct(), selectItems);
    }

    @Override
    protected Node visitShowStats(ShowStats node, C context)
    {
        Node relation = process(node.getRelation(), context);
        if (node.getRelation() == relation) {
            return node;
        }

        return new ShowStats((Relation) relation);
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, C context)
    {
        List<Expression> columns = process(node.getExpressions(), context);
        if (sameElements(node.getExpressions(), columns)) {
            return node;
        }

        return new SimpleGroupBy(columns);
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, C context)
    {
        Node expression = process(node.getExpression(), context);
        if (node.getExpression() == expression) {
            return node;
        }

        return new SingleColumn((Expression) expression, node.getAlias());
    }

    @Override
    protected Node visitSortItem(SortItem node, C context)
    {
        Node sortKey = process(node.getSortKey(), context);
        if (node.getSortKey() == sortKey) {
            return node;
        }

        return new SortItem((Expression) sortKey, node.getOrdering(), node.getNullOrdering());
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, C context)
    {
        Node query = process(node.getQuery(), context);
        if (node.getQuery() == query) {
            return node;
        }

        return new TableSubquery((Query) query);
    }

    @Override
    protected Node visitUnion(Union node, C context)
    {
        List<Relation> relations = process(node.getRelations(), context);
        if (sameElements(node.getRelations(), relations)) {
            return node;
        }

        return new Union(relations, node.isDistinct());
    }

    @Override
    protected Node visitUnnest(Unnest node, C context)
    {
        List<Expression> expressions = process(node.getExpressions(), context);
        if (sameElements(node.getExpressions(), expressions)) {
            return node;
        }

        return new Unnest(expressions, node.isWithOrdinality());
    }

    @Override
    protected Node visitValues(Values node, C context)
    {
        List<Expression> expressions = process(node.getRows(), context);
        if (sameElements(node.getRows(), expressions)) {
            return node;
        }

        return new Values(expressions);
    }

    @Override
    protected Node visitWindow(Window node, C context)
    {
        List<Expression> partitionBy = process(node.getPartitionBy(), context);
        Optional<OrderBy> orderBy = process(node.getOrderBy(), context);
        Optional<WindowFrame> frame = process(node.getFrame(), context);
        if (sameElements(node.getPartitionBy(), partitionBy) && sameElement(node.getOrderBy(), orderBy) && sameElement(node.getFrame(), frame)) {
            return node;
        }

        return new Window(partitionBy, orderBy, frame);
    }

    @Override
    protected Node visitWindowFrame(WindowFrame node, C context)
    {
        Node start = process(node.getStart(), context);
        Optional<FrameBound> end = process(node.getEnd(), context);
        if (node.getStart() == start && sameElement(node.getEnd(), end)) {
            return node;
        }

        return new WindowFrame(node.getType(), (FrameBound) start, end);
    }

    @Override
    protected Node visitWith(With node, C context)
    {
        List<WithQuery> queries = process(node.getQueries(), context);
        if (sameElements(node.getQueries(), queries)) {
            return node;
        }

        return new With(node.isRecursive(), queries);
    }

    @Override
    protected Node visitWithQuery(WithQuery node, C context)
    {
        Node name = process(node.getName(), context);
        Node query = process(node.getQuery(), context);
        Optional<List<Identifier>> columnNames = node.getColumnNames().map(columnNamesList -> process(columnNamesList, context));
        if (node.getName() == name && node.getQuery() == query && sameElement(node.getColumnNames(), columnNames)) {
            return node;
        }

        return new WithQuery(node.getName(), (Query) query, node.getColumnNames());
    }

    private <T extends Node> List<T> process(List<T> elements, C context)
    {
        if (elements == null) {
            return null;
        }
        List<T> result = elements.stream().map(element -> (T) process(element, context)).collect(ImmutableList.toImmutableList());
        return sameElements(elements, result) ? elements : result;
    }

    private <T extends Node> Optional<T> process(Optional<T> element, C context)
    {
        if (element == null) {
            return null;
        }
        Optional<T> result = element.map(e -> (T) process(e, context));
        return sameElement(element, result) ? element : result;
    }

    private static <T> boolean sameElement(Optional<T> a, Optional<T> b)
    {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (!a.isPresent() && !b.isPresent()) {
            return true;
        }
        else if (a.isPresent() != b.isPresent()) {
            return false;
        }

        return a.get() == b.get();
    }

    @SuppressWarnings("ObjectEquality")
    private static <T> boolean sameElements(Iterable<? extends T> a, Iterable<? extends T> b)
    {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }

        if (Iterables.size(a) != Iterables.size(b)) {
            return false;
        }

        Iterator<? extends T> first = a.iterator();
        Iterator<? extends T> second = b.iterator();

        while (first.hasNext() && second.hasNext()) {
            if (first.next() != second.next()) {
                return false;
            }
        }

        return true;
    }
}
