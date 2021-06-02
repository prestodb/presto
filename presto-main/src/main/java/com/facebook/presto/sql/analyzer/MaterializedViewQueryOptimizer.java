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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class MaterializedViewQueryOptimizer
        extends AstVisitor<Node, MaterializedViewQueryOptimizer.MaterializedViewQueryOptimizerContext>
{
    @Override
    public Node process(Node node, MaterializedViewQueryOptimizerContext context)
    {
        return super.process(node, context);
    }

    @Override
    public Node visitNode(Node node, MaterializedViewQueryOptimizerContext context)
    {
        return node;
    }

    @Override
    protected Node visitQuery(Query node, MaterializedViewQueryOptimizerContext context)
    {
        return new Query(
                node.getWith(),
                (QueryBody) process(node.getQueryBody(), context),
                node.getOrderBy(),
                node.getLimit());
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, MaterializedViewQueryOptimizerContext context)
    {
        return new TableSubquery((Query) process(node.getQuery(), context));
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, MaterializedViewQueryOptimizerContext context)
    {
        // Process getFrom before all others to acquire alias relation if any
        Optional<Relation> rewriteFrom = node.getFrom().isPresent() ? Optional.of((Relation) process(node.getFrom().get(), context)) : Optional.empty();
        Select rewriteSelect = (Select) process(node.getSelect(), context);
        Optional<Expression> rewriteWhere = node.getWhere().isPresent() ? Optional.of((Expression) process(node.getWhere().get(), context)) : Optional.empty();
        Optional<GroupBy> rewriteGroupBy = node.getGroupBy().isPresent() ? Optional.of((GroupBy) process(node.getGroupBy().get(), context)) : Optional.empty();
        Optional<Expression> rewriteHaving = node.getHaving().isPresent() ? Optional.of((Expression) process(node.getHaving().get(), context)) : Optional.empty();
        Optional<OrderBy> rewriteOrderBy = node.getOrderBy().isPresent() ? Optional.of((OrderBy) process(node.getOrderBy().get(), context)) : Optional.empty();

        return new QuerySpecification(
                rewriteSelect,
                rewriteFrom,
                rewriteWhere,
                rewriteGroupBy,
                rewriteHaving,
                rewriteOrderBy,
                node.getLimit());
    }

    @Override
    protected Node visitSelect(Select node, MaterializedViewQueryOptimizerContext context)
    {
        ImmutableList.Builder<SelectItem> rewriteSelectItems = ImmutableList.builder();

        for (SelectItem selectItem : node.getSelectItems()) {
            SelectItem rewriteSelectItem = (SelectItem) process(selectItem, context);
            rewriteSelectItems.add(rewriteSelectItem);
        }

        Select viewSelect = new Select(node.isDistinct(), rewriteSelectItems.build());

        return viewSelect;
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, MaterializedViewQueryOptimizerContext context)
    {
        Expression selectExpression = node.getExpression();
        return new SingleColumn(
                (Expression) process(selectExpression, context),
                node.getAlias());
    }

    @Override
    protected Node visitAllColumns(AllColumns node, MaterializedViewQueryOptimizerContext context)
    {
        return node;
    }

    @Override
    protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, MaterializedViewQueryOptimizerContext context)
    {
        Expression rewriteLeft = (Expression) process(node.getLeft(), context);
        Expression rewriteRight = (Expression) process(node.getRight(), context);
        return new ArithmeticBinaryExpression(
                node.getOperator(),
                rewriteLeft,
                rewriteRight);
    }

    @Override
    protected Node visitIdentifier(Identifier node, MaterializedViewQueryOptimizerContext context)
    {
        String baseColumnName = node.getValue();
        return new Identifier(context.getViewColumnName(baseColumnName), node.isDelimited());
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, MaterializedViewQueryOptimizerContext context)
    {
        String functionCall = node.toString();
        ImmutableList.Builder<Expression> rewriteArguments = ImmutableList.builder();

        if (context.containsColumnName(functionCall)) {
            Expression derivedExpression = new Identifier(context.getViewColumnName(functionCall));
            rewriteArguments.add(derivedExpression);
        }
        else {
            for (Expression argument : node.getArguments()) {
                Expression rewriteArgument = (Expression) process(argument, context);
                rewriteArguments.add(rewriteArgument);
            }
        }

        return new FunctionCall(
                node.getName(),
                node.getWindow(),
                node.getFilter(),
                node.getOrderBy(),
                node.isDistinct(),
                node.isIgnoreNulls(),
                rewriteArguments.build());
    }

    // Assuming the current base query applies to this specific materialized view
    @Override
    protected Node visitRelation(Relation node, MaterializedViewQueryOptimizerContext context)
    {
        return context.getMaterializedViewTable();
    }

    @Override
    protected Node visitTable(Table node, MaterializedViewQueryOptimizerContext context)
    {
        return context.getMaterializedViewTable();
    }

    @Override
    protected Node visitLogicalBinaryExpression(LogicalBinaryExpression node, MaterializedViewQueryOptimizerContext context)
    {
        Expression rewriteLeft = (Expression) process(node.getLeft(), context);
        Expression rewriteRight = (Expression) process(node.getRight(), context);
        return new LogicalBinaryExpression(
                node.getOperator(),
                rewriteLeft,
                rewriteRight);
    }

    @Override
    protected Node visitComparisonExpression(ComparisonExpression node, MaterializedViewQueryOptimizerContext context)
    {
        Expression rewriteLeft = (Expression) process(node.getLeft(), context);
        Expression rewriteRight = (Expression) process(node.getRight(), context);
        return new ComparisonExpression(
                node.getOperator(),
                rewriteLeft,
                rewriteRight);
    }

    @Override
    protected Node visitLiteral(Literal node, MaterializedViewQueryOptimizerContext context)
    {
        return node;
    }

    @Override
    protected Node visitGroupBy(GroupBy node, MaterializedViewQueryOptimizerContext context)
    {
        ImmutableList.Builder<GroupingElement> rewriteGroupBy = ImmutableList.builder();
        for (GroupingElement element : node.getGroupingElements()) {
            GroupingElement rewriteElement = (GroupingElement) process(element, context);
            rewriteGroupBy.add(rewriteElement);
        }
        return new GroupBy(
                node.isDistinct(),
                rewriteGroupBy.build());
    }

    @Override
    protected Node visitOrderBy(OrderBy node, MaterializedViewQueryOptimizerContext context)
    {
        ImmutableList.Builder<SortItem> rewriteOrderBy = ImmutableList.builder();
        for (SortItem sortItem : node.getSortItems()) {
            SortItem rewriteSortItem = (SortItem) process(sortItem, context);
            rewriteOrderBy.add(rewriteSortItem);
        }
        return new OrderBy(rewriteOrderBy.build());
    }

    @Override
    protected Node visitSortItem(SortItem node, MaterializedViewQueryOptimizerContext context)
    {
        Expression rewriteSortKey = (Expression) process(node.getSortKey(), context);
        return new SortItem(
                rewriteSortKey,
                node.getOrdering(),
                node.getNullOrdering());
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, MaterializedViewQueryOptimizerContext context)
    {
        ImmutableList.Builder<Expression> rewriteSimpleGroupBy = ImmutableList.builder();
        for (Expression column : node.getExpressions()) {
            Expression rewriteColumn = (Expression) process(column, context);
            rewriteSimpleGroupBy.add(rewriteColumn);
        }
        return new SimpleGroupBy(rewriteSimpleGroupBy.build());
    }

    public static final class MaterializedViewQueryOptimizerContext
    {
        private Table materializedViewTable;
        private Query originalSqlQuery;
        private Map<String, String> baseToViewColumnMap;

        public MaterializedViewQueryOptimizerContext(
                Table materializedViewTable,
                Query originalSqlQuery)
        {
            this.materializedViewTable = materializedViewTable;
            this.originalSqlQuery = originalSqlQuery;
            baseToViewColumnMap = new HashMap<>();
            createBaseToViewColumnMap();
        }

        public void createBaseToViewColumnMap()
        {
            QuerySpecification originalSqlQueryBody = (QuerySpecification) originalSqlQuery.getQueryBody();
            Select baseTableSelectFields = originalSqlQueryBody.getSelect();

            for (SelectItem baseTableSelectColumn : baseTableSelectFields.getSelectItems()) {
                if (baseTableSelectColumn instanceof SingleColumn) {
                    String baseColumnName = ((SingleColumn) baseTableSelectColumn).getExpression().toString();
                    Optional<Identifier> viewOptionalDerivedName = ((SingleColumn) baseTableSelectColumn).getAlias();
                    String viewDerivedColumnName = baseColumnName;

                    if (viewOptionalDerivedName.isPresent()) {
                        viewDerivedColumnName = viewOptionalDerivedName.get().getValue();
                    }

                    baseToViewColumnMap.put(baseColumnName, viewDerivedColumnName);
                }
            }
        }

        public Table getMaterializedViewTable()
        {
            return materializedViewTable;
        }

        public String getViewColumnName(String baseColumnName)
        {
            checkState(baseToViewColumnMap.containsKey(baseColumnName), "Missing column name in the conversion map: " + baseColumnName);
            return baseToViewColumnMap.get(baseColumnName);
        }

        public boolean containsColumnName(String baseColumnName)
        {
            return baseToViewColumnMap.containsKey(baseColumnName);
        }
    }
}
