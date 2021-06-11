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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
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
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class MaterializedViewQueryOptimizer
        extends AstVisitor<Node, Void>
{
    private static final Logger logger = Logger.get(MaterializedViewQueryOptimizer.class);
    private final Table materializedView;
    private final ImmutableMap<String, String> baseToViewColumnMap;
    private final Optional<Relation> baseQueryRelation;

    public MaterializedViewQueryOptimizer(Table materializedView, Query originalSqlQuery)
    {
        this.materializedView = requireNonNull(materializedView, "materialized view is null");

        QuerySpecification materializedViewDefinition = (QuerySpecification) originalSqlQuery.getQueryBody();
        this.baseQueryRelation = materializedViewDefinition.getFrom().isPresent() ? materializedViewDefinition.getFrom() : Optional.empty();

        // Create base to materialized view column map
        ImmutableMap.Builder<String, String> baseToViewColumnMap = ImmutableMap.builder();

        for (SelectItem baseTableSelectColumn : materializedViewDefinition.getSelect().getSelectItems()) {
            if (baseTableSelectColumn instanceof SingleColumn) {
                String baseColumnName = ((SingleColumn) baseTableSelectColumn).getExpression().toString();
                Optional<Identifier> baseColumnAlias = ((SingleColumn) baseTableSelectColumn).getAlias();
                String viewDerivedColumnName = baseColumnAlias.orElse(new Identifier(baseColumnName)).getValue();

                baseToViewColumnMap.put(baseColumnName, viewDerivedColumnName);
            }
        }

        this.baseToViewColumnMap = baseToViewColumnMap.build();
    }

    public Node rewrite(Node node)
    {
        try {
            return process(node);
        }
        catch (Exception ex) {
            logger.error(ex, "Failed to rewrite query: %s.", node);
            return node;
        }
    }

    @Override
    public Node visitNode(Node node, Void context)
    {
        return node;
    }

    @Override
    protected Node visitQuery(Query node, Void context)
    {
        return new Query(
                node.getWith(),
                (QueryBody) process(node.getQueryBody(), context),
                node.getOrderBy(),
                node.getLimit());
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, Void context)
    {
        return new TableSubquery((Query) process(node.getQuery(), context));
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, Void context)
    {
        // Process from and select clause before all others to check if query is rewritable and acquire alias relation if any
        Optional<Relation> rewrittenFrom = node.getFrom().map(from -> (Relation) process(from, context));
        Select rewrittenSelect = (Select) process(node.getSelect(), context);
        Optional<Expression> rewrittenWhere = node.getWhere().map(where -> (Expression) process(where, context));
        Optional<GroupBy> rewrittenGroupBy = node.getGroupBy().map(groupBy -> (GroupBy) process(groupBy, context));
        Optional<Expression> rewrittenHaving = node.getHaving().map(having -> (Expression) process(having, context));
        Optional<OrderBy> rewrittenOrderBy = node.getOrderBy().map(orderBy -> (OrderBy) process(orderBy, context));

        return new QuerySpecification(
                rewrittenSelect,
                rewrittenFrom,
                rewrittenWhere,
                rewrittenGroupBy,
                rewrittenHaving,
                rewrittenOrderBy,
                node.getLimit());
    }

    @Override
    protected Node visitSelect(Select node, Void context)
    {
        ImmutableList.Builder<SelectItem> rewrittenSelectItems = ImmutableList.builder();

        for (SelectItem selectItem : node.getSelectItems()) {
            SelectItem rewrittenSelectItem = (SelectItem) process(selectItem, context);
            rewrittenSelectItems.add(rewrittenSelectItem);
        }

        return new Select(node.isDistinct(), rewrittenSelectItems.build());
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, Void context)
    {
        return new SingleColumn((Expression) process(node.getExpression(), context), node.getAlias());
    }

    @Override
    protected Node visitAllColumns(AllColumns node, Void context)
    {
        return node;
    }

    @Override
    protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
    {
        return new ArithmeticBinaryExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitIdentifier(Identifier node, Void context)
    {
        return new Identifier(getViewColumnName(node.getValue()), node.isDelimited());
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, Void context)
    {
        String functionCall = node.toString();
        ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();

        if (containsColumnName(functionCall)) {
            Expression derivedExpression = new Identifier(getViewColumnName(functionCall));
            rewrittenArguments.add(derivedExpression);
        }
        else {
            for (Expression argument : node.getArguments()) {
                Expression rewrittenArgument = (Expression) process(argument, context);
                rewrittenArguments.add(rewrittenArgument);
            }
        }

        return new FunctionCall(
                node.getName(),
                node.getWindow(),
                node.getFilter(),
                node.getOrderBy(),
                node.isDistinct(),
                node.isIgnoreNulls(),
                rewrittenArguments.build());
    }

    @Override
    protected Node visitRelation(Relation node, Void context)
    {
        Optional<Relation> baseQueryFrom = getBaseQueryFrom();
        if (baseQueryFrom.isPresent() && node.equals(baseQueryFrom.get())) {
            return getMaterializedView();
        }
        throw new IllegalStateException("Failed to find the matching materialized view for: " + node);
    }

    @Override
    protected Node visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
    {
        return new LogicalBinaryExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitComparisonExpression(ComparisonExpression node, Void context)
    {
        return new ComparisonExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitGroupBy(GroupBy node, Void context)
    {
        ImmutableList.Builder<GroupingElement> rewrittenGroupBy = ImmutableList.builder();
        for (GroupingElement element : node.getGroupingElements()) {
            GroupingElement rewrittenElement = (GroupingElement) process(element, context);
            rewrittenGroupBy.add(rewrittenElement);
        }
        return new GroupBy(node.isDistinct(), rewrittenGroupBy.build());
    }

    @Override
    protected Node visitOrderBy(OrderBy node, Void context)
    {
        ImmutableList.Builder<SortItem> rewrittenOrderBy = ImmutableList.builder();
        for (SortItem sortItem : node.getSortItems()) {
            SortItem rewrittenSortItem = (SortItem) process(sortItem, context);
            rewrittenOrderBy.add(rewrittenSortItem);
        }
        return new OrderBy(rewrittenOrderBy.build());
    }

    @Override
    protected Node visitSortItem(SortItem node, Void context)
    {
        return new SortItem((Expression) process(node.getSortKey(), context), node.getOrdering(), node.getNullOrdering());
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, Void context)
    {
        ImmutableList.Builder<Expression> rewrittenSimpleGroupBy = ImmutableList.builder();
        for (Expression column : node.getExpressions()) {
            Expression rewrittenColumn = (Expression) process(column, context);
            rewrittenSimpleGroupBy.add(rewrittenColumn);
        }
        return new SimpleGroupBy(rewrittenSimpleGroupBy.build());
    }

    // Supporting functions for MaterializedViewQueryOptimizer
    private Table getMaterializedView()
    {
        return materializedView;
    }

    private String getViewColumnName(String baseColumnName)
    {
        if (!containsColumnName(baseColumnName)) {
            throw new IllegalStateException("Failed to find the base column: " + baseColumnName);
        }
        return baseToViewColumnMap.getOrDefault(baseColumnName, baseColumnName);
    }

    private boolean containsColumnName(String baseColumnName)
    {
        return baseToViewColumnMap.containsKey(baseColumnName);
    }

    private Optional<Relation> getBaseQueryFrom()
    {
        return baseQueryRelation;
    }
}
