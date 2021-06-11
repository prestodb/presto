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
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.sql.analyzer.MaterializedViewInformationExtractor.MaterializedViewInfo;
import static java.util.Objects.requireNonNull;

public class MaterializedViewQueryOptimizer
        extends AstVisitor<Node, Void>
{
    private final Table materializedView;

    private final MaterializedViewInfo materializedViewInfo;

    private boolean isRewritable = true;

    public MaterializedViewQueryOptimizer(Table materializedView, Query materializedViewQuery)
    {
        this.materializedView = requireNonNull(materializedView, "materialized view is null");
        MaterializedViewInfo materializedViewInfo = new MaterializedViewInfo();
        MaterializedViewInformationExtractor materializedViewInformationExtractor = new MaterializedViewInformationExtractor();
        materializedViewInformationExtractor.process(materializedViewQuery, materializedViewInfo);
        this.materializedViewInfo = materializedViewInfo;

        // If materialized view has a LIMIT clause, then it would reject the rewrite optimization
        if (materializedViewInfo.isLimitClausePresented()) {
            isRewritable = false;
        }

        // If materialized view has table alias in the definition query, then it would reject the rewrite optimization
        // TODO: Handle table alias rewrite
        if (materializedViewInfo.isTableAliasPresented()) {
            isRewritable = false;
        }
    }

    public Node rewrite(Node node)
    {
        if (!isRewritable) {
            return node;
        }
        return process(node);
    }

    @Override
    protected Node visitNode(Node node, Void context)
    {
        return node;
    }

    @Override
    protected Node visitQuery(Query node, Void context)
    {
        QueryBody rewrittenQueryBody = (QueryBody) process(node.getQueryBody(), context);
        if (!isRewritable) {
            return node;
        }
        return new Query(
                node.getWith(),
                rewrittenQueryBody,
                node.getOrderBy(),
                node.getLimit());
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, Void context)
    {
        // Each of the process could potentially set isRewritable to false. So return as early as possible is an potential optimization
        // TODO: Reject the rewrite optimization as soon as possible (optimization opportunity)

        // Process from and select clause before all others to check if query is rewritable and acquire alias relation if any
        Optional<Relation> rewrittenFrom = node.getFrom().map(from -> (Relation) process(from, context));
        Select rewrittenSelect = (Select) process(node.getSelect(), context);

        // In WHERE clause validation process, validator only allows two conditions for rewrite so far:
        // 1. MV's definition query has no WHERE clause
        // 2. MV's WHERE clause is the exact match of base query's WHERE clause
        // TODO: Improve the implication logic in the validator so that it could handle more eligible conditions
        if (materializedViewInfo.getMaterializedViewDefinitionWhere().isPresent() && !materializedViewInfo.getMaterializedViewDefinitionWhere().get().equals(node.getWhere().orElse(null))) {
            isRewritable = false;
            return node;
        }
        Optional<Expression> rewrittenWhere = node.getWhere().map(where -> (Expression) process(where, context));

        // If materialized view contains groupBy clause but base query does not, then it rejects rewrite optimization.
        if (materializedViewInfo.getMaterializedViewDefinitionGroupBy().isPresent() && !node.getGroupBy().isPresent()) {
            isRewritable = false;
            return node;
        }
        Optional<GroupBy> rewrittenGroupBy = node.getGroupBy().map(groupBy -> (GroupBy) process(groupBy, context));

        // Currently validator allows all HAVING clause (does not detect invalid HAVING logic implication):
        // TODO: Add HAVING validation to the validator
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
        if (!isRewritable) {
            return node;
        }
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
        if (!materializedViewInfo.getBaseToViewColumnMap().containsKey(node)) {
            isRewritable = false;
            return node;
        }
        return new Identifier(materializedViewInfo.getBaseToViewColumnMap().get(node).getValue(), node.isDelimited());
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, Void context)
    {
        ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();

        if (materializedViewInfo.getBaseToViewColumnMap().containsKey(node)) {
            Expression derivedExpression = materializedViewInfo.getBaseToViewColumnMap().get(node);
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
        if (materializedViewInfo.getMaterializedViewDefinitionRelation().isPresent() && node.equals(materializedViewInfo.getMaterializedViewDefinitionRelation().get())) {
            return materializedView;
        }
        isRewritable = false;
        return node;
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
            if (materializedViewInfo.getMaterializedViewDefinitionGroupBy().isPresent() && !materializedViewInfo.getMaterializedViewDefinitionGroupBy().get().contains(element)) {
                isRewritable = false;
                return node;
            }
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
            if (!materializedViewInfo.getBaseToViewColumnMap().containsKey(sortItem.getSortKey())) {
                isRewritable = false;
                return node;
            }
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
}
