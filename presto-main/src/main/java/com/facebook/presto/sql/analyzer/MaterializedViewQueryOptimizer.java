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
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.sql.analyzer.MaterializedViewInformationExtractor.MaterializedViewInfo;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MaterializedViewQueryOptimizer
        extends AstVisitor<Node, Void>
{
    private static final Logger logger = Logger.get(MaterializedViewQueryOptimizer.class);

    private final Table materializedView;
    private final Query materializedViewQuery;

    private MaterializedViewInfo materializedViewInfo;

    public MaterializedViewQueryOptimizer(Table materializedView, Query materializedViewQuery)
    {
        this.materializedView = requireNonNull(materializedView, "materialized view is null");
        this.materializedViewQuery = requireNonNull(materializedViewQuery, "materialized view query is null");
    }

    public Node rewrite(Node node)
    {
        try {
            MaterializedViewInformationExtractor materializedViewInformationExtractor = new MaterializedViewInformationExtractor();
            materializedViewInformationExtractor.process(materializedViewQuery);
            materializedViewInfo = materializedViewInformationExtractor.getMaterializedViewInfo();
            return process(node);
        }
        catch (Exception ex) {
            logger.error(ex.getMessage());
            return node;
        }
    }

    @Override
    protected Node visitNode(Node node, Void context)
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
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, Void context)
    {
        if (!node.getFrom().isPresent()) {
            throw new IllegalStateException("Query with no From clause is not rewritable by materialized view");
        }
        // TODO: Handle filter containment problem https://github.com/prestodb/presto/issues/16405
        if (materializedViewInfo.getWhereClause().isPresent() && !materializedViewInfo.getWhereClause().equals(node.getWhere())) {
            throw new IllegalStateException("Query with no where clause is not rewritable by materialized view with where clause");
        }
        if (materializedViewInfo.getGroupBy().isPresent() && !node.getGroupBy().isPresent()) {
            throw new IllegalStateException("Query with no groupBy clause is not rewritable by materialized view with groupBy clause");
        }
        // TODO: Add HAVING validation to the validator https://github.com/prestodb/presto/issues/16406
        if (node.getHaving().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Having clause is not supported in query optimizer");
        }

        return new QuerySpecification(
                (Select) process(node.getSelect(), context),
                node.getFrom().map(from -> (Relation) process(from, context)),
                node.getWhere().map(where -> (Expression) process(where, context)),
                node.getGroupBy().map(groupBy -> (GroupBy) process(groupBy, context)),
                node.getHaving().map(having -> (Expression) process(having, context)),
                node.getOrderBy().map(orderBy -> (OrderBy) process(orderBy, context)),
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitSelect(Select node, Void context)
    {
        if (materializedViewInfo.isDistinct() && !node.isDistinct()) {
            throw new IllegalStateException("Materialized view has distinct and base query does not");
        }
        ImmutableList.Builder<SelectItem> rewrittenSelectItems = ImmutableList.builder();

        for (SelectItem selectItem : node.getSelectItems()) {
            rewrittenSelectItems.add((SelectItem) process(selectItem, context));
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
        throw new SemanticException(NOT_SUPPORTED, node, "All columns rewrite is not supported in query optimizer");
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
            throw new IllegalStateException("Materialized view definition does not contain mapping for the column: " + node.getValue());
        }
        return new Identifier(materializedViewInfo.getBaseToViewColumnMap().get(node).getValue(), node.isDelimited());
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, Void context)
    {
        ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();

        if (materializedViewInfo.getBaseToViewColumnMap().containsKey(node)) {
            rewrittenArguments.add(materializedViewInfo.getBaseToViewColumnMap().get(node));
        }
        else {
            for (Expression argument : node.getArguments()) {
                rewrittenArguments.add((Expression) process(argument, context));
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
        if (materializedViewInfo.getBaseTable().isPresent() && node.equals(materializedViewInfo.getBaseTable().get())) {
            return materializedView;
        }
        throw new IllegalStateException("Mismatching table or non-supporting relation format in base query");
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
            if (materializedViewInfo.getGroupBy().isPresent() && !materializedViewInfo.getGroupBy().get().contains(element)) {
                throw new IllegalStateException(format("Grouping element %s is not present in materialized view groupBy field", element));
            }
            rewrittenGroupBy.add((GroupingElement) process(element, context));
        }
        return new GroupBy(node.isDistinct(), rewrittenGroupBy.build());
    }

    @Override
    protected Node visitOrderBy(OrderBy node, Void context)
    {
        ImmutableList.Builder<SortItem> rewrittenOrderBy = ImmutableList.builder();
        for (SortItem sortItem : node.getSortItems()) {
            if (!materializedViewInfo.getBaseToViewColumnMap().containsKey(sortItem.getSortKey())) {
                throw new IllegalStateException(format("Sort key %s is not present in materialized view select fields", sortItem.getSortKey()));
            }
            rewrittenOrderBy.add((SortItem) process(sortItem, context));
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
            rewrittenSimpleGroupBy.add((Expression) process(column, context));
        }
        return new SimpleGroupBy(rewrittenSimpleGroupBy.build());
    }
}
