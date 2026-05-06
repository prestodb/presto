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

import com.facebook.presto.sql.MaterializedViewUtils;
import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.sql.MaterializedViewUtils.ASSOCIATIVE_REWRITE_FUNCTIONS;
import static com.facebook.presto.sql.MaterializedViewUtils.NON_ASSOCIATIVE_REWRITE_FUNCTIONS;
import static com.facebook.presto.sql.MaterializedViewUtils.rewriteAssociativeFunction;
import static com.facebook.presto.sql.analyzer.MaterializedViewInformationExtractor.MaterializedViewInfo;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Shared expression-level rewriter for materialized view query optimization.
 * Handles column resolution, function call rewriting, GROUP BY/ORDER BY rewriting
 * for both single-table and JOIN query paths.
 *
 * In single-table mode (tablePrefix absent): maps bare {@link Identifier} nodes
 * through the MV's column map. Expressions have already been prefix-stripped by the caller.
 *
 * In JOIN mode (tablePrefix present): maps qualified {@link DereferenceExpression}
 * nodes whose base matches the swapped table's prefix. Other tables pass through.
 */
public class MaterializedViewExpressionRewriter
{
    private final Optional<Identifier> tablePrefix;
    private final Optional<Identifier> mvPrefix;
    private final MaterializedViewInfo mvInfo;

    public MaterializedViewExpressionRewriter(
            Optional<Identifier> tablePrefix,
            Optional<Identifier> mvPrefix,
            MaterializedViewInfo mvInfo)
    {
        this.tablePrefix = requireNonNull(tablePrefix, "tablePrefix is null");
        this.mvPrefix = requireNonNull(mvPrefix, "mvPrefix is null");
        this.mvInfo = requireNonNull(mvInfo, "mvInfo is null");
    }

    public Expression rewriteExpression(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (tablePrefix.isPresent()) {
                    return node;
                }
                return resolveColumn(node);
            }

            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (tablePrefix.isPresent() && belongsToRewrittenTable(node)) {
                    return resolveColumn(node.getField());
                }
                return node;
            }

            @Override
            public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return MaterializedViewExpressionRewriter.this.rewriteFunctionCall(node);
            }
        }, expression);
    }

    // --- Query component rewriting ---

    public SingleColumn rewriteSingleColumn(SingleColumn node)
    {
        Expression expression = node.getExpression();
        Expression rewritten = rewriteExpression(expression);
        Optional<Identifier> alias = node.getAlias();

        if (!alias.isPresent() && rewritten != expression) {
            if (expression instanceof DereferenceExpression) {
                alias = Optional.of(((DereferenceExpression) expression).getField());
            }
            else if (expression instanceof Identifier && rewritten instanceof Identifier
                    && !rewritten.equals(expression)) {
                alias = Optional.of((Identifier) expression);
            }
        }
        return new SingleColumn(rewritten, alias);
    }

    public GroupBy rewriteGroupBy(GroupBy groupBy, List<SelectItem> selectItems)
    {
        ImmutableList.Builder<GroupingElement> rewrittenElements = ImmutableList.builder();
        for (GroupingElement element : groupBy.getGroupingElements()) {
            rewrittenElements.add(rewriteGroupingElement(element, expr -> rewriteGroupByExpression(expr, selectItems)));
        }
        return new GroupBy(groupBy.isDistinct(), rewrittenElements.build());
    }

    public static GroupingElement rewriteGroupingElement(GroupingElement element, java.util.function.Function<Expression, Expression> rewriter)
    {
        List<Expression> rewritten = element.getExpressions().stream()
                .map(rewriter)
                .collect(toImmutableList());
        if (element instanceof SimpleGroupBy) {
            return new SimpleGroupBy(rewritten);
        }
        if (element instanceof Cube) {
            return new Cube(rewritten);
        }
        if (element instanceof Rollup) {
            return new Rollup(rewritten);
        }
        if (element instanceof GroupingSets) {
            ImmutableList.Builder<List<Expression>> rewrittenSets = ImmutableList.builder();
            for (List<Expression> set : ((GroupingSets) element).getSets()) {
                rewrittenSets.add(set.stream().map(rewriter).collect(toImmutableList()));
            }
            return new GroupingSets(rewrittenSets.build());
        }
        return element;
    }

    public OrderBy rewriteOrderBy(OrderBy orderBy)
    {
        ImmutableList.Builder<SortItem> rewrittenItems = ImmutableList.builder();
        for (SortItem sortItem : orderBy.getSortItems()) {
            rewrittenItems.add(new SortItem(
                    rewriteExpression(sortItem.getSortKey()),
                    sortItem.getOrdering(),
                    sortItem.getNullOrdering()));
        }
        return new OrderBy(rewrittenItems.build());
    }

    // --- Column resolution ---

    private Expression resolveColumn(Expression lookup)
    {
        Map<Expression, Identifier> columnMap = mvInfo.getBaseToViewColumnMap();
        if (!columnMap.containsKey(lookup)) {
            throw new IllegalStateException("Column " + lookup + " not covered by materialized view");
        }
        Identifier mapped = columnMap.get(lookup);
        return wrapResult(mapped);
    }

    private Expression wrapResult(Identifier mapped)
    {
        if (mvPrefix.isPresent()) {
            return new DereferenceExpression(mvPrefix.get(), mapped);
        }
        return mapped;
    }

    private Expression addMvPrefixToExpression(Expression expression)
    {
        if (!mvPrefix.isPresent()) {
            return expression;
        }
        Identifier prefix = mvPrefix.get();
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new DereferenceExpression(prefix, node);
            }
        }, expression);
    }

    // --- Function call rewriting ---

    private Expression rewriteFunctionCall(FunctionCall node)
    {
        if (tablePrefix.isPresent()) {
            return rewriteFunctionCallJoinMode(node);
        }
        return rewriteFunctionCallSingleTableMode(node);
    }

    private Expression rewriteFunctionCallSingleTableMode(FunctionCall node)
    {
        Map<Expression, Identifier> baseToViewColumnMap = mvInfo.getBaseToViewColumnMap();

        if (NON_ASSOCIATIVE_REWRITE_FUNCTIONS.containsKey(node.getName())) {
            return MaterializedViewUtils.rewriteNonAssociativeFunction(node, baseToViewColumnMap);
        }

        if (!ASSOCIATIVE_REWRITE_FUNCTIONS.contains(node.getName())) {
            throw new SemanticException(NOT_SUPPORTED, node, "Was unable to rewrite non-associative function call with materialized view");
        }

        if (baseToViewColumnMap.containsKey(node)) {
            Identifier derivedColumn = baseToViewColumnMap.get(node);
            if (node.isDistinct()) {
                throw new SemanticException(NOT_SUPPORTED, node, "COUNT(DISTINCT) is not supported for materialized view query rewrite optimization");
            }
            return rewriteAssociativeFunction(node, wrapResult(derivedColumn));
        }

        if (mvInfo.getGroupBy().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Materialized view does not pre-compute aggregate: " + node.getName());
        }

        ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();
        for (Expression argument : node.getArguments()) {
            rewrittenArguments.add(rewriteExpression(argument));
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

    private Expression rewriteFunctionCallJoinMode(FunctionCall node)
    {
        boolean refRewritten = referencesRewrittenTable(node);
        boolean refOther = referencesOtherTable(node);

        if (!refRewritten && !refOther) {
            if (mvInfo.getGroupBy().isPresent()) {
                return rewriteTablelessAggregate(node);
            }
            return node;
        }
        if (!refRewritten && refOther) {
            if (mvInfo.getGroupBy().isPresent()) {
                throw new IllegalStateException(
                        "Aggregate on non-rewritten table columns not supported when materialized view has GROUP BY: " + node.getName());
            }
            return node;
        }
        if (refRewritten && refOther) {
            throw new IllegalStateException("Mixed-table aggregate not supported for materialized view join rewrite");
        }

        Map<Expression, Identifier> baseToViewColumnMap = mvInfo.getBaseToViewColumnMap();
        FunctionCall strippedCall = stripPrefixFromFunctionCall(node);

        if (NON_ASSOCIATIVE_REWRITE_FUNCTIONS.containsKey(node.getName())) {
            Expression rewritten = MaterializedViewUtils.rewriteNonAssociativeFunction(strippedCall, baseToViewColumnMap);
            return addMvPrefixToExpression(rewritten);
        }

        if (!ASSOCIATIVE_REWRITE_FUNCTIONS.contains(node.getName())) {
            throw new SemanticException(NOT_SUPPORTED, node, "Unsupported function for materialized view join rewrite: " + node.getName());
        }

        if (node.isDistinct()) {
            throw new SemanticException(NOT_SUPPORTED, node, "COUNT(DISTINCT) is not supported for materialized view query rewrite optimization");
        }

        if (baseToViewColumnMap.containsKey(strippedCall)) {
            Identifier derivedColumn = baseToViewColumnMap.get(strippedCall);
            return rewriteAssociativeFunction(node, wrapResult(derivedColumn));
        }

        ImmutableList.Builder<Expression> rewrittenArgs = ImmutableList.builder();
        for (Expression arg : node.getArguments()) {
            rewrittenArgs.add(rewriteExpression(arg));
        }
        return new FunctionCall(
                node.getName(),
                node.getWindow(),
                node.getFilter(),
                node.getOrderBy(),
                node.isDistinct(),
                node.isIgnoreNulls(),
                rewrittenArgs.build());
    }

    private Expression rewriteTablelessAggregate(FunctionCall node)
    {
        Map<Expression, Identifier> baseToViewColumnMap = mvInfo.getBaseToViewColumnMap();
        if (baseToViewColumnMap.containsKey(node)) {
            Identifier derivedColumn = baseToViewColumnMap.get(node);
            return rewriteAssociativeFunction(node, wrapResult(derivedColumn));
        }
        throw new IllegalStateException("Aggregate " + node.getName() + " without column references cannot be rewritten with pre-aggregated materialized view");
    }

    private FunctionCall stripPrefixFromFunctionCall(FunctionCall functionCall)
    {
        ImmutableList.Builder<Expression> strippedArgs = ImmutableList.builder();
        for (Expression arg : functionCall.getArguments()) {
            strippedArgs.add(stripPrefix(arg));
        }
        return new FunctionCall(
                functionCall.getName(),
                functionCall.getWindow(),
                functionCall.getFilter(),
                functionCall.getOrderBy(),
                functionCall.isDistinct(),
                functionCall.isIgnoreNulls(),
                strippedArgs.build());
    }

    // --- GROUP BY ordinal resolution ---

    private Expression rewriteGroupByExpression(Expression expression, List<SelectItem> selectItems)
    {
        if (expression instanceof LongLiteral) {
            int ordinal = toIntExact(((LongLiteral) expression).getValue());
            if (ordinal >= 1 && ordinal <= selectItems.size()) {
                SelectItem selectItem = selectItems.get(ordinal - 1);
                if (selectItem instanceof SingleColumn) {
                    Expression resolved = ((SingleColumn) selectItem).getExpression();
                    if (tablePrefix.isPresent()) {
                        resolved = stripPrefix(resolved);
                    }
                    return rewriteExpression(expression);
                }
            }
            return expression;
        }
        return rewriteExpression(expression);
    }

    // --- Table reference helpers ---

    public boolean belongsToRewrittenTable(Expression expression)
    {
        if (!tablePrefix.isPresent()) {
            return true;
        }
        if (expression instanceof DereferenceExpression) {
            DereferenceExpression deref = (DereferenceExpression) expression;
            return deref.getBase() instanceof Identifier && tablePrefix.get().equals(deref.getBase());
        }
        return false;
    }

    public Expression stripPrefix(Expression expression)
    {
        if (!tablePrefix.isPresent()) {
            return expression;
        }
        if (expression instanceof DereferenceExpression) {
            DereferenceExpression deref = (DereferenceExpression) expression;
            if (deref.getBase() instanceof Identifier && tablePrefix.get().equals(deref.getBase())) {
                return deref.getField();
            }
        }
        return expression;
    }

    public boolean referencesRewrittenTable(Expression expression)
    {
        if (!tablePrefix.isPresent()) {
            return true;
        }
        AtomicBoolean found = new AtomicBoolean(false);
        new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
            {
                if (belongsToRewrittenTable(node)) {
                    found.set(true);
                }
                return null;
            }
        }.process(expression, null);
        return found.get();
    }

    public boolean referencesOtherTable(Expression expression)
    {
        if (!tablePrefix.isPresent()) {
            return false;
        }
        AtomicBoolean found = new AtomicBoolean(false);
        new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Void context)
            {
                if (!belongsToRewrittenTable(node) && node.getBase() instanceof Identifier) {
                    found.set(true);
                }
                return null;
            }
        }.process(expression, null);
        return found.get();
    }
}
