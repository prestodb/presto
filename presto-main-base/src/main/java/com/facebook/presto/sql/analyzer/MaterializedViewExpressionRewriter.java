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
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.facebook.presto.sql.MaterializedViewUtils.ASSOCIATIVE_REWRITE_FUNCTIONS;
import static com.facebook.presto.sql.MaterializedViewUtils.NON_ASSOCIATIVE_REWRITE_FUNCTIONS;
import static com.facebook.presto.sql.MaterializedViewUtils.rewriteAssociativeFunction;
import static com.facebook.presto.sql.MaterializedViewUtils.rewriteGroupingElement;
import static com.facebook.presto.sql.analyzer.MaterializedViewInformationExtractor.MaterializedViewInfo;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MaterializedViewExpressionRewriter
{
    private final MaterializedViewInfo mvInfo;
    private final Set<String> builtInScalarFunctionNames;
    private final Optional<Identifier> tablePrefix;
    private final Optional<Identifier> mvPrefix;
    private Optional<Set<Expression>> expressionsInGroupBy = Optional.empty();

    public MaterializedViewExpressionRewriter(MaterializedViewInfo mvInfo, Set<String> builtInScalarFunctionNames)
    {
        this(mvInfo, builtInScalarFunctionNames, Optional.empty(), Optional.empty());
    }

    public MaterializedViewExpressionRewriter(
            Optional<Identifier> tablePrefix,
            Optional<Identifier> mvPrefix,
            MaterializedViewInfo mvInfo)
    {
        this(mvInfo, ImmutableSet.of(), tablePrefix, mvPrefix);
    }

    private MaterializedViewExpressionRewriter(
            MaterializedViewInfo mvInfo,
            Set<String> builtInScalarFunctionNames,
            Optional<Identifier> tablePrefix,
            Optional<Identifier> mvPrefix)
    {
        this.mvInfo = requireNonNull(mvInfo, "mvInfo is null");
        this.builtInScalarFunctionNames = requireNonNull(builtInScalarFunctionNames, "builtInScalarFunctionNames is null");
        this.tablePrefix = requireNonNull(tablePrefix, "tablePrefix is null");
        this.mvPrefix = requireNonNull(mvPrefix, "mvPrefix is null");
    }

    public void setExpressionsInGroupBy(Optional<Set<Expression>> expressionsInGroupBy)
    {
        this.expressionsInGroupBy = requireNonNull(expressionsInGroupBy, "expressionsInGroupBy is null");
    }

    public Expression rewriteIdentifier(Identifier node)
    {
        Map<Expression, Identifier> columnMap = mvInfo.getBaseToViewColumnMap();
        if (!columnMap.containsKey(node)) {
            throw new IllegalStateException("Materialized view definition does not contain mapping for the column: " + node.getValue());
        }
        return new Identifier(columnMap.get(node).getValue(), node.isDelimited());
    }

    public Expression rewriteFunctionCall(FunctionCall node, Function<Expression, Expression> argRewriter)
    {
        Map<Expression, Identifier> baseToViewColumnMap = mvInfo.getBaseToViewColumnMap();

        if (NON_ASSOCIATIVE_REWRITE_FUNCTIONS.containsKey(node.getName())) {
            return MaterializedViewUtils.rewriteNonAssociativeFunction(node, baseToViewColumnMap);
        }

        if (!ASSOCIATIVE_REWRITE_FUNCTIONS.contains(node.getName())) {
            if (!isScalarFunction(node)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Unsupported function for materialized view rewrite: " + node.getName());
            }
            return rebuildWithRewrittenArgs(node, argRewriter);
        }

        if (baseToViewColumnMap.containsKey(node)) {
            return rewriteAssociativeFunction(node, baseToViewColumnMap.get(node));
        }

        if (mvInfo.getGroupBy().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Materialized view does not pre-compute aggregate: " + node.getName());
        }

        return rebuildWithRewrittenArgs(node, argRewriter);
    }

    public boolean isScalarFunction(FunctionCall functionCall)
    {
        return !functionCall.getWindow().isPresent()
                && builtInScalarFunctionNames.contains(functionCall.getName().getSuffix().toLowerCase(Locale.ENGLISH));
    }

    // Determines whether an expression in a SELECT clause can be safely rewritten
    // when the materialized view has a GROUP BY. Returns false for dimension columns
    // (which require matching GROUP BY in the base query) and true for expressions
    // that can be rolled up or computed from the MV's pre-aggregated data.
    public boolean isRewritableWithMvGroupBy(Set<Expression> mvGroupBy, Expression expression)
    {
        // Dimension column — MV collapses rows on this column, not rewritable
        // without matching GROUP BY in the base query
        if (isExpressionInMvGroupBy(expression, mvGroupBy)) {
            return false;
        }

        // Rewritable aggregate (SUM, COUNT, MIN, MAX, AVG) — rollup is always safe
        if (expression instanceof FunctionCall
                && (ASSOCIATIVE_REWRITE_FUNCTIONS.contains(((FunctionCall) expression).getName())
                || MaterializedViewUtils.validateNonAssociativeFunctionRewrite(
                        (FunctionCall) expression, mvInfo.getBaseToViewColumnMap()))) {
            return true;
        }

        // Non-dimension column mapped in MV — safe to rewrite
        if (mvInfo.getBaseToViewColumnMap().containsKey(expression)) {
            return true;
        }

        // Scalar expression (IF, CAST, ABS, arithmetic, etc.) — safe only when
        // the base query has GROUP BY. Without GROUP BY, the MV collapses rows
        // and these expressions would silently lose duplicates.
        if (expressionsInGroupBy.isPresent()
                && !(expression instanceof Identifier)
                && (!(expression instanceof FunctionCall) || isScalarFunction((FunctionCall) expression))) {
            return true;
        }

        // Unrecognized expression — not rewritable
        return false;
    }

    public boolean isExpressionInMvGroupBy(Expression expression, Set<Expression> mvGroupBy)
    {
        if (mvGroupBy.contains(expression)) {
            return true;
        }
        Identifier viewColumn = mvInfo.getBaseToViewColumnMap().get(expression);
        return viewColumn != null && mvGroupBy.contains(viewColumn);
    }

    private static FunctionCall rebuildWithRewrittenArgs(FunctionCall node, Function<Expression, Expression> argRewriter)
    {
        ImmutableList.Builder<Expression> rewrittenArgs = ImmutableList.builder();
        for (Expression argument : node.getArguments()) {
            rewrittenArgs.add(argRewriter.apply(argument));
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

    public Expression rewriteExpression(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (belongsToRewrittenTable(node)) {
                    return resolveColumnInJoinMode(node.getField());
                }
                return node;
            }

            @Override
            public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return rewriteFunctionCallInJoinMode(node);
            }
        }, expression);
    }

    public SingleColumn rewriteSingleColumn(SingleColumn node)
    {
        Expression expression = node.getExpression();
        Expression rewritten = rewriteExpression(expression);
        Optional<Identifier> alias = node.getAlias();

        if (!alias.isPresent()
                && expression instanceof DereferenceExpression
                && rewritten instanceof DereferenceExpression
                && !((DereferenceExpression) expression).getField().equals(((DereferenceExpression) rewritten).getField())) {
            alias = Optional.of(((DereferenceExpression) expression).getField());
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

    public boolean belongsToRewrittenTable(Expression expression)
    {
        if (!tablePrefix.isPresent()) {
            return false;
        }
        if (expression instanceof DereferenceExpression) {
            DereferenceExpression deref = (DereferenceExpression) expression;
            return deref.getBase() instanceof Identifier && tablePrefix.get().equals(deref.getBase());
        }
        return false;
    }

    public Expression stripPrefix(Expression expression)
    {
        if (tablePrefix.isPresent() && expression instanceof DereferenceExpression) {
            DereferenceExpression deref = (DereferenceExpression) expression;
            if (deref.getBase() instanceof Identifier && tablePrefix.get().equals(deref.getBase())) {
                return deref.getField();
            }
        }
        return expression;
    }

    public boolean referencesRewrittenTable(Expression expression)
    {
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

    private Expression resolveColumnInJoinMode(Expression lookup)
    {
        Map<Expression, Identifier> columnMap = mvInfo.getBaseToViewColumnMap();
        if (!columnMap.containsKey(lookup)) {
            throw new IllegalStateException("Column " + lookup + " not covered by materialized view");
        }
        return new DereferenceExpression(mvPrefix.get(), columnMap.get(lookup));
    }

    private Expression rewriteFunctionCallInJoinMode(FunctionCall node)
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

        // Strip table prefix and delegate to the shared rewriter.
        // rewriteFunctionCall returns results with bare Identifiers;
        // addMvPrefixToExpression wraps them with the MV table prefix.
        FunctionCall strippedCall = stripPrefixFromFunctionCall(node);
        Expression result = rewriteFunctionCall(strippedCall, this::rewriteBareArg);
        return addMvPrefixToExpression(result);
    }

    private Expression rewriteTablelessAggregate(FunctionCall node)
    {
        Map<Expression, Identifier> baseToViewColumnMap = mvInfo.getBaseToViewColumnMap();
        if (baseToViewColumnMap.containsKey(node)) {
            Identifier derivedColumn = baseToViewColumnMap.get(node);
            return rewriteAssociativeFunction(node, new DereferenceExpression(mvPrefix.get(), derivedColumn));
        }
        throw new IllegalStateException("Aggregate " + node.getName() + " without column references cannot be rewritten with pre-aggregated materialized view");
    }

    private FunctionCall stripPrefixFromFunctionCall(FunctionCall functionCall)
    {
        return (FunctionCall) ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (belongsToRewrittenTable(node)) {
                    return node.getField();
                }
                return node;
            }
        }, functionCall);
    }

    private Expression addMvPrefixToExpression(Expression expression)
    {
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

    private Expression rewriteBareArg(Expression arg)
    {
        if (arg instanceof Identifier) {
            return rewriteIdentifier((Identifier) arg);
        }
        throw new IllegalStateException("Complex expression in aggregate argument not supported for JOIN mode materialized view rewrite: " + arg);
    }

    private Expression rewriteGroupByExpression(Expression expression, List<SelectItem> selectItems)
    {
        if (expression instanceof LongLiteral) {
            int ordinal = toIntExact(((LongLiteral) expression).getValue());
            if (ordinal >= 1 && ordinal <= selectItems.size()) {
                SelectItem selectItem = selectItems.get(ordinal - 1);
                if (selectItem instanceof SingleColumn) {
                    Expression resolved = stripPrefix(((SingleColumn) selectItem).getExpression());
                    return rewriteExpression(resolved);
                }
            }
            return expression;
        }
        return rewriteExpression(expression);
    }
}
