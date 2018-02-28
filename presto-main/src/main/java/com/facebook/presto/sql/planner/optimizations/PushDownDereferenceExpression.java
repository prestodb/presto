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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.optimizations.MergeNestedColumn.prefixExist;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class PushDownDereferenceExpression
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public PushDownDereferenceExpression(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlparser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        Map<Expression, DereferenceInfo> expressionInfoMap = new HashMap<>();
        return SimplePlanRewriter.rewriteWith(new Optimizer(session, metadata, sqlParser, symbolAllocator, idAllocator, warningCollector), plan, expressionInfoMap);
    }

    private static class DereferenceReplacer
            extends ExpressionRewriter<Void>
    {
        private final Map<Expression, DereferenceInfo> map;

        DereferenceReplacer(Map<Expression, DereferenceInfo> map)
        {
            this.map = map;
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (map.containsKey(node) && map.get(node).isFromValidSource()) {
                return map.get(node).getSymbol().toSymbolReference();
            }
            return treeRewriter.defaultRewrite(node, context);
        }
    }

    private static class Optimizer
            extends SimplePlanRewriter<Map<Expression, DereferenceInfo>>
    {
        private final Session session;
        private final SqlParser sqlParser;
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final WarningCollector warningCollector;

        private Optimizer(Session session, Metadata metadata, SqlParser sqlParser, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
        {
            this.session = session;
            this.sqlParser = sqlParser;
            this.metadata = metadata;
            this.symbolAllocator = symbolAllocator;
            this.idAllocator = idAllocator;
            this.warningCollector = warningCollector;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            extractDereferenceInfos(node).forEach(expressionInfoMap::putIfAbsent);

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            Map<Symbol, AggregationNode.Aggregation> aggregations = new HashMap<>();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> symbolAggregationEntry : node.getAggregations().entrySet()) {
                Symbol symbol = symbolAggregationEntry.getKey();
                AggregationNode.Aggregation oldAggregation = symbolAggregationEntry.getValue();
                AggregationNode.Aggregation newAggregation = new AggregationNode.Aggregation(ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressionInfoMap), oldAggregation.getCall()), oldAggregation.getSignature(), oldAggregation.getMask());
                aggregations.put(symbol, newAggregation);
            }
            return new AggregationNode(
                    idAllocator.getNextId(),
                    child,
                    aggregations,
                    node.getGroupingSets(),
                    node.getPreGroupedSymbols(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            extractDereferenceInfos(node).forEach(expressionInfoMap::putIfAbsent);

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            Expression predicate = ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressionInfoMap), node.getPredicate());
            return new FilterNode(idAllocator.getNextId(), child, predicate);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            //  parentDereferenceInfos is used to find out passThroughSymbol. we will only pass those symbols that are needed by upstream
            List<DereferenceInfo> parentDereferenceInfos = expressionInfoMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
            Map<Expression, DereferenceInfo> newDereferences = extractDereferenceInfos(node);
            newDereferences.forEach(expressionInfoMap::putIfAbsent);

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            List<Symbol> passThroughSymbols = getUsedDereferenceInfo(node.getOutputSymbols(), parentDereferenceInfos).stream().filter(DereferenceInfo::isFromValidSource).map(DereferenceInfo::getSymbol).collect(Collectors.toList());

            Assignments.Builder assignmentsBuilder = Assignments.builder();
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                assignmentsBuilder.put(entry.getKey(), ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressionInfoMap), entry.getValue()));
            }
            assignmentsBuilder.putIdentities(passThroughSymbols);
            ProjectNode newProjectNode = new ProjectNode(idAllocator.getNextId(), child, assignmentsBuilder.build());
            newDereferences.forEach(expressionInfoMap::remove);
            return newProjectNode;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            List<DereferenceInfo> usedDereferenceInfo = getUsedDereferenceInfo(node.getOutputSymbols(), expressionInfoMap.values());
            if (!usedDereferenceInfo.isEmpty()) {
                usedDereferenceInfo.forEach(DereferenceInfo::doesFromValidSource);
                Map<Symbol, Expression> assignmentMap = usedDereferenceInfo.stream().collect(Collectors.toMap(DereferenceInfo::getSymbol, DereferenceInfo::getDereference));
                return new ProjectNode(idAllocator.getNextId(), node, Assignments.builder().putAll(assignmentMap).putIdentities(node.getOutputSymbols()).build());
            }
            return node;
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            List<DereferenceInfo> usedDereferenceInfo = getUsedDereferenceInfo(node.getOutputSymbols(), expressionInfoMap.values());
            if (!usedDereferenceInfo.isEmpty()) {
                usedDereferenceInfo.forEach(DereferenceInfo::doesFromValidSource);
                Map<Symbol, Expression> assignmentMap = usedDereferenceInfo.stream().collect(Collectors.toMap(DereferenceInfo::getSymbol, DereferenceInfo::getDereference));
                return new ProjectNode(idAllocator.getNextId(), node, Assignments.builder().putAll(assignmentMap).putIdentities(node.getOutputSymbols()).build());
            }
            return node;
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            extractDereferenceInfos(joinNode).forEach(expressionInfoMap::putIfAbsent);

            PlanNode leftNode = context.rewrite(joinNode.getLeft(), expressionInfoMap);
            PlanNode rightNode = context.rewrite(joinNode.getRight(), expressionInfoMap);

            List<JoinNode.EquiJoinClause> equiJoinClauses = joinNode.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::toExpression)
                    .map(expr -> ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressionInfoMap), expr))
                    .map(this::getEquiJoinClause)
                    .collect(Collectors.toList());

            Optional<Expression> joinFilter = joinNode.getFilter().map(expression -> ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressionInfoMap), expression));

            return new JoinNode(
                    joinNode.getId(),
                    joinNode.getType(),
                    leftNode,
                    rightNode,
                    equiJoinClauses,
                    ImmutableList.<Symbol>builder().addAll(leftNode.getOutputSymbols()).addAll(rightNode.getOutputSymbols()).build(),
                    joinFilter,
                    joinNode.getLeftHashSymbol(),
                    joinNode.getRightHashSymbol(),
                    joinNode.getDistributionType());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            List<DereferenceInfo> parentDereferenceInfos = expressionInfoMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            List<Symbol> passThroughSymbols = getUsedDereferenceInfo(child.getOutputSymbols(), parentDereferenceInfos).stream().filter(DereferenceInfo::isFromValidSource).map(DereferenceInfo::getSymbol).collect(Collectors.toList());
            UnnestNode unnestNode = new UnnestNode(idAllocator.getNextId(), child, ImmutableList.<Symbol>builder().addAll(node.getReplicateSymbols()).addAll(passThroughSymbols).build(), node.getUnnestSymbols(), node.getOrdinalitySymbol());

            List<Symbol> unnestSymbols = unnestNode.getUnnestSymbols().entrySet().stream().flatMap(entry -> entry.getValue().stream()).collect(Collectors.toList());
            List<DereferenceInfo> dereferenceExpressionInfos = getUsedDereferenceInfo(unnestSymbols, expressionInfoMap.values());
            if (!dereferenceExpressionInfos.isEmpty()) {
                dereferenceExpressionInfos.forEach(DereferenceInfo::doesFromValidSource);
                Map<Symbol, Expression> assignmentMap = dereferenceExpressionInfos.stream().collect(Collectors.toMap(DereferenceInfo::getSymbol, DereferenceInfo::getDereference));
                return new ProjectNode(idAllocator.getNextId(), unnestNode, Assignments.builder().putAll(assignmentMap).putIdentities(unnestNode.getOutputSymbols()).build());
            }
            return unnestNode;
        }

        private List<DereferenceInfo> getUsedDereferenceInfo(List<Symbol> symbols, Collection<DereferenceInfo> dereferenceExpressionInfos)
        {
            Set<Symbol> symbolSet = symbols.stream().collect(Collectors.toSet());
            return dereferenceExpressionInfos.stream().filter(dereferenceExpressionInfo -> symbolSet.contains(dereferenceExpressionInfo.getBaseSymbol())).collect(Collectors.toList());
        }

        private JoinNode.EquiJoinClause getEquiJoinClause(Expression expression)
        {
            checkArgument(expression instanceof ComparisonExpression, "expression [%s] is not equal expression", expression);
            ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
            return new JoinNode.EquiJoinClause(Symbol.from(comparisonExpression.getLeft()), Symbol.from(comparisonExpression.getRight()));
        }

        private Type extractType(Expression expression)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, symbolAllocator.getTypes(), expression, emptyList(), warningCollector);
            return expressionTypes.get(NodeRef.of(expression));
        }

        private DereferenceInfo getDereferenceInfo(Expression expression)
        {
            Symbol symbol = symbolAllocator.newSymbol(expression, extractType(expression));
            Symbol base = Iterables.getOnlyElement(SymbolsExtractor.extractAll(expression));
            return new DereferenceInfo(expression, symbol, base);
        }

        private List<Expression> extractDereference(Expression expression)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            new DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Expression>>()
            {
                @Override
                protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableList.Builder<Expression> context)
                {
                    context.add(node);
                    return null;
                }
            }.process(expression, builder);
            return builder.build();
        }

        private Map<Expression, DereferenceInfo> extractDereferenceInfos(PlanNode node)
        {
            Set<Expression> allExpressions = ExpressionExtractor.extractExpressionsNonRecursive(node).stream()
                    .flatMap(expression -> extractDereference(expression).stream())
                    .map(MergeNestedColumn::validDereferenceExpression).filter(Objects::nonNull).collect(toImmutableSet());

            return allExpressions.stream()
                    .filter(expression -> !prefixExist(expression, allExpressions))
                    .filter(expression -> expression instanceof DereferenceExpression)
                    .distinct()
                    .map(this::getDereferenceInfo)
                    .collect(Collectors.toMap(DereferenceInfo::getDereference, Function.identity()));
        }
    }

    private static class DereferenceInfo
    {
        // e.g. for dereference expression msg.foo[1].bar, base is "msg", newSymbol is new assigned symbol to replace this dereference expression
        private final Expression dereferenceExpression;
        private final Symbol symbol;
        private final Symbol baseSymbol;

        // fromValidSource is used to check whether the dereference expression is from either TableScan or Unnest
        // it will be false for following node therefore we won't rewrite:
        // Project[expr_1 := "max_by"."field1"]
        // - Aggregate[max_by := "max_by"("expr", "app_rating")] => [max_by:row(field0 varchar, field1 varchar)]
        private boolean fromValidSource;

        public DereferenceInfo(Expression dereferenceExpression, Symbol symbol, Symbol baseSymbol)
        {
            this.dereferenceExpression = requireNonNull(dereferenceExpression);
            this.symbol = requireNonNull(symbol);
            this.baseSymbol = requireNonNull(baseSymbol);
            this.fromValidSource = false;
        }

        public Symbol getSymbol()
        {
            return symbol;
        }

        public Symbol getBaseSymbol()
        {
            return baseSymbol;
        }

        public Expression getDereference()
        {
            return dereferenceExpression;
        }

        public boolean isFromValidSource()
        {
            return fromValidSource;
        }

        public void doesFromValidSource()
        {
            fromValidSource = true;
        }

        @Override
        public String toString()
        {
            return String.format("(%s, %s, %s)", dereferenceExpression, symbol, baseSymbol);
        }
    }
}
