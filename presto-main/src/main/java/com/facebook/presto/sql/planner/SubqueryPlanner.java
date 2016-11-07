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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.planner.optimizations.Predicates;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression.Quantifier;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.SemanticExceptions.throwNotSupportedException;
import static com.facebook.presto.sql.planner.ExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.util.AstUtils.nodeContains;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class SubqueryPlanner
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;
    private final List<Expression> parameters;

    SubqueryPlanner(Analysis analysis, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session, List<Expression> parameters)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");
        requireNonNull(parameters, "parameters is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
        this.parameters = parameters;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node, true);
        }
        return builder;
    }

    public PlanBuilder handleUncorrelatedSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node, false);
        }
        return builder;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node)
    {
        return handleSubqueries(builder, expression, node, true);
    }

    private PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node, boolean correlationAllowed)
    {
        builder = appendInPredicateApplyNodes(builder, collectInPredicateSubqueries(expression, node), correlationAllowed);
        builder = appendScalarSubqueryApplyNodes(builder, collectScalarSubqueries(expression, node), correlationAllowed);
        builder = appendExistsSubqueryApplyNodes(builder, collectExistsSubqueries(expression, node), correlationAllowed);
        builder = appendQuantifiedComparisonApplyNodes(builder, collectQuantifiedComparisonSubqueries(expression, node), correlationAllowed);
        return builder;
    }

    public Set<InPredicate> collectInPredicateSubqueries(Expression expression, Node node)
    {
        return analysis.getInPredicateSubqueries(node)
                .stream()
                .filter(inPredicate -> nodeContains(expression, inPredicate.getValueList()))
                .collect(toImmutableSet());
    }

    public Set<SubqueryExpression> collectScalarSubqueries(Expression expression, Node node)
    {
        return analysis.getScalarSubqueries(node)
                .stream()
                .filter(subquery -> nodeContains(expression, subquery))
                .collect(toImmutableSet());
    }

    public Set<ExistsPredicate> collectExistsSubqueries(Expression expression, Node node)
    {
        return analysis.getExistsSubqueries(node)
                .stream()
                .filter(subquery -> nodeContains(expression, subquery))
                .collect(toImmutableSet());
    }

    public Set<QuantifiedComparisonExpression> collectQuantifiedComparisonSubqueries(Expression expression, Node node)
    {
        return analysis.getQuantifiedComparisonSubqueries(node)
                .stream()
                .filter(quantifiedComparison -> nodeContains(expression, quantifiedComparison.getSubquery()))
                .collect(toImmutableSet());
    }

    private PlanBuilder appendInPredicateApplyNodes(PlanBuilder subPlan, Set<InPredicate> inPredicates, boolean correlationAllowed)
    {
        for (InPredicate inPredicate : inPredicates) {
            subPlan = appendInPredicateApplyNode(subPlan, inPredicate, correlationAllowed);
        }
        return subPlan;
    }

    private PlanBuilder appendInPredicateApplyNode(PlanBuilder subPlan, InPredicate inPredicate, boolean correlationAllowed)
    {
        subPlan = subPlan.appendProjections(ImmutableList.of(inPredicate.getValue()), symbolAllocator, idAllocator);

        checkState(inPredicate.getValueList() instanceof SubqueryExpression);
        PlanNode subquery = createRelationPlan(((SubqueryExpression) inPredicate.getValueList()).getQuery()).getRoot();
        Map<Expression, Symbol> correlation = extractCorrelation(subPlan, subquery);
        if (!correlationAllowed && correlation.isEmpty()) {
            throwNotSupportedException(inPredicate, "Correlated subquery in given context");
        }
        subPlan = subPlan.appendProjections(correlation.keySet(), symbolAllocator, idAllocator);
        subquery = replaceExpressionsWithSymbols(subquery, correlation);

        TranslationMap translationMap = subPlan.copyTranslations();
        InPredicate parametersReplaced = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters, analysis), inPredicate);
        translationMap.addIntermediateMapping(inPredicate, parametersReplaced);
        SymbolReference valueList = getOnlyElement(subquery.getOutputSymbols()).toSymbolReference();
        translationMap.addIntermediateMapping(parametersReplaced, new InPredicate(parametersReplaced.getValue(), valueList));

        return new PlanBuilder(translationMap,
                new ApplyNode(idAllocator.getNextId(),
                        subPlan.getRoot(),
                        subquery,
                        ImmutableList.copyOf(correlation.values())),
                analysis.getParameters());
    }

    private PlanBuilder appendScalarSubqueryApplyNodes(PlanBuilder builder, Set<SubqueryExpression> scalarSubqueries, boolean correlationAllowed)
    {
        for (SubqueryExpression scalarSubquery : scalarSubqueries) {
            builder = appendScalarSubqueryApplyNode(builder, scalarSubquery, correlationAllowed);
        }
        return builder;
    }

    private PlanBuilder appendScalarSubqueryApplyNode(PlanBuilder subPlan, SubqueryExpression scalarSubquery, boolean correlationAllowed)
    {
        if (subPlan.canTranslate(scalarSubquery)) {
            // given subquery is already appended
            return subPlan;
        }

        return appendSubqueryApplyNode(
                subPlan,
                scalarSubquery,
                scalarSubquery.getQuery(),
                new EnforceSingleRowNode(idAllocator.getNextId(), createRelationPlan(scalarSubquery.getQuery()).getRoot()),
                correlationAllowed);
    }

    private PlanBuilder appendExistsSubqueryApplyNodes(PlanBuilder builder, Set<ExistsPredicate> existsPredicates, boolean correlationAllowed)
    {
        for (ExistsPredicate existsPredicate : existsPredicates) {
            builder = appendExistSubqueryApplyNode(builder, existsPredicate, correlationAllowed);
        }
        return builder;
    }

    /**
     * Exists is modeled as:
     * <pre>
     *     - EnforceSingleRow
     *       - Project($0 > 0)
     *         - Aggregation(COUNT(*))
     *           - Limit(1)
     *             -- subquery
     * </pre>
     */
    private PlanBuilder appendExistSubqueryApplyNode(PlanBuilder subPlan, ExistsPredicate existsPredicate, boolean correlationAllowed)
    {
        if (subPlan.canTranslate(existsPredicate)) {
            // given subquery is already appended
            return subPlan;
        }

        PlanNode subqueryPlan = createRelationPlan(existsPredicate.getSubquery()).getRoot();

        Symbol exists = symbolAllocator.newSymbol("exists", BOOLEAN);
        if (isAggregationWithEmptyGroupBy(subqueryPlan)) {
            subPlan.getTranslations().put(existsPredicate, BooleanLiteral.TRUE_LITERAL);
            return subPlan;
        }

        subqueryPlan = new LimitNode(idAllocator.getNextId(), subqueryPlan, 1, false);

        FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
        QualifiedName countFunction = QualifiedName.of("count");
        Symbol count = symbolAllocator.newSymbol(countFunction.toString(), BIGINT);
        subqueryPlan = new AggregationNode(
                idAllocator.getNextId(),
                subqueryPlan,
                ImmutableMap.of(count, new FunctionCall(countFunction, ImmutableList.of())),
                ImmutableMap.of(count, functionRegistry.resolveFunction(countFunction, ImmutableList.of())),
                ImmutableMap.of(),
                ImmutableList.of(ImmutableList.of()),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        ComparisonExpression countGreaterThanZero = new ComparisonExpression(GREATER_THAN, count.toSymbolReference(), new Cast(new LongLiteral("0"), BIGINT.toString()));
        subqueryPlan = new EnforceSingleRowNode(
                idAllocator.getNextId(),
                new ProjectNode(
                        idAllocator.getNextId(),
                        subqueryPlan,
                        ImmutableMap.of(exists, countGreaterThanZero)));

        return appendSubqueryApplyNode(subPlan, existsPredicate, existsPredicate.getSubquery(), subqueryPlan, correlationAllowed);
    }

    private PlanBuilder appendQuantifiedComparisonApplyNodes(PlanBuilder subPlan, Set<QuantifiedComparisonExpression> quantifiedComparisons, boolean correlationAllowed)
    {
        for (QuantifiedComparisonExpression quantifiedComparison : quantifiedComparisons) {
            subPlan = appendQuantifiedComparisonApplyNode(subPlan, quantifiedComparison, correlationAllowed);
        }
        return subPlan;
    }

    private PlanBuilder appendQuantifiedComparisonApplyNode(PlanBuilder subPlan, QuantifiedComparisonExpression quantifiedComparison, boolean correlationAllowed)
    {
        if (subPlan.canTranslate(quantifiedComparison)) {
            // given subquery is already appended
            return subPlan;
        }
        switch (quantifiedComparison.getComparisonType()) {
            case EQUAL:
                switch (quantifiedComparison.getQuantifier()) {
                    case ALL:
                        return planQuantifiedEqualsAll(subPlan, quantifiedComparison, correlationAllowed);
                    case ANY:
                    case SOME:
                        // A = ANY B <=> A IN B
                        InPredicate inPredicate = new InPredicate(quantifiedComparison.getValue(), quantifiedComparison.getSubquery());
                        subPlan.getTranslations().addIntermediateMapping(quantifiedComparison, inPredicate);
                        return appendInPredicateApplyNode(subPlan, inPredicate, correlationAllowed);
                }
                break;

            case NOT_EQUAL:
                switch (quantifiedComparison.getQuantifier()) {
                    case ALL:
                        // A <> ALL B <=> !(A IN B) <=> !(A = ANY B)
                        QuantifiedComparisonExpression rewrittenAny = new QuantifiedComparisonExpression(
                                EQUAL,
                                Quantifier.ANY,
                                quantifiedComparison.getValue(),
                                quantifiedComparison.getSubquery());
                        Expression notAny = new NotExpression(rewrittenAny);
                        // "A <> ALL B" is equivalent to "NOT (A = ANY B)" so add a rewrite for the initial quantifiedComparison to notAny
                        subPlan.getTranslations().addIntermediateMapping(quantifiedComparison, notAny);
                        // now plan "A = ANY B" part by calling ourselves for rewrittenAny
                        return appendQuantifiedComparisonApplyNode(subPlan, rewrittenAny, correlationAllowed);
                    case ANY:
                    case SOME:
                        // A <> ANY B <=> min B <> max B || A <> min B <=> !(min B = max B && A = min B) <=> !(A = ALL B)
                        QuantifiedComparisonExpression rewrittenAll = new QuantifiedComparisonExpression(
                                EQUAL,
                                QuantifiedComparisonExpression.Quantifier.ALL,
                                quantifiedComparison.getValue(),
                                quantifiedComparison.getSubquery());
                        Expression notAll = new NotExpression(rewrittenAll);
                        // "A <> ANY B" is equivalent to "NOT (A = ALL B)" so add a rewrite for the initial quantifiedComparison to notAll
                        subPlan.getTranslations().addIntermediateMapping(quantifiedComparison, notAll);
                        // now plan "A = ALL B" part by calling ourselves for rewrittenAll
                        return appendQuantifiedComparisonApplyNode(subPlan, rewrittenAll, correlationAllowed);
                }
                break;

            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return planQuantifiedOrderable(subPlan, quantifiedComparison, correlationAllowed);
        }
        // all cases are checked, so this exception should never be thrown
        throw new IllegalArgumentException(
                format("Unexpected quantified comparison: '%s %s'", quantifiedComparison.getComparisonType().getValue(), quantifiedComparison.getQuantifier()));
    }

    private PlanBuilder planQuantifiedEqualsAll(PlanBuilder subPlan, QuantifiedComparisonExpression quantifiedComparison, boolean correlationAllowed)
    {
        checkArgument(quantifiedComparison.getComparisonType() == EQUAL && quantifiedComparison.getQuantifier() == Quantifier.ALL);
        // A = ALL B <=> min B = max B && A = min B
        RelationPlan subqueryRelationPlan = createRelationPlan(quantifiedComparison.getSubquery());
        PlanNode subqueryPlan = subqueryRelationPlan.getRoot();
        List<Expression> outputColumnReference = ImmutableList.of(getOnlyElement(subqueryRelationPlan.getOutputSymbols()).toSymbolReference());
        Type outputColumnType = getOnlyElement(subqueryRelationPlan.getDescriptor().getAllFields()).getType();
        if (!outputColumnType.isOrderable()) {
            throwNotSupportedException(quantifiedComparison, "Quantified comparison '= ALL' or '<> ANY' for unorderable type " + outputColumnType.getDisplayName());
        }
        List<TypeSignature> outputColumnTypeSignature = ImmutableList.of(outputColumnType.getTypeSignature());
        FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
        QualifiedName min = QualifiedName.of("min");
        QualifiedName max = QualifiedName.of("max");
        Symbol minValue = symbolAllocator.newSymbol(min.toString(), outputColumnType);
        Symbol maxValue = symbolAllocator.newSymbol(max.toString(), outputColumnType);
        subqueryPlan = new AggregationNode(
                idAllocator.getNextId(),
                subqueryPlan,
                ImmutableMap.of(
                        minValue, new FunctionCall(min, outputColumnReference),
                        maxValue, new FunctionCall(max, outputColumnReference)
                ),
                ImmutableMap.of(
                        minValue, functionRegistry.resolveFunction(min, outputColumnTypeSignature),
                        maxValue, functionRegistry.resolveFunction(max, outputColumnTypeSignature)
                ),
                ImmutableMap.of(),
                ImmutableList.of(ImmutableList.of()),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
        subqueryPlan = new EnforceSingleRowNode(idAllocator.getNextId(), subqueryPlan);
        LogicalBinaryExpression valueComparedToSubquery = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
                new ComparisonExpression(EQUAL, minValue.toSymbolReference(), maxValue.toSymbolReference()),
                new ComparisonExpression(EQUAL, quantifiedComparison.getValue(), minValue.toSymbolReference())
        );
        subPlan.getTranslations().addIntermediateMapping(quantifiedComparison, valueComparedToSubquery);
        return appendSubqueryApplyNode(subPlan, quantifiedComparison.getSubquery(), quantifiedComparison.getSubquery(), subqueryPlan, correlationAllowed);
    }

    private PlanBuilder planQuantifiedOrderable(PlanBuilder subPlan, QuantifiedComparisonExpression quantifiedComparison, boolean correlationAllowed)
    {
        checkArgument(EnumSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL).contains(quantifiedComparison.getComparisonType()));
        // A < ALL B <=> A < min B
        // A > ALL B <=> A > max B
        // A < ANY B <=> A < max B
        // A > ANY B <=> A > min B
        QualifiedName aggregationFunction = chooseAggregationFunction(quantifiedComparison);
        RelationPlan subqueryRelationPlan = createRelationPlan(quantifiedComparison.getSubquery());
        PlanNode subqueryPlan = subqueryRelationPlan.getRoot();
        Symbol outputColumn = getOnlyElement(subqueryRelationPlan.getOutputSymbols());
        Type outputColumnType = getOnlyElement(subqueryRelationPlan.getDescriptor().getAllFields()).getType();
        checkState(outputColumnType.isOrderable(), "Subquery result type must be orderable");
        FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
        Symbol subValue = symbolAllocator.newSymbol(aggregationFunction.toString(), outputColumnType);
        subqueryPlan = new AggregationNode(
                idAllocator.getNextId(),
                subqueryPlan,
                ImmutableMap.of(subValue, new FunctionCall(aggregationFunction, ImmutableList.of(outputColumn.toSymbolReference()))),
                ImmutableMap.of(subValue, functionRegistry.resolveFunction(aggregationFunction, ImmutableList.of(outputColumnType.getTypeSignature()))),
                ImmutableMap.of(),
                ImmutableList.of(ImmutableList.of()),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
        subqueryPlan = new EnforceSingleRowNode(idAllocator.getNextId(), subqueryPlan);
        ComparisonExpression valueComparedToSubquery = new ComparisonExpression(quantifiedComparison.getComparisonType(), quantifiedComparison.getValue(), subValue.toSymbolReference());
        subPlan.getTranslations().addIntermediateMapping(quantifiedComparison, valueComparedToSubquery);
        return appendSubqueryApplyNode(subPlan, quantifiedComparison.getSubquery(), quantifiedComparison.getSubquery(), subqueryPlan, correlationAllowed);
    }

    private static QualifiedName chooseAggregationFunction(QuantifiedComparisonExpression quantifiedComparison)
    {
        switch (quantifiedComparison.getQuantifier()) {
            case ALL:
                switch (quantifiedComparison.getComparisonType()) {
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        return QualifiedName.of("min");
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        return QualifiedName.of("max");
                }
                break;
            case ANY:
            case SOME:
                switch (quantifiedComparison.getComparisonType()) {
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        return QualifiedName.of("max");
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        return QualifiedName.of("min");
                }
                break;
        }
        throw new IllegalArgumentException("Unexpected quantifier: " + quantifiedComparison.getQuantifier());
    }

    private boolean isAggregationWithEmptyGroupBy(PlanNode subqueryPlan)
    {
        return searchFrom(subqueryPlan)
                .skipOnlyWhen(Predicates.isInstanceOfAny(ProjectNode.class))
                .where(AggregationNode.class::isInstance)
                .findFirst()
                .map(AggregationNode.class::cast)
                .map(aggregation -> aggregation.getGroupingKeys().isEmpty())
                .orElse(false);
    }

    private PlanBuilder appendSubqueryApplyNode(PlanBuilder subPlan, Expression subqueryExpression, Node subquery, PlanNode subqueryNode, boolean correlationAllowed)
    {
        Map<Expression, Symbol> correlation = extractCorrelation(subPlan, subqueryNode);
        if (!correlationAllowed && !correlation.isEmpty()) {
            throwNotSupportedException(subquery, "Correlated subquery in given context");
        }
        subPlan = subPlan.appendProjections(correlation.keySet(), symbolAllocator, idAllocator);
        subqueryNode = replaceExpressionsWithSymbols(subqueryNode, correlation);

        TranslationMap translations = subPlan.copyTranslations();
        if (subqueryNode.getOutputSymbols().size() == 1) {
            translations.put(subqueryExpression, subqueryNode.getOutputSymbols().get(0));
        }

        PlanNode root = subPlan.getRoot();
        if (root.getOutputSymbols().isEmpty()) {
            // there is nothing to join with - e.g. SELECT (SELECT 1)
            return new PlanBuilder(translations, subqueryNode, analysis.getParameters());
        }
        else {
            return new PlanBuilder(translations,
                    new ApplyNode(idAllocator.getNextId(),
                            root,
                            subqueryNode,
                            ImmutableList.copyOf(correlation.values())),
                    analysis.getParameters());
        }
    }

    private Map<Expression, Symbol> extractCorrelation(PlanBuilder subPlan, PlanNode subquery)
    {
        Set<Expression> missingReferences = extractOuterColumnReferences(subquery);
        ImmutableMap.Builder<Expression, Symbol> correlation = ImmutableMap.builder();
        for (Expression missingReference : missingReferences) {
            // missing reference expression can be solved within current subPlan,
            // or within outer plans in case of multiple nesting levels of subqueries.
            tryResolveMissingExpression(subPlan, missingReference)
                    .ifPresent(symbolReference -> correlation.put(missingReference, Symbol.from(symbolReference)));
        }
        return correlation.build();
    }

    /**
     * Checks if give reference expression can resolved within given plan.
     */
    private Optional<Expression> tryResolveMissingExpression(PlanBuilder subPlan, Expression expression)
    {
        Expression rewritten = subPlan.rewrite(expression);
        if (rewritten instanceof SymbolReference) {
            return Optional.of(rewritten);
        }
        return Optional.empty();
    }

    private RelationPlan createRelationPlan(Query subquery)
    {
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session)
                .process(subquery, null);
    }

    private RelationPlan createRelationPlan(Expression subquery)
    {
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session)
                .process(subquery, null);
    }

    /**
     * @return a set of reference expressions which cannot be resolved within this plan. For plan representing:
     * SELECT a, b FROM (VALUES 1) T(a). It will return a set containing single expression reference to 'b'.
     */
    private Set<Expression> extractOuterColumnReferences(PlanNode planNode)
    {
        // at this point all the column references are already rewritten to SymbolReference
        // when reference expression is not rewritten that means it cannot be satisfied within given PlaNode
        // see that TranslationMap only resolves (local) fields in current scope
        return ExpressionExtractor.extractExpressions(planNode).stream()
                .flatMap(expression -> extractColumnReferences(expression, analysis.getColumnReferences()).stream())
                .collect(toImmutableSet());
    }

    private static Set<Expression> extractColumnReferences(Expression expression, Set<Expression> columnReferences)
    {
        ImmutableSet.Builder<Expression> expressionColumnReferences = ImmutableSet.builder();
        new ColumnReferencesExtractor(columnReferences).process(expression, expressionColumnReferences);
        return expressionColumnReferences.build();
    }

    private PlanNode replaceExpressionsWithSymbols(PlanNode planNode, Map<Expression, Symbol> mapping)
    {
        if (mapping.isEmpty()) {
            return planNode;
        }

        Map<Expression, Expression> expressionMapping = mapping.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue().toSymbolReference()));
        return SimplePlanRewriter.rewriteWith(new ExpressionReplacer(idAllocator, expressionMapping), planNode, null);
    }

    private static class ColumnReferencesExtractor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<Expression>>
    {
        private final Set<Expression> columnReferences;

        private ColumnReferencesExtractor(Set<Expression> columnReferences)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<Expression> builder)
        {
            if (columnReferences.contains(node)) {
                builder.add(node);
            }
            else {
                process(node.getBase(), builder);
            }
            return null;
        }

        @Override
        protected Void visitQualifiedNameReference(QualifiedNameReference node, ImmutableSet.Builder<Expression> builder)
        {
            builder.add(node);
            return null;
        }
    }

    private static class ExpressionReplacer
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Map<Expression, Expression> mapping;

        public ExpressionReplacer(PlanNodeIdAllocator idAllocator, Map<Expression, Expression> mapping)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.mapping = requireNonNull(mapping, "mapping is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            ProjectNode rewrittenNode = (ProjectNode) context.defaultRewrite(node);

            ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();
            for (Map.Entry<Symbol, Expression> assignment : rewrittenNode.getAssignments().entrySet()) {
                Expression expression = assignment.getValue();
                Expression rewritten = replaceExpression(expression, mapping);
                assignments.put(assignment.getKey(), rewritten);
            }

            return new ProjectNode(idAllocator.getNextId(), rewrittenNode.getSource(), assignments.build());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            FilterNode rewrittenNode = (FilterNode) context.defaultRewrite(node);
            return new FilterNode(idAllocator.getNextId(), rewrittenNode.getSource(), replaceExpression(rewrittenNode.getPredicate(), mapping));
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
        {
            ValuesNode rewrittenNode = (ValuesNode) context.defaultRewrite(node);
            List<List<Expression>> rewrittenRows = rewrittenNode.getRows().stream()
                    .map(row -> row.stream()
                            .map(column -> replaceExpression(column, mapping))
                            .collect(toImmutableList()))
                    .collect(toImmutableList());
            return new ValuesNode(
                    idAllocator.getNextId(),
                    rewrittenNode.getOutputSymbols(),
                    rewrittenRows);
        }
    }
}
