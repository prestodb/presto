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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ExistsExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignmentUtils;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression.Quantifier;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.util.MorePredicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.createSymbolReference;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;
import static com.facebook.presto.sql.analyzer.SemanticExceptions.notSupportedException;
import static com.facebook.presto.sql.analyzer.SemanticExceptions.subQueryNotSupportedError;
import static com.facebook.presto.sql.planner.PlannerUtils.newVariable;
import static com.facebook.presto.sql.planner.TranslateExpressionsUtil.toRowExpression;
import static com.facebook.presto.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.OR;
import static com.facebook.presto.sql.util.AstUtils.nodeContains;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class SubqueryPlanner
{
    private final Analysis analysis;
    private final VariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaDeclarationToVariableMap;
    private final Metadata metadata;
    private final Session session;
    private final SqlParser sqlParser;

    SubqueryPlanner(
            Analysis analysis,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaDeclarationToVariableMap,
            Metadata metadata,
            Session session,
            SqlParser sqlParser)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToVariableMap, "lambdaDeclarationToVariableMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");
        requireNonNull(sqlParser, "sqlParser is null");

        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToVariableMap = lambdaDeclarationToVariableMap;
        this.metadata = metadata;
        this.session = session;
        this.sqlParser = sqlParser;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node, SqlPlannerContext context)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node, true, context);
        }
        return builder;
    }

    public PlanBuilder handleUncorrelatedSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node, SqlPlannerContext context)
    {
        for (Expression expression : expressions) {
            builder = handleSubqueries(builder, expression, node, false, context);
        }
        return builder;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node, SqlPlannerContext context)
    {
        return handleSubqueries(builder, expression, node, true, context);
    }

    private PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node, boolean correlationAllowed, SqlPlannerContext context)
    {
        boolean mayParticipateInAntiJoin = detectAntiJoinInExpression(expression);
        builder = appendInPredicateApplyNodes(builder, collectInPredicateSubqueries(expression, node), correlationAllowed, mayParticipateInAntiJoin, node, context);
        builder = appendScalarSubqueryApplyNodes(builder, collectScalarSubqueries(expression, node), correlationAllowed, context);
        builder = appendExistsSubqueryApplyNodes(builder, collectExistsSubqueries(expression, node), correlationAllowed, mayParticipateInAntiJoin, context);
        builder = appendQuantifiedComparisonApplyNodes(builder, collectQuantifiedComparisonSubqueries(expression, node), correlationAllowed, mayParticipateInAntiJoin, node, context);
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

    private PlanBuilder appendInPredicateApplyNodes(PlanBuilder subPlan, Set<InPredicate> inPredicates, boolean correlationAllowed, boolean mayParticipateInAntiJoin, Node node, SqlPlannerContext context)
    {
        for (InPredicate inPredicate : inPredicates) {
            subPlan = appendInPredicateApplyNode(subPlan, inPredicate, correlationAllowed, mayParticipateInAntiJoin, node, context);
        }
        return subPlan;
    }

    private PlanBuilder appendInPredicateApplyNode(PlanBuilder subPlan, InPredicate inPredicate, boolean correlationAllowed, boolean mayParticipateInAntiJoin, Node node, SqlPlannerContext context)
    {
        if (subPlan.canTranslate(inPredicate)) {
            // given subquery is already appended
            return subPlan;
        }

        subPlan = handleSubqueries(subPlan, inPredicate.getValue(), node, context);

        subPlan = subPlan.appendProjections(ImmutableList.of(inPredicate.getValue()), variableAllocator, idAllocator, session, metadata, sqlParser, analysis, context);

        checkState(inPredicate.getValueList() instanceof SubqueryExpression);
        SubqueryExpression valueListSubquery = (SubqueryExpression) inPredicate.getValueList();
        SubqueryExpression uncoercedValueListSubquery = uncoercedSubquery(valueListSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedValueListSubquery, context);

        subqueryPlan = subqueryPlan.appendProjections(ImmutableList.of(valueListSubquery), variableAllocator, idAllocator, session, metadata, sqlParser, analysis, context);
        SymbolReference valueList = createSymbolReference(subqueryPlan.translate(valueListSubquery));

        VariableReferenceExpression rewrittenValue = subPlan.translate(inPredicate.getValue());
        InPredicate inPredicateSubqueryExpression = new InPredicate(new SymbolReference(inPredicate.getLocation(), rewrittenValue.getName()), valueList);
        VariableReferenceExpression inPredicateSubqueryVariable = newVariable(variableAllocator, inPredicateSubqueryExpression, BOOLEAN);

        subPlan.getTranslations().put(inPredicate, inPredicateSubqueryVariable);

        return appendApplyNode(subPlan, inPredicate, subqueryPlan.getRoot(), Assignments.of(inPredicateSubqueryVariable, rowExpression(inPredicateSubqueryExpression, context)), correlationAllowed, mayParticipateInAntiJoin, context);
    }

    private PlanBuilder appendScalarSubqueryApplyNodes(PlanBuilder builder, Set<SubqueryExpression> scalarSubqueries, boolean correlationAllowed, SqlPlannerContext context)
    {
        for (SubqueryExpression scalarSubquery : scalarSubqueries) {
            builder = appendScalarSubqueryApplyNode(builder, scalarSubquery, correlationAllowed, context);
        }
        return builder;
    }

    private PlanBuilder appendScalarSubqueryApplyNode(PlanBuilder subPlan, SubqueryExpression scalarSubquery, boolean correlationAllowed, SqlPlannerContext context)
    {
        if (subPlan.canTranslate(scalarSubquery)) {
            // given subquery is already appended
            return subPlan;
        }

        List<Expression> coercions = coercionsFor(scalarSubquery);

        SubqueryExpression uncoercedScalarSubquery = uncoercedSubquery(scalarSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedScalarSubquery, context);
        subqueryPlan = subqueryPlan.withNewRoot(new EnforceSingleRowNode(subPlan.getRoot().getSourceLocation(), idAllocator.getNextId(), subqueryPlan.getRoot()));
        subqueryPlan = subqueryPlan.appendProjections(coercions, variableAllocator, idAllocator, session, metadata, sqlParser, analysis, context);

        VariableReferenceExpression uncoercedScalarSubqueryVariable = subqueryPlan.translate(uncoercedScalarSubquery);
        subPlan.getTranslations().put(uncoercedScalarSubquery, uncoercedScalarSubqueryVariable);

        for (Expression coercion : coercions) {
            VariableReferenceExpression coercionVariable = subqueryPlan.translate(coercion);
            subPlan.getTranslations().put(coercion, coercionVariable);
        }

        return appendLateralJoin(subPlan, subqueryPlan, scalarSubquery.getQuery(), correlationAllowed, LateralJoinNode.Type.LEFT, context);
    }

    public PlanBuilder appendLateralJoin(PlanBuilder subPlan, PlanBuilder subqueryPlan, Query query, boolean correlationAllowed, LateralJoinNode.Type type, SqlPlannerContext context)
    {
        PlanNode subqueryNode = subqueryPlan.getRoot();
        Map<Expression, Expression> correlation = extractCorrelation(subPlan, subqueryNode, context);
        if (!correlationAllowed && !correlation.isEmpty()) {
            throw notSupportedException(query, "Correlated subquery in given context");
        }
        subqueryNode = replaceExpressionsWithSymbols(subqueryNode, correlation, context);

        return new PlanBuilder(
                subPlan.copyTranslations(),
                new LateralJoinNode(
                        subPlan.getRoot().getSourceLocation(),
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        subqueryNode,
                        ImmutableList.copyOf(VariablesExtractor.extractUnique(correlation.values(), TypeProvider.viewOf(variableAllocator.getVariables()))),
                        type,
                        subQueryNotSupportedError(query, "Given correlated subquery")));
    }

    private PlanBuilder appendExistsSubqueryApplyNodes(PlanBuilder builder, Set<ExistsPredicate> existsPredicates, boolean correlationAllowed, boolean mayParticipateInAntiJoin, SqlPlannerContext context)
    {
        for (ExistsPredicate existsPredicate : existsPredicates) {
            builder = appendExistSubqueryApplyNode(builder, existsPredicate, correlationAllowed, mayParticipateInAntiJoin, context);
        }
        return builder;
    }

    /**
     * Exists is modeled as:
     * <pre>
     *     - Project($0 > 0)
     *       - Aggregation(COUNT(*))
     *         - Limit(1)
     *           -- subquery
     * </pre>
     */
    private PlanBuilder appendExistSubqueryApplyNode(PlanBuilder subPlan, ExistsPredicate existsPredicate, boolean correlationAllowed, boolean mayParticipateInAntiJoin, SqlPlannerContext context)
    {
        if (subPlan.canTranslate(existsPredicate)) {
            // given subquery is already appended
            return subPlan;
        }

        PlanBuilder subqueryPlan = createPlanBuilder(existsPredicate.getSubquery(), context);

        PlanNode subqueryPlanRoot = subqueryPlan.getRoot();
        if (isAggregationWithEmptyGroupBy(subqueryPlanRoot)) {
            subPlan.getTranslations().put(existsPredicate, BooleanLiteral.TRUE_LITERAL);
            return subPlan;
        }

        // add an explicit projection that removes all columns
        PlanNode subqueryNode = new ProjectNode(idAllocator.getNextId(), subqueryPlan.getRoot(), Assignments.of());

        VariableReferenceExpression exists = variableAllocator.newVariable(getSourceLocation(existsPredicate), "exists", BOOLEAN);
        subPlan.getTranslations().put(existsPredicate, exists);

        return appendApplyNode(
                subPlan,
                existsPredicate.getSubquery(),
                subqueryNode,
                Assignments.of(exists, new ExistsExpression(Optional.empty(), TRUE_CONSTANT)),
                correlationAllowed,
                mayParticipateInAntiJoin,
                context);
    }

    private PlanBuilder appendQuantifiedComparisonApplyNodes(PlanBuilder subPlan, Set<QuantifiedComparisonExpression> quantifiedComparisons, boolean correlationAllowed, boolean mayParticipateInAntiJoin, Node node, SqlPlannerContext context)
    {
        for (QuantifiedComparisonExpression quantifiedComparison : quantifiedComparisons) {
            subPlan = appendQuantifiedComparisonApplyNode(subPlan, quantifiedComparison, correlationAllowed, mayParticipateInAntiJoin, node, context);
        }
        return subPlan;
    }

    private PlanBuilder appendQuantifiedComparisonApplyNode(PlanBuilder subPlan, QuantifiedComparisonExpression quantifiedComparison, boolean correlationAllowed, boolean mayParticipateInAntiJoin, Node node, SqlPlannerContext context)
    {
        if (subPlan.canTranslate(quantifiedComparison)) {
            // given subquery is already appended
            return subPlan;
        }
        switch (quantifiedComparison.getOperator()) {
            case EQUAL:
                switch (quantifiedComparison.getQuantifier()) {
                    case ALL:
                        return planQuantifiedApplyNode(subPlan, quantifiedComparison, correlationAllowed, mayParticipateInAntiJoin, context);
                    case ANY:
                    case SOME:
                        // A = ANY B <=> A IN B
                        InPredicate inPredicate = new InPredicate(quantifiedComparison.getValue(), quantifiedComparison.getSubquery());
                        subPlan = appendInPredicateApplyNode(subPlan, inPredicate, correlationAllowed, mayParticipateInAntiJoin, node, context);
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.translate(inPredicate));
                        return subPlan;
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
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.getTranslations().rewrite(notAny));
                        // now plan "A = ANY B" part by calling ourselves for rewrittenAny
                        return appendQuantifiedComparisonApplyNode(subPlan, rewrittenAny, correlationAllowed, true, node, context);
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
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.getTranslations().rewrite(notAll));
                        // now plan "A = ALL B" part by calling ourselves for rewrittenAll
                        return appendQuantifiedComparisonApplyNode(subPlan, rewrittenAll, correlationAllowed, true, node, context);
                }
                break;

            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return planQuantifiedApplyNode(subPlan, quantifiedComparison, correlationAllowed, mayParticipateInAntiJoin, context);
        }
        // all cases are checked, so this exception should never be thrown
        throw new IllegalArgumentException(
                format("Unexpected quantified comparison: '%s %s'", quantifiedComparison.getOperator().getValue(), quantifiedComparison.getQuantifier()));
    }

    private PlanBuilder planQuantifiedApplyNode(PlanBuilder subPlan, QuantifiedComparisonExpression quantifiedComparison, boolean correlationAllowed, boolean mayParticipateInAntiJoin, SqlPlannerContext context)
    {
        subPlan = subPlan.appendProjections(ImmutableList.of(quantifiedComparison.getValue()), variableAllocator, idAllocator, session, metadata, sqlParser, analysis, context);

        checkState(quantifiedComparison.getSubquery() instanceof SubqueryExpression);
        SubqueryExpression quantifiedSubquery = (SubqueryExpression) quantifiedComparison.getSubquery();

        SubqueryExpression uncoercedQuantifiedSubquery = uncoercedSubquery(quantifiedSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedQuantifiedSubquery, context);
        subqueryPlan = subqueryPlan.appendProjections(ImmutableList.of(quantifiedSubquery), variableAllocator, idAllocator, session, metadata, sqlParser, analysis, context);

        QuantifiedComparisonExpression coercedQuantifiedComparison = new QuantifiedComparisonExpression(
                quantifiedComparison.getOperator(),
                quantifiedComparison.getQuantifier(),
                createSymbolReference(subPlan.translate(quantifiedComparison.getValue())),
                createSymbolReference(subqueryPlan.translate(quantifiedSubquery)));

        VariableReferenceExpression coercedQuantifiedComparisonVariable = newVariable(variableAllocator, coercedQuantifiedComparison, BOOLEAN);
        subPlan.getTranslations().put(quantifiedComparison, coercedQuantifiedComparisonVariable);

        return appendApplyNode(
                subPlan,
                quantifiedComparison.getSubquery(),
                subqueryPlan.getRoot(),
                Assignments.of(coercedQuantifiedComparisonVariable, rowExpression(coercedQuantifiedComparison, context)),
                correlationAllowed,
                mayParticipateInAntiJoin,
                context);
    }

    private static boolean isAggregationWithEmptyGroupBy(PlanNode planNode)
    {
        return searchFrom(planNode)
                .recurseOnlyWhen(MorePredicates.isInstanceOfAny(ProjectNode.class))
                .where(AggregationNode.class::isInstance)
                .findFirst()
                .map(AggregationNode.class::cast)
                .map(aggregation -> aggregation.getGroupingKeys().isEmpty())
                .orElse(false);
    }

    /**
     * Implicit coercions are added when mapping an expression to symbol in {@link TranslationMap}. Coercions
     * for expression are obtained from {@link Analysis} by identity comparison. Create a copy of subquery
     * in order to get a subquery expression that does not have any coercion assigned to it {@link Analysis}.
     */
    private SubqueryExpression uncoercedSubquery(SubqueryExpression subquery)
    {
        return new SubqueryExpression(subquery.getQuery());
    }

    private List<Expression> coercionsFor(Expression expression)
    {
        return analysis.getCoercions().keySet().stream()
                .map(NodeRef::getNode)
                .filter(coercionExpression -> coercionExpression.equals(expression))
                .collect(toImmutableList());
    }

    private PlanBuilder appendApplyNode(PlanBuilder subPlan, Node subquery, PlanNode subqueryNode, Assignments subqueryAssignments, boolean correlationAllowed, boolean mayParticipateInAntiJoin, SqlPlannerContext context)
    {
        Map<Expression, Expression> correlation = extractCorrelation(subPlan, subqueryNode, context);
        if (!correlationAllowed && !correlation.isEmpty()) {
            throw notSupportedException(subquery, "Correlated subquery in given context");
        }
        subPlan = subPlan.appendProjections(correlation.keySet(), variableAllocator, idAllocator, session, metadata, sqlParser, analysis, context);
        subqueryNode = replaceExpressionsWithSymbols(subqueryNode, correlation, context);

        TranslationMap translations = subPlan.copyTranslations();
        PlanNode root = subPlan.getRoot();
        verifySubquerySupported(subqueryAssignments);
        return new PlanBuilder(translations,
                new ApplyNode(
                        subqueryNode.getSourceLocation(),
                        idAllocator.getNextId(),
                        root,
                        subqueryNode,
                        subqueryAssignments,
                        ImmutableList.copyOf(VariablesExtractor.extractUnique(correlation.values(), TypeProvider.viewOf(variableAllocator.getVariables()))),
                        subQueryNotSupportedError(subquery, "Given correlated subquery"),
                        mayParticipateInAntiJoin));
    }

    private Map<Expression, Expression> extractCorrelation(PlanBuilder subPlan, PlanNode subquery, SqlPlannerContext context)
    {
        Set<Expression> missingReferences = extractOuterColumnReferences(subquery, context);
        ImmutableMap.Builder<Expression, Expression> correlation = ImmutableMap.builder();
        for (Expression missingReference : missingReferences) {
            // missing reference expression can be solved within current subPlan,
            // or within outer plans in case of multiple nesting levels of subqueries.
            tryResolveMissingExpression(subPlan, missingReference)
                    .ifPresent(symbolReference -> correlation.put(missingReference, symbolReference));
        }
        return correlation.build();
    }

    /**
     * Checks if give reference expression can resolved within given plan.
     */
    private static Optional<Expression> tryResolveMissingExpression(PlanBuilder subPlan, Expression expression)
    {
        Expression rewritten = subPlan.rewrite(expression);
        if (rewritten != expression) {
            return Optional.of(rewritten);
        }
        return Optional.empty();
    }

    private PlanBuilder createPlanBuilder(Node node, SqlPlannerContext context)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session, sqlParser)
                .process(node, context);
        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToVariableMap);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        if (node instanceof Expression && relationPlan.getFieldMappings().size() == 1) {
            translations.put((Expression) node, getOnlyElement(relationPlan.getFieldMappings()));
        }

        return new PlanBuilder(translations, relationPlan.getRoot());
    }

    /**
     * @return a set of reference expressions which cannot be resolved within this plan. For plan representing:
     * SELECT a, b FROM (VALUES 1) T(a). It will return a set containing single expression reference to 'b'.
     */
    private Set<Expression> extractOuterColumnReferences(PlanNode planNode, SqlPlannerContext context)
    {
        // at this point all the column references are already rewritten to SymbolReference
        // when reference expression is not rewritten that means it cannot be satisfied within given PlaNode
        // see that TranslationMap only resolves (local) fields in current scope
        return ExpressionExtractor.extractExpressions(planNode).stream()
                .filter(rowExpression -> context.getTranslatorContext().getExpressionMap().containsKey(rowExpression))
                .map(rowExpression -> context.getTranslatorContext().getExpressionMap().get(rowExpression))
                .flatMap(expression -> extractColumnReferences(expression, analysis.getColumnReferences()).stream())
                .collect(toImmutableSet());
    }

    private RowExpression rowExpression(Expression expression, SqlPlannerContext context)
    {
        return toRowExpression(
                expression,
                metadata,
                session,
                sqlParser,
                variableAllocator,
                analysis,
                context.getTranslatorContext());
    }

    private static Set<Expression> extractColumnReferences(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<Expression> expressionColumnReferences = ImmutableSet.builder();
        new ColumnReferencesExtractor(columnReferences).process(expression, expressionColumnReferences);
        return expressionColumnReferences.build();
    }

    private PlanNode replaceExpressionsWithSymbols(PlanNode planNode, Map<Expression, Expression> mapping, SqlPlannerContext context)
    {
        if (mapping.isEmpty()) {
            return planNode;
        }
        // There can be duplicates Expressions here.
        Map<RowExpression, RowExpression> rowExpressionMapping = new HashMap<>();
        mapping.forEach((key, value) ->
                rowExpressionMapping.put(
                        context.getTranslatorContext().getRowExpressionMap().get(key),
                        rowExpression(value, context)));

        return SimplePlanRewriter.rewriteWith(new ExpressionReplacer(idAllocator, ImmutableMap.copyOf(rowExpressionMapping)), planNode, null);
    }

    private static class ColumnReferencesExtractor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<Expression>>
    {
        private final Set<NodeRef<Expression>> columnReferences;

        private ColumnReferencesExtractor(Set<NodeRef<Expression>> columnReferences)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<Expression> builder)
        {
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                builder.add(node);
            }
            else {
                process(node.getBase(), builder);
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<Expression> builder)
        {
            builder.add(node);
            return null;
        }
    }

    private static class ExpressionReplacer
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Map<RowExpression, RowExpression> mapping;

        public ExpressionReplacer(PlanNodeIdAllocator idAllocator, Map<RowExpression, RowExpression> mapping)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.mapping = requireNonNull(mapping, "mapping is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            ProjectNode rewrittenNode = (ProjectNode) context.defaultRewrite(node);

            Assignments assignments = AssignmentUtils.rewrite(rewrittenNode.getAssignments(), expression -> replaceExpression(expression, mapping));

            return new ProjectNode(idAllocator.getNextId(), rewrittenNode.getSource(), assignments);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            FilterNode rewrittenNode = (FilterNode) context.defaultRewrite(node);
            return new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), rewrittenNode.getSource(), replaceExpression(rewrittenNode.getPredicate(), mapping));
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
        {
            ValuesNode rewrittenNode = (ValuesNode) context.defaultRewrite(node);
            List<List<RowExpression>> rewrittenRows = rewrittenNode.getRows().stream()
                    .map(row -> row.stream()
                            .map(column -> replaceExpression(column, mapping))
                            .collect(toImmutableList()))
                    .collect(toImmutableList());
            return new ValuesNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    rewrittenNode.getOutputVariables(),
                    rewrittenRows,
                    node.getValuesNodeLabel());
        }
    }

    private boolean detectAntiJoinInExpression(Expression expression)
    {
        AntiJoinDetector antiJoinDetector = new AntiJoinDetector();
        antiJoinDetector.process(expression);
        return antiJoinDetector.getMayContainAntiJoin();
    }

    private static class AntiJoinDetector
            extends DefaultExpressionTraversalVisitor<Void, Void>
    {
        private boolean mayContainAntiJoin;

        @Override
        protected Void visitNotExpression(NotExpression node, Void context)
        {
            mayContainAntiJoin = true;
            return null;
        }

        @Override
        protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            if (node.getOperator() == OR) {
                mayContainAntiJoin = true;
                return null;
            }
            process(node.getRight(), context);
            process(node.getLeft(), context);
            return null;
        }

        @Override
        protected Void visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            mayContainAntiJoin = true;
            return null;
        }

        @Override
        protected Void visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            mayContainAntiJoin = true;
            return null;
        }

        public boolean getMayContainAntiJoin()
        {
            return mayContainAntiJoin;
        }
    }
}
