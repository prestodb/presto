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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.RelationId;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExceptNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.Lateral;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.UnmodifiableIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.SemanticExceptions.notSupportedException;
import static com.facebook.presto.sql.tree.Join.Type.INNER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void>
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final Metadata metadata;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;

    RelationPlanner(
            Analysis analysis,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap,
            Metadata metadata,
            Session session)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToSymbolMap, "lambdaDeclarationToSymbolMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToSymbolMap = lambdaDeclarationToSymbolMap;
        this.metadata = metadata;
        this.session = session;
        this.subqueryPlanner = new SubqueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session, analysis.getParameters());
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        Query namedQuery = analysis.getNamedQuery(node);
        Scope scope = analysis.getScope(node);

        if (namedQuery != null) {
            RelationPlan subPlan = process(namedQuery, null);

            // Add implicit coercions if view query produces types that don't match the declared output types
            // of the view (e.g., if the underlying tables referenced by the view changed)
            Type[] types = scope.getRelationType().getAllFields().stream().map(Field::getType).toArray(Type[]::new);
            RelationPlan withCoercions = addCoercions(subPlan, types);
            return new RelationPlan(withCoercions.getRoot(), scope, withCoercions.getFieldMappings());
        }

        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : scope.getRelationType().getAllFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field.getName().get(), field.getType());

            outputSymbolsBuilder.add(symbol);
            columns.put(symbol, analysis.getColumn(field));
        }

        List<Symbol> outputSymbols = outputSymbolsBuilder.build();
        PlanNode root = new TableScanNode(idAllocator.getNextId(), handle, outputSymbols, columns.build(), Optional.empty(), TupleDomain.all(), null);
        return new RelationPlan(root, scope, outputSymbols);
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        return new RelationPlan(subPlan.getRoot(), analysis.getScope(node), subPlan.getFieldMappings());
    }

    @Override
    protected RelationPlan visitSampledRelation(SampledRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        double ratio = analysis.getSampleRatio(node);
        PlanNode planNode = new SampleNode(idAllocator.getNextId(),
                subPlan.getRoot(),
                ratio,
                SampleNode.Type.fromType(node.getType()));
        return new RelationPlan(planNode, analysis.getScope(node), subPlan.getFieldMappings());
    }

    @Override
    protected RelationPlan visitJoin(Join node, Void context)
    {
        // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
        RelationPlan leftPlan = process(node.getLeft(), context);

        Optional<Unnest> unnest = getUnnest(node.getRight());
        if (unnest.isPresent()) {
            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                throw notSupportedException(unnest.get(), "UNNEST on other than the right side of CROSS JOIN");
            }
            return planCrossJoinUnnest(leftPlan, node, unnest.get());
        }

        Optional<Lateral> lateral = getLateral(node.getRight());
        if (lateral.isPresent()) {
            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                throw notSupportedException(lateral.get(), "LATERAL on other than the right side of CROSS JOIN");
            }
            return planLateralJoin(node, leftPlan, lateral.get());
        }

        RelationPlan rightPlan = process(node.getRight(), context);

        if (!SystemSessionProperties.isLegacyJoinUsingEnabled(session) &&
                node.getCriteria().isPresent() && node.getCriteria().get() instanceof JoinUsing) {
            return planJoinUsing(node, leftPlan, rightPlan);
        }

        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        // NOTE: symbols must be in the same order as the outputDescriptor
        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getFieldMappings())
                .addAll(rightPlan.getFieldMappings())
                .build();

        ImmutableList.Builder<JoinNode.EquiJoinClause> equiClauses = ImmutableList.builder();
        List<Expression> complexJoinExpressions = new ArrayList<>();
        List<Expression> postInnerJoinConditions = new ArrayList<>();

        if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
            Expression criteria = analysis.getJoinCriteria(node);

            RelationType left = analysis.getOutputDescriptor(node.getLeft());
            RelationType right = analysis.getOutputDescriptor(node.getRight());

            List<Expression> leftComparisonExpressions = new ArrayList<>();
            List<Expression> rightComparisonExpressions = new ArrayList<>();
            List<ComparisonExpression.Operator> joinConditionComparisonOperators = new ArrayList<>();

            for (Expression conjunct : ExpressionUtils.extractConjuncts(criteria)) {
                conjunct = ExpressionUtils.normalize(conjunct);

                if (!isEqualComparisonExpression(conjunct) && node.getType() != INNER) {
                    complexJoinExpressions.add(conjunct);
                    continue;
                }

                Set<QualifiedName> dependencies = SymbolsExtractor.extractNames(conjunct, analysis.getColumnReferences());

                boolean isJoinUsing = node.getCriteria().filter(JoinUsing.class::isInstance).isPresent();
                checkState(!isJoinUsing || SystemSessionProperties.isLegacyJoinUsingEnabled(session));
                if (!isJoinUsing && (dependencies.stream().allMatch(left::canResolve) || dependencies.stream().allMatch(right::canResolve))) {
                    // If the conjunct can be evaluated entirely with the inputs on either side of the join, add
                    // it to the list complex expressions and let the optimizers figure out how to push it down later.
                    // Due to legacy reasons, the expression for "join using" looks like "x = x", which (incorrectly)
                    // appears to fit the condition we're after. So we skip them.

                    complexJoinExpressions.add(conjunct);
                }
                else if (conjunct instanceof ComparisonExpression) {
                    Expression firstExpression = ((ComparisonExpression) conjunct).getLeft();
                    Expression secondExpression = ((ComparisonExpression) conjunct).getRight();
                    ComparisonExpression.Operator comparisonOperator = ((ComparisonExpression) conjunct).getOperator();
                    Set<QualifiedName> firstDependencies = SymbolsExtractor.extractNames(firstExpression, analysis.getColumnReferences());
                    Set<QualifiedName> secondDependencies = SymbolsExtractor.extractNames(secondExpression, analysis.getColumnReferences());

                    if (firstDependencies.stream().allMatch(left::canResolve) && secondDependencies.stream().allMatch(right::canResolve)) {
                        leftComparisonExpressions.add(firstExpression);
                        rightComparisonExpressions.add(secondExpression);
                        joinConditionComparisonOperators.add(comparisonOperator);
                    }
                    else if (firstDependencies.stream().allMatch(right::canResolve) && secondDependencies.stream().allMatch(left::canResolve)) {
                        leftComparisonExpressions.add(secondExpression);
                        rightComparisonExpressions.add(firstExpression);
                        joinConditionComparisonOperators.add(comparisonOperator.flip());
                    }
                    else {
                        // the case when we mix symbols from both left and right join side on either side of condition.
                        complexJoinExpressions.add(conjunct);
                    }
                }
                else {
                    complexJoinExpressions.add(conjunct);
                }
            }

            leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder, leftComparisonExpressions, node);
            rightPlanBuilder = subqueryPlanner.handleSubqueries(rightPlanBuilder, rightComparisonExpressions, node);

            // Add projections for join criteria
            leftPlanBuilder = leftPlanBuilder.appendProjections(leftComparisonExpressions, symbolAllocator, idAllocator);
            rightPlanBuilder = rightPlanBuilder.appendProjections(rightComparisonExpressions, symbolAllocator, idAllocator);

            for (int i = 0; i < leftComparisonExpressions.size(); i++) {
                if (joinConditionComparisonOperators.get(i) == ComparisonExpression.Operator.EQUAL) {
                    Symbol leftSymbol = leftPlanBuilder.translate(leftComparisonExpressions.get(i));
                    Symbol rightSymbol = rightPlanBuilder.translate(rightComparisonExpressions.get(i));

                    equiClauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
                }
                else {
                    Expression leftExpression = leftPlanBuilder.rewrite(leftComparisonExpressions.get(i));
                    Expression rightExpression = rightPlanBuilder.rewrite(rightComparisonExpressions.get(i));
                    postInnerJoinConditions.add(new ComparisonExpression(joinConditionComparisonOperators.get(i), leftExpression, rightExpression));
                }
            }
        }

        PlanNode root = new JoinNode(idAllocator.getNextId(),
                JoinNode.Type.typeConvert(node.getType()),
                leftPlanBuilder.getRoot(),
                rightPlanBuilder.getRoot(),
                equiClauses.build(),
                ImmutableList.<Symbol>builder()
                        .addAll(leftPlanBuilder.getRoot().getOutputSymbols())
                        .addAll(rightPlanBuilder.getRoot().getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        if (node.getType() != INNER) {
            for (Expression complexExpression : complexJoinExpressions) {
                Set<InPredicate> inPredicates = subqueryPlanner.collectInPredicateSubqueries(complexExpression, node);
                if (!inPredicates.isEmpty()) {
                    InPredicate inPredicate = Iterables.getLast(inPredicates);
                    throw notSupportedException(inPredicate, "IN with subquery predicate in join condition");
                }
            }

            // subqueries can be applied only to one side of join - left side is selected in arbitrary way
            leftPlanBuilder = subqueryPlanner.handleUncorrelatedSubqueries(leftPlanBuilder, complexJoinExpressions, node);
        }

        RelationPlan intermediateRootRelationPlan = new RelationPlan(root, analysis.getScope(node), outputSymbols);
        TranslationMap translationMap = new TranslationMap(intermediateRootRelationPlan, analysis, lambdaDeclarationToSymbolMap);
        translationMap.setFieldMappings(outputSymbols);
        translationMap.putExpressionMappingsFrom(leftPlanBuilder.getTranslations());
        translationMap.putExpressionMappingsFrom(rightPlanBuilder.getTranslations());

        if (node.getType() != INNER && !complexJoinExpressions.isEmpty()) {
            Expression joinedFilterCondition = ExpressionUtils.and(complexJoinExpressions);
            Expression rewrittenFilterCondition = translationMap.rewrite(joinedFilterCondition);
            root = new JoinNode(idAllocator.getNextId(),
                    JoinNode.Type.typeConvert(node.getType()),
                    leftPlanBuilder.getRoot(),
                    rightPlanBuilder.getRoot(),
                    equiClauses.build(),
                    ImmutableList.<Symbol>builder()
                            .addAll(leftPlanBuilder.getRoot().getOutputSymbols())
                            .addAll(rightPlanBuilder.getRoot().getOutputSymbols())
                            .build(),
                    Optional.of(rewrittenFilterCondition),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        if (node.getType() == INNER) {
            // rewrite all the other conditions using output symbols from left + right plan node.
            PlanBuilder rootPlanBuilder = new PlanBuilder(translationMap, root, analysis.getParameters());
            rootPlanBuilder = subqueryPlanner.handleSubqueries(rootPlanBuilder, complexJoinExpressions, node);

            for (Expression expression : complexJoinExpressions) {
                postInnerJoinConditions.add(rootPlanBuilder.rewrite(expression));
            }
            root = rootPlanBuilder.getRoot();

            Expression postInnerJoinCriteria;
            if (!postInnerJoinConditions.isEmpty()) {
                postInnerJoinCriteria = ExpressionUtils.and(postInnerJoinConditions);
                root = new FilterNode(idAllocator.getNextId(), root, postInnerJoinCriteria);
            }
        }

        return new RelationPlan(root, analysis.getScope(node), outputSymbols);
    }

    private RelationPlan planJoinUsing(Join node, RelationPlan left, RelationPlan right)
    {
        /* Given: l JOIN r USING (k1, ..., kn)

           produces:

            - project
                    coalesce(l.k1, r.k1)
                    ...,
                    coalesce(l.kn, r.kn)
                    l.v1,
                    ...,
                    l.vn,
                    r.v1,
                    ...,
                    r.vn
              - join (l.k1 = r.k1 and ... l.kn = r.kn)
                    - project
                        cast(l.k1 as commonType(l.k1, r.k1))
                        ...
                    - project
                        cast(rl.k1 as commonType(l.k1, r.k1))

            If casts are redundant (due to column type and common type being equal),
            they will be removed by optimization passes.
        */

        List<Identifier> joinColumns = ((JoinUsing) node.getCriteria().get()).getColumns();

        Analysis.JoinUsingAnalysis joinAnalysis = analysis.getJoinUsing(node);

        ImmutableList.Builder<JoinNode.EquiJoinClause> clauses = ImmutableList.builder();

        Map<Identifier, Symbol> leftJoinColumns = new HashMap<>();
        Map<Identifier, Symbol> rightJoinColumns = new HashMap<>();

        Assignments.Builder leftCoercions = Assignments.builder();
        Assignments.Builder rightCoercions = Assignments.builder();

        leftCoercions.putIdentities(left.getRoot().getOutputSymbols());
        rightCoercions.putIdentities(right.getRoot().getOutputSymbols());
        for (int i = 0; i < joinColumns.size(); i++) {
            Identifier identifier = joinColumns.get(i);
            Type type = analysis.getType(identifier);

            // compute the coercion for the field on the left to the common supertype of left & right
            Symbol leftOutput = symbolAllocator.newSymbol(identifier, type);
            int leftField = joinAnalysis.getLeftJoinFields().get(i);
            leftCoercions.put(leftOutput, new Cast(
                    left.getSymbol(leftField).toSymbolReference(),
                    type.getTypeSignature().toString(),
                    false,
                    metadata.getTypeManager().isTypeOnlyCoercion(left.getDescriptor().getFieldByIndex(leftField).getType(), type)));
            leftJoinColumns.put(identifier, leftOutput);

            // compute the coercion for the field on the right to the common supertype of left & right
            Symbol rightOutput = symbolAllocator.newSymbol(identifier, type);
            int rightField = joinAnalysis.getRightJoinFields().get(i);
            rightCoercions.put(rightOutput, new Cast(
                    right.getSymbol(rightField).toSymbolReference(),
                    type.getTypeSignature().toString(),
                    false,
                    metadata.getTypeManager().isTypeOnlyCoercion(right.getDescriptor().getFieldByIndex(rightField).getType(), type)));
            rightJoinColumns.put(identifier, rightOutput);

            clauses.add(new JoinNode.EquiJoinClause(leftOutput, rightOutput));
        }

        ProjectNode leftCoercion = new ProjectNode(idAllocator.getNextId(), left.getRoot(), leftCoercions.build());
        ProjectNode rightCoercion = new ProjectNode(idAllocator.getNextId(), right.getRoot(), rightCoercions.build());

        JoinNode join = new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.typeConvert(node.getType()),
                leftCoercion,
                rightCoercion,
                clauses.build(),
                ImmutableList.<Symbol>builder()
                        .addAll(leftCoercion.getOutputSymbols())
                        .addAll(rightCoercion.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // Add a projection to produce the outputs of the columns in the USING clause,
        // which are defined as coalesce(l.k, r.k)
        Assignments.Builder assignments = Assignments.builder();

        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        for (Identifier column : joinColumns) {
            Symbol output = symbolAllocator.newSymbol(column, analysis.getType(column));
            outputs.add(output);
            assignments.put(output, new CoalesceExpression(
                    leftJoinColumns.get(column).toSymbolReference(),
                    rightJoinColumns.get(column).toSymbolReference()));
        }

        for (int field : joinAnalysis.getOtherLeftFields()) {
            Symbol symbol = left.getFieldMappings().get(field);
            outputs.add(symbol);
            assignments.put(symbol, symbol.toSymbolReference());
        }

        for (int field : joinAnalysis.getOtherRightFields()) {
            Symbol symbol = right.getFieldMappings().get(field);
            outputs.add(symbol);
            assignments.put(symbol, symbol.toSymbolReference());
        }

        return new RelationPlan(
                new ProjectNode(idAllocator.getNextId(), join, assignments.build()),
                analysis.getScope(node),
                outputs.build());
    }

    private Optional<Unnest> getUnnest(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return getUnnest(((AliasedRelation) relation).getRelation());
        }
        if (relation instanceof Unnest) {
            return Optional.of((Unnest) relation);
        }
        return Optional.empty();
    }

    private Optional<Lateral> getLateral(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return getLateral(((AliasedRelation) relation).getRelation());
        }
        if (relation instanceof Lateral) {
            return Optional.of((Lateral) relation);
        }
        return Optional.empty();
    }

    private RelationPlan planLateralJoin(Join join, RelationPlan leftPlan, Lateral lateral)
    {
        RelationPlan rightPlan = process(lateral.getQuery(), null);
        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        PlanBuilder planBuilder = subqueryPlanner.appendLateralJoin(leftPlanBuilder, rightPlanBuilder, lateral.getQuery(), true, LateralJoinNode.Type.INNER);

        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getRoot().getOutputSymbols())
                .addAll(rightPlan.getRoot().getOutputSymbols())
                .build();
        return new RelationPlan(planBuilder.getRoot(), analysis.getScope(join), outputSymbols);
    }

    private static boolean isEqualComparisonExpression(Expression conjunct)
    {
        return conjunct instanceof ComparisonExpression && ((ComparisonExpression) conjunct).getOperator() == ComparisonExpression.Operator.EQUAL;
    }

    private RelationPlan planCrossJoinUnnest(RelationPlan leftPlan, Join joinNode, Unnest node)
    {
        RelationType unnestOutputDescriptor = analysis.getOutputDescriptor(node);
        // Create symbols for the result of unnesting
        ImmutableList.Builder<Symbol> unnestedSymbolsBuilder = ImmutableList.builder();
        for (Field field : unnestOutputDescriptor.getVisibleFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field);
            unnestedSymbolsBuilder.add(symbol);
        }
        ImmutableList<Symbol> unnestedSymbols = unnestedSymbolsBuilder.build();

        // Add a projection for all the unnest arguments
        PlanBuilder planBuilder = initializePlanBuilder(leftPlan);
        planBuilder = planBuilder.appendProjections(node.getExpressions(), symbolAllocator, idAllocator);
        TranslationMap translations = planBuilder.getTranslations();
        ProjectNode projectNode = (ProjectNode) planBuilder.getRoot();

        ImmutableMap.Builder<Symbol, List<Symbol>> unnestSymbols = ImmutableMap.builder();
        UnmodifiableIterator<Symbol> unnestedSymbolsIterator = unnestedSymbols.iterator();
        for (Expression expression : node.getExpressions()) {
            Type type = analysis.getType(expression);
            Symbol inputSymbol = translations.get(expression);
            if (type instanceof ArrayType) {
                unnestSymbols.put(inputSymbol, ImmutableList.of(unnestedSymbolsIterator.next()));
            }
            else if (type instanceof MapType) {
                unnestSymbols.put(inputSymbol, ImmutableList.of(unnestedSymbolsIterator.next(), unnestedSymbolsIterator.next()));
            }
            else {
                throw new IllegalArgumentException("Unsupported type for UNNEST: " + type);
            }
        }
        Optional<Symbol> ordinalitySymbol = node.isWithOrdinality() ? Optional.of(unnestedSymbolsIterator.next()) : Optional.empty();
        checkState(!unnestedSymbolsIterator.hasNext(), "Not all output symbols were matched with input symbols");

        UnnestNode unnestNode = new UnnestNode(idAllocator.getNextId(), projectNode, leftPlan.getFieldMappings(), unnestSymbols.build(), ordinalitySymbol);
        return new RelationPlan(unnestNode, analysis.getScope(joinNode), unnestNode.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitTableSubquery(TableSubquery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        return new QueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session)
                .plan(node);
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, symbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session)
                .plan(node);
    }

    @Override
    protected RelationPlan visitValues(Values node, Void context)
    {
        Scope scope = analysis.getScope(node);
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        for (Field field : scope.getRelationType().getVisibleFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field);
            outputSymbolsBuilder.add(symbol);
        }

        ImmutableList.Builder<List<Expression>> rows = ImmutableList.builder();
        for (Expression row : node.getRows()) {
            ImmutableList.Builder<Expression> values = ImmutableList.builder();
            if (row instanceof Row) {
                for (Expression item : ((Row) row).getItems()) {
                    Expression expression = Coercer.addCoercions(item, analysis);
                    values.add(ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis.getParameters(), analysis), expression));
                }
            }
            else {
                Expression expression = Coercer.addCoercions(row, analysis);
                values.add(ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis.getParameters(), analysis), expression));
            }

            rows.add(values.build());
        }

        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), outputSymbolsBuilder.build(), rows.build());
        return new RelationPlan(valuesNode, scope, outputSymbolsBuilder.build());
    }

    @Override
    protected RelationPlan visitUnnest(Unnest node, Void context)
    {
        Scope scope = analysis.getScope(node);
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        for (Field field : scope.getRelationType().getVisibleFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field);
            outputSymbolsBuilder.add(symbol);
        }
        List<Symbol> unnestedSymbols = outputSymbolsBuilder.build();

        // If we got here, then we must be unnesting a constant, and not be in a join (where there could be column references)
        ImmutableList.Builder<Symbol> argumentSymbols = ImmutableList.builder();
        ImmutableList.Builder<Expression> values = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, List<Symbol>> unnestSymbols = ImmutableMap.builder();
        Iterator<Symbol> unnestedSymbolsIterator = unnestedSymbols.iterator();
        for (Expression expression : node.getExpressions()) {
            Type type = analysis.getType(expression);
            Expression rewritten = Coercer.addCoercions(expression, analysis);
            rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis.getParameters(), analysis), rewritten);
            values.add(rewritten);
            Symbol inputSymbol = symbolAllocator.newSymbol(rewritten, type);
            argumentSymbols.add(inputSymbol);
            if (type instanceof ArrayType) {
                unnestSymbols.put(inputSymbol, ImmutableList.of(unnestedSymbolsIterator.next()));
            }
            else if (type instanceof MapType) {
                unnestSymbols.put(inputSymbol, ImmutableList.of(unnestedSymbolsIterator.next(), unnestedSymbolsIterator.next()));
            }
            else {
                throw new IllegalArgumentException("Unsupported type for UNNEST: " + type);
            }
        }
        Optional<Symbol> ordinalitySymbol = node.isWithOrdinality() ? Optional.of(unnestedSymbolsIterator.next()) : Optional.empty();
        checkState(!unnestedSymbolsIterator.hasNext(), "Not all output symbols were matched with input symbols");
        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), argumentSymbols.build(), ImmutableList.of(values.build()));

        UnnestNode unnestNode = new UnnestNode(idAllocator.getNextId(), valuesNode, ImmutableList.of(), unnestSymbols.build(), ordinalitySymbol);
        return new RelationPlan(unnestNode, scope, unnestedSymbols);
    }

    private RelationPlan processAndCoerceIfNecessary(Relation node, Void context)
    {
        Type[] coerceToTypes = analysis.getRelationCoercion(node);

        RelationPlan plan = this.process(node, context);

        if (coerceToTypes == null) {
            return plan;
        }

        return addCoercions(plan, coerceToTypes);
    }

    private RelationPlan addCoercions(RelationPlan plan, Type[] targetColumnTypes)
    {
        List<Symbol> oldSymbols = plan.getFieldMappings();
        RelationType oldDescriptor = plan.getDescriptor().withOnlyVisibleFields();
        verify(targetColumnTypes.length == oldSymbols.size());
        ImmutableList.Builder<Symbol> newSymbols = new ImmutableList.Builder<>();
        Field[] newFields = new Field[targetColumnTypes.length];
        Assignments.Builder assignments = Assignments.builder();
        for (int i = 0; i < targetColumnTypes.length; i++) {
            Symbol inputSymbol = oldSymbols.get(i);
            Type inputType = symbolAllocator.getTypes().get(inputSymbol);
            Type outputType = targetColumnTypes[i];
            if (!outputType.equals(inputType)) {
                Expression cast = new Cast(inputSymbol.toSymbolReference(), outputType.getTypeSignature().toString());
                Symbol outputSymbol = symbolAllocator.newSymbol(cast, outputType);
                assignments.put(outputSymbol, cast);
                newSymbols.add(outputSymbol);
            }
            else {
                SymbolReference symbolReference = inputSymbol.toSymbolReference();
                Symbol outputSymbol = symbolAllocator.newSymbol(symbolReference, outputType);
                assignments.put(outputSymbol, symbolReference);
                newSymbols.add(outputSymbol);
            }
            Field oldField = oldDescriptor.getFieldByIndex(i);
            newFields[i] = new Field(
                    oldField.getRelationAlias(),
                    oldField.getName(),
                    targetColumnTypes[i],
                    oldField.isHidden(),
                    oldField.getOriginTable(),
                    oldField.isAliased());
        }
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());
        return new RelationPlan(projectNode, Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(newFields)).build(), newSymbols.build());
    }

    @Override
    protected RelationPlan visitUnion(Union node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new UnionNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()));
        if (node.isDistinct()) {
            planNode = distinct(planNode);
        }
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitIntersect(Intersect node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for INTERSECT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new IntersectNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()));
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitExcept(Except node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for EXCEPT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new ExceptNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()));
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols());
    }

    private SetOperationPlan process(SetOperation node)
    {
        List<Symbol> outputs = null;
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.builder();

        List<RelationPlan> subPlans = node.getRelations().stream()
                .map(relation -> processAndCoerceIfNecessary(relation, null))
                .collect(toImmutableList());

        for (RelationPlan relationPlan : subPlans) {
            List<Symbol> childOutputSymbols = relationPlan.getFieldMappings();
            if (outputs == null) {
                // Use the first Relation to derive output symbol names
                RelationType descriptor = relationPlan.getDescriptor();
                ImmutableList.Builder<Symbol> outputSymbolBuilder = ImmutableList.builder();
                for (Field field : descriptor.getVisibleFields()) {
                    int fieldIndex = descriptor.indexOf(field);
                    Symbol symbol = childOutputSymbols.get(fieldIndex);
                    outputSymbolBuilder.add(symbolAllocator.newSymbol(symbol.getName(), symbolAllocator.getTypes().get(symbol)));
                }
                outputs = outputSymbolBuilder.build();
            }

            RelationType descriptor = relationPlan.getDescriptor();
            checkArgument(descriptor.getVisibleFieldCount() == outputs.size(),
                    "Expected relation to have %s symbols but has %s symbols",
                    descriptor.getVisibleFieldCount(),
                    outputs.size());

            int fieldId = 0;
            for (Field field : descriptor.getVisibleFields()) {
                int fieldIndex = descriptor.indexOf(field);
                symbolMapping.put(outputs.get(fieldId), childOutputSymbols.get(fieldIndex));
                fieldId++;
            }

            sources.add(relationPlan.getRoot());
        }

        return new SetOperationPlan(sources.build(), symbolMapping.build());
    }

    private PlanBuilder initializePlanBuilder(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToSymbolMap);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        return new PlanBuilder(translations, relationPlan.getRoot(), analysis.getParameters());
    }

    private PlanNode distinct(PlanNode node)
    {
        return new AggregationNode(idAllocator.getNextId(),
                node,
                ImmutableMap.of(),
                ImmutableList.of(node.getOutputSymbols()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
    }

    private static class SetOperationPlan
    {
        private final List<PlanNode> sources;
        private final ListMultimap<Symbol, Symbol> symbolMapping;

        private SetOperationPlan(List<PlanNode> sources, ListMultimap<Symbol, Symbol> symbolMapping)
        {
            this.sources = sources;
            this.symbolMapping = symbolMapping;
        }

        public List<PlanNode> getSources()
        {
            return sources;
        }

        public ListMultimap<Symbol, Symbol> getSymbolMapping()
        {
            return symbolMapping;
        }
    }
}
