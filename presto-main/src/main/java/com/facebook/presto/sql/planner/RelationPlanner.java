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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.Approximate;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.facebook.presto.sql.tree.Join.Type.INNER;
import static com.facebook.presto.type.TypeRegistry.isTypeOnlyCoercion;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void>
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;

    RelationPlanner(Analysis analysis, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        this.analysis = analysis;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
        this.session = session;
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        Query namedQuery = analysis.getNamedQuery(node);
        RelationType tableType = analysis.getOutputDescriptor(node);

        if (namedQuery != null) {
            RelationPlan subPlan = process(namedQuery, null);

            // Add implicit coercions if view query produces types that don't match the declared output types
            // of the view (e.g., if the underlying tables referenced by the view changed)
            Type[] types = tableType.getAllFields().stream().map(Field::getType).toArray(Type[]::new);
            RelationPlan withCoercions = addCoercions(subPlan, types);
            return new RelationPlan(withCoercions.getRoot(), tableType, withCoercions.getOutputSymbols(), withCoercions.getSampleWeight());
        }

        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : tableType.getAllFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field.getName().get(), field.getType());

            outputSymbolsBuilder.add(symbol);
            columns.put(symbol, analysis.getColumn(field));
        }

        List<Symbol> planOutputSymbols = outputSymbolsBuilder.build();
        Optional<ColumnHandle> sampleWeightColumn = metadata.getSampleWeightColumnHandle(session, handle);
        Symbol sampleWeightSymbol = null;
        if (sampleWeightColumn.isPresent()) {
            sampleWeightSymbol = symbolAllocator.newSymbol("$sampleWeight", BIGINT);
            outputSymbolsBuilder.add(sampleWeightSymbol);
            columns.put(sampleWeightSymbol, sampleWeightColumn.get());
        }

        List<Symbol> nodeOutputSymbols = outputSymbolsBuilder.build();
        PlanNode root = new TableScanNode(idAllocator.getNextId(), handle, nodeOutputSymbols, columns.build(), Optional.empty(), TupleDomain.all(), null);
        return new RelationPlan(root, tableType, planOutputSymbols, Optional.ofNullable(sampleWeightSymbol));
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        RelationType outputDescriptor = analysis.getOutputDescriptor(node);

        return new RelationPlan(subPlan.getRoot(), outputDescriptor, subPlan.getOutputSymbols(), subPlan.getSampleWeight());
    }

    @Override
    protected RelationPlan visitSampledRelation(SampledRelation node, Void context)
    {
        if (node.getColumnsToStratifyOn().isPresent()) {
            throw new UnsupportedOperationException("STRATIFY ON is not yet implemented");
        }

        RelationPlan subPlan = process(node.getRelation(), context);

        RelationType outputDescriptor = analysis.getOutputDescriptor(node);
        double ratio = analysis.getSampleRatio(node);
        Symbol sampleWeightSymbol = null;
        if (node.getType() == SampledRelation.Type.POISSONIZED) {
            sampleWeightSymbol = symbolAllocator.newSymbol("$sampleWeight", BIGINT);
        }
        PlanNode planNode = new SampleNode(idAllocator.getNextId(),
                subPlan.getRoot(),
                ratio,
                SampleNode.Type.fromType(node.getType()),
                node.isRescaled(),
                Optional.ofNullable(sampleWeightSymbol));
        return new RelationPlan(planNode, outputDescriptor, subPlan.getOutputSymbols(), Optional.ofNullable(sampleWeightSymbol));
    }

    @Override
    protected RelationPlan visitJoin(Join node, Void context)
    {
        // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
        RelationPlan leftPlan = process(node.getLeft(), context);

        // Convert CROSS JOIN UNNEST to an UnnestNode
        if (node.getRight() instanceof Unnest || (node.getRight() instanceof AliasedRelation && ((AliasedRelation) node.getRight()).getRelation() instanceof Unnest)) {
            Unnest unnest;
            if (node.getRight() instanceof AliasedRelation) {
                unnest = (Unnest) ((AliasedRelation) node.getRight()).getRelation();
            }
            else {
                unnest = (Unnest) node.getRight();
            }
            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                throw new SemanticException(NOT_SUPPORTED, unnest, "UNNEST only supported on the right side of CROSS JOIN");
            }
            return planCrossJoinUnnest(leftPlan, node, unnest);
        }

        RelationPlan rightPlan = process(node.getRight(), context);

        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        RelationType outputDescriptor = analysis.getOutputDescriptor(node);

        // NOTE: symbols must be in the same order as the outputDescriptor
        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getOutputSymbols())
                .addAll(rightPlan.getOutputSymbols())
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
            List<ComparisonExpression.Type> joinConditionComparisonTypes = new ArrayList<>();

            for (Expression conjunct : ExpressionUtils.extractConjuncts(criteria)) {
                conjunct = ExpressionUtils.normalize(conjunct);
                if (!isEqualComparisonExpression(conjunct) && node.getType() != INNER) {
                    throw new SemanticException(NOT_SUPPORTED, node, "Non-equi joins only supported for inner join: %s", conjunct);
                }

                if (conjunct instanceof ComparisonExpression) {
                    Expression firstExpression = ((ComparisonExpression) conjunct).getLeft();
                    Expression secondExpression = ((ComparisonExpression) conjunct).getRight();
                    ComparisonExpression.Type comparisonType = ((ComparisonExpression) conjunct).getType();
                    Set<QualifiedName> firstDependencies = DependencyExtractor.extractNames(firstExpression, analysis.getColumnReferences());
                    Set<QualifiedName> secondDependencies = DependencyExtractor.extractNames(secondExpression, analysis.getColumnReferences());

                    if (firstDependencies.stream().allMatch(left.canResolvePredicate()) && secondDependencies.stream().allMatch(right.canResolvePredicate())) {
                        leftComparisonExpressions.add(firstExpression);
                        rightComparisonExpressions.add(secondExpression);
                        joinConditionComparisonTypes.add(comparisonType);
                    }
                    else if (firstDependencies.stream().allMatch(right.canResolvePredicate()) && secondDependencies.stream().allMatch(left.canResolvePredicate())) {
                        leftComparisonExpressions.add(secondExpression);
                        rightComparisonExpressions.add(firstExpression);
                        joinConditionComparisonTypes.add(comparisonType.flip());
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

            Analysis.JoinInPredicates joinInPredicates = analysis.getJoinInPredicates(node);

            // Add semi joins if necessary
            if (joinInPredicates != null) {
                leftPlanBuilder = appendSemiJoins(leftPlanBuilder, joinInPredicates.getLeftInPredicates());
                rightPlanBuilder = appendSemiJoins(rightPlanBuilder, joinInPredicates.getRightInPredicates());
            }

            // Add projections for join criteria
            leftPlanBuilder = appendProjections(leftPlanBuilder, leftComparisonExpressions);
            rightPlanBuilder = appendProjections(rightPlanBuilder, rightComparisonExpressions);

            for (int i = 0; i < leftComparisonExpressions.size(); i++) {
                Symbol leftSymbol = leftPlanBuilder.translate(leftComparisonExpressions.get(i));
                Symbol rightSymbol = rightPlanBuilder.translate(rightComparisonExpressions.get(i));

                if (joinConditionComparisonTypes.get(i) == ComparisonExpression.Type.EQUAL) {
                    equiClauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
                }
                else {
                    Expression leftExpression = leftPlanBuilder.rewrite(leftComparisonExpressions.get(i));
                    Expression rightExpression = rightPlanBuilder.rewrite(rightComparisonExpressions.get(i));
                    postInnerJoinConditions.add(new ComparisonExpression(joinConditionComparisonTypes.get(i), leftExpression, rightExpression));
                }
            }
        }

        PlanNode root = new JoinNode(idAllocator.getNextId(),
                JoinNode.Type.typeConvert(node.getType()),
                leftPlanBuilder.getRoot(),
                rightPlanBuilder.getRoot(),
                equiClauses.build(),
                Optional.empty(),
                Optional.empty());

        Optional<Symbol> sampleWeight = Optional.empty();
        if (leftPlanBuilder.getSampleWeight().isPresent() || rightPlanBuilder.getSampleWeight().isPresent()) {
            Expression expression = new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Type.MULTIPLY,
                    oneIfNull(leftPlanBuilder.getSampleWeight()),
                    oneIfNull(rightPlanBuilder.getSampleWeight()));
            sampleWeight = Optional.of(symbolAllocator.newSymbol(expression, BIGINT));
            ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
            projections.put(sampleWeight.get(), expression);
            for (Symbol symbol : root.getOutputSymbols()) {
                projections.put(symbol, new QualifiedNameReference(symbol.toQualifiedName()));
            }
            root = new ProjectNode(idAllocator.getNextId(), root, projections.build());
        }

        // rewrite all the other conditions using output symbols from left + right plan node.
        RelationPlan intermediateRootRelationPlan = new RelationPlan(root, outputDescriptor, outputSymbols, sampleWeight);
        TranslationMap translationMap = new TranslationMap(intermediateRootRelationPlan, analysis);
        translationMap.setFieldMappings(outputSymbols);
        translationMap.putExpressionMappingsFrom(leftPlanBuilder.getTranslations());
        translationMap.putExpressionMappingsFrom(rightPlanBuilder.getTranslations());
        PlanBuilder rootPlanBuilder = new PlanBuilder(translationMap, root, sampleWeight);
        List<InPredicate> inPredicates = analysis.getInPredicates(node);
        if (inPredicates != null) {
            rootPlanBuilder = appendSemiJoins(rootPlanBuilder, inPredicates);
        }
        for (Expression expression : complexJoinExpressions) {
            postInnerJoinConditions.add(rootPlanBuilder.rewrite(expression));
        }
        root = rootPlanBuilder.getRoot();

        Expression postInnerJoinCriteria;
        if (!postInnerJoinConditions.isEmpty()) {
            postInnerJoinCriteria = ExpressionUtils.and(postInnerJoinConditions);
        }
        else {
            postInnerJoinCriteria = new BooleanLiteral("TRUE");
        }

        if (node.getType() == INNER) {
            root = new FilterNode(idAllocator.getNextId(), root, postInnerJoinCriteria);
        }

        return new RelationPlan(root, outputDescriptor, outputSymbols, sampleWeight);
    }

    private boolean isEqualComparisonExpression(Expression conjunct)
    {
        return conjunct instanceof ComparisonExpression && ((ComparisonExpression) conjunct).getType() == ComparisonExpression.Type.EQUAL;
    }

    private RelationPlan planCrossJoinUnnest(RelationPlan leftPlan, Join joinNode, Unnest node)
    {
        RelationType outputDescriptor = analysis.getOutputDescriptor(joinNode);
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
        planBuilder = appendProjections(planBuilder, node.getExpressions());
        TranslationMap translations = planBuilder.getTranslations();
        ProjectNode projectNode = checkType(planBuilder.getRoot(), ProjectNode.class, "planBuilder.getRoot()");

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

        UnnestNode unnestNode = new UnnestNode(idAllocator.getNextId(), projectNode, leftPlan.getOutputSymbols(), unnestSymbols.build(), ordinalitySymbol);
        return new RelationPlan(unnestNode, outputDescriptor, unnestNode.getOutputSymbols(), Optional.empty());
    }

    private static Expression oneIfNull(Optional<Symbol> symbol)
    {
        if (symbol.isPresent()) {
            return new CoalesceExpression(new QualifiedNameReference(symbol.get().toQualifiedName()), new LongLiteral("1"));
        }
        else {
            return new LongLiteral("1");
        }
    }

    @Override
    protected RelationPlan visitTableSubquery(TableSubquery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        Optional<Double> approximationConfidence = node.getApproximate().map(Approximate::getConfidence).map(confidence -> Double.valueOf(confidence) / 100.0);
        return new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session, approximationConfidence)
                .plan(node);
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session, Optional.empty())
                .plan(node);
    }

    @Override
    protected RelationPlan visitValues(Values node, Void context)
    {
        RelationType descriptor = analysis.getOutputDescriptor(node);
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        for (Field field : descriptor.getVisibleFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field);
            outputSymbolsBuilder.add(symbol);
        }

        ImmutableList.Builder<List<Expression>> rows = ImmutableList.builder();
        for (Expression row : node.getRows()) {
            ImmutableList.Builder<Expression> values = ImmutableList.builder();
            if (row instanceof Row) {
                List<Expression> items = ((Row) row).getItems();
                for (int i = 0; i < items.size(); i++) {
                    Expression expression = items.get(i);
                    Object constantValue = evaluateConstantExpression(expression, analysis.getCoercions(), metadata, session, analysis.getColumnReferences());
                    values.add(LiteralInterpreter.toExpression(constantValue, descriptor.getFieldByIndex(i).getType()));
                }
            }
            else {
                Object constantValue = evaluateConstantExpression(row, analysis.getCoercions(), metadata, session, analysis.getColumnReferences());
                values.add(LiteralInterpreter.toExpression(constantValue, descriptor.getFieldByIndex(0).getType()));
            }

            rows.add(values.build());
        }

        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), outputSymbolsBuilder.build(), rows.build());
        return new RelationPlan(valuesNode, descriptor, outputSymbolsBuilder.build(), Optional.empty());
    }

    @Override
    protected RelationPlan visitUnnest(Unnest node, Void context)
    {
        RelationType descriptor = analysis.getOutputDescriptor(node);
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        for (Field field : descriptor.getVisibleFields()) {
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
            Object constantValue = evaluateConstantExpression(expression, analysis.getCoercions(), metadata, session, analysis.getColumnReferences());
            Type type = analysis.getType(expression);
            values.add(LiteralInterpreter.toExpression(constantValue, type));
            Symbol inputSymbol = symbolAllocator.newSymbol(expression, type);
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
        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), argumentSymbols.build(), ImmutableList.<List<Expression>>of(values.build()));

        UnnestNode unnestNode = new UnnestNode(idAllocator.getNextId(), valuesNode, ImmutableList.<Symbol>of(), unnestSymbols.build(), ordinalitySymbol);
        return new RelationPlan(unnestNode, descriptor, unnestedSymbols, Optional.empty());
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
        List<Symbol> oldSymbols = plan.getOutputSymbols();
        RelationType oldDescriptor = plan.getDescriptor().withOnlyVisibleFields();
        verify(targetColumnTypes.length == oldSymbols.size());
        ImmutableList.Builder<Symbol> newSymbols = new ImmutableList.Builder<>();
        Field[] newFields = new Field[targetColumnTypes.length];
        ImmutableMap.Builder<Symbol, Expression> assignments = new ImmutableMap.Builder<>();
        for (int i = 0; i < targetColumnTypes.length; i++) {
            Symbol inputSymbol = oldSymbols.get(i);
            Type inputType = symbolAllocator.getTypes().get(inputSymbol);
            Type outputType = targetColumnTypes[i];
            if (outputType != inputType && !isTypeOnlyCoercion(inputType.getTypeSignature(), outputType.getTypeSignature())) {
                Expression cast = new Cast(new QualifiedNameReference(inputSymbol.toQualifiedName()), outputType.getTypeSignature().toString());
                Symbol outputSymbol = symbolAllocator.newSymbol(cast, outputType);
                assignments.put(outputSymbol, cast);
                newSymbols.add(outputSymbol);
            }
            else {
                QualifiedNameReference qualifiedNameReference = new QualifiedNameReference(inputSymbol.toQualifiedName());
                Symbol outputSymbol = symbolAllocator.newSymbol(qualifiedNameReference, outputType);
                assignments.put(outputSymbol, qualifiedNameReference);
                newSymbols.add(outputSymbol);
            }
            Field oldField = oldDescriptor.getFieldByIndex(i);
            newFields[i] = new Field(oldField.getRelationAlias(), oldField.getName(), targetColumnTypes[i], oldField.isHidden());
        }
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());
        return new RelationPlan(projectNode, new RelationType(newFields), newSymbols.build(), plan.getSampleWeight());
    }

    @Override
    protected RelationPlan visitUnion(Union node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

        List<Symbol> unionOutputSymbols = null;
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.builder();
        List<RelationPlan> subPlans = node.getRelations().stream()
                .map(relation -> processAndCoerceIfNecessary(relation, context))
                .collect(toImmutableList());

        boolean hasSampleWeight = false;
        for (RelationPlan subPlan : subPlans) {
            if (subPlan.getSampleWeight().isPresent()) {
                hasSampleWeight = true;
                break;
            }
        }

        Optional<Symbol> outputSampleWeight = Optional.empty();
        for (RelationPlan relationPlan : subPlans) {
            if (hasSampleWeight && !relationPlan.getSampleWeight().isPresent()) {
                relationPlan = addConstantSampleWeight(relationPlan);
            }

            List<Symbol> childOutputSymbols = relationPlan.getOutputSymbols();
            if (unionOutputSymbols == null) {
                // Use the first Relation to derive output symbol names
                RelationType descriptor = relationPlan.getDescriptor();
                ImmutableList.Builder<Symbol> outputSymbolBuilder = ImmutableList.builder();
                for (Field field : descriptor.getVisibleFields()) {
                    int fieldIndex = descriptor.indexOf(field);
                    Symbol symbol = childOutputSymbols.get(fieldIndex);
                    outputSymbolBuilder.add(symbolAllocator.newSymbol(symbol.getName(), symbolAllocator.getTypes().get(symbol)));
                }
                unionOutputSymbols = outputSymbolBuilder.build();
                outputSampleWeight = relationPlan.getSampleWeight();
            }

            RelationType descriptor = relationPlan.getDescriptor();
            checkArgument(descriptor.getVisibleFieldCount() == unionOutputSymbols.size(),
                    "Expected relation to have %s symbols but has %s symbols",
                    descriptor.getVisibleFieldCount(),
                    unionOutputSymbols.size());

            int unionFieldId = 0;
            for (Field field : descriptor.getVisibleFields()) {
                int fieldIndex = descriptor.indexOf(field);
                symbolMapping.put(unionOutputSymbols.get(unionFieldId), childOutputSymbols.get(fieldIndex));
                unionFieldId++;
            }

            sources.add(relationPlan.getRoot());
        }

        PlanNode planNode = new UnionNode(idAllocator.getNextId(), sources.build(), symbolMapping.build(), ImmutableList.copyOf(symbolMapping.build().keySet()));
        if (node.isDistinct()) {
            planNode = distinct(planNode);
        }
        return new RelationPlan(planNode, analysis.getOutputDescriptor(node), planNode.getOutputSymbols(), outputSampleWeight);
    }

    private RelationPlan addConstantSampleWeight(RelationPlan subPlan)
    {
        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
        for (Symbol symbol : subPlan.getOutputSymbols()) {
            Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
            projections.put(symbol, expression);
        }
        Expression one = new LongLiteral("1");

        Symbol sampleWeightSymbol = symbolAllocator.newSymbol("$sampleWeight", BIGINT);
        projections.put(sampleWeightSymbol, one);
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build());
        return new RelationPlan(projectNode, subPlan.getDescriptor(), projectNode.getOutputSymbols(), Optional.of(sampleWeightSymbol));
    }

    private PlanBuilder initializePlanBuilder(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
        translations.setFieldMappings(relationPlan.getOutputSymbols());

        return new PlanBuilder(translations, relationPlan.getRoot(), relationPlan.getSampleWeight());
    }

    private PlanBuilder appendProjections(PlanBuilder subPlan, Iterable<Expression> expressions)
    {
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);

        // Carry over the translations from the source because we are appending projections
        translations.copyMappingsFrom(subPlan.getTranslations());

        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();

        // add an identity projection for underlying plan
        for (Symbol symbol : subPlan.getRoot().getOutputSymbols()) {
            Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
            projections.put(symbol, expression);
        }

        ImmutableMap.Builder<Symbol, Expression> newTranslations = ImmutableMap.builder();
        for (Expression expression : expressions) {
            Symbol symbol = symbolAllocator.newSymbol(expression, analysis.getTypeWithCoercions(expression));

            // TODO: CHECK IF THE REWRITE OF A SEMI JOINED EXPRESSION WILL WORK!!!!!!!

            projections.put(symbol, translations.rewrite(expression));
            newTranslations.put(symbol, expression);
        }
        // Now append the new translations into the TranslationMap
        for (Map.Entry<Symbol, Expression> entry : newTranslations.build().entrySet()) {
            translations.put(entry.getValue(), entry.getKey());
        }

        return new PlanBuilder(translations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()), subPlan.getSampleWeight());
    }

    private PlanBuilder appendSemiJoins(PlanBuilder subPlan, Iterable<InPredicate> inPredicates)
    {
        for (InPredicate inPredicate : inPredicates) {
            subPlan = appendSemiJoin(subPlan, inPredicate);
        }
        return subPlan;
    }

    private PlanBuilder appendSemiJoin(PlanBuilder subPlan, InPredicate inPredicate)
    {
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);
        translations.copyMappingsFrom(subPlan.getTranslations());

        subPlan = appendProjections(subPlan, ImmutableList.of(inPredicate.getValue()));
        Symbol sourceJoinSymbol = subPlan.translate(inPredicate.getValue());

        checkState(inPredicate.getValueList() instanceof SubqueryExpression);
        SubqueryExpression subqueryExpression = (SubqueryExpression) inPredicate.getValueList();
        RelationPlanner relationPlanner = new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session);
        RelationPlan valueListRelation = relationPlanner.process(subqueryExpression.getQuery(), null);
        Symbol filteringSourceJoinSymbol = Iterables.getOnlyElement(valueListRelation.getRoot().getOutputSymbols());

        Symbol semiJoinOutputSymbol = symbolAllocator.newSymbol("semijoinresult", BOOLEAN);

        translations.put(inPredicate, semiJoinOutputSymbol);

        return new PlanBuilder(translations,
                new SemiJoinNode(idAllocator.getNextId(),
                        subPlan.getRoot(),
                        valueListRelation.getRoot(),
                        sourceJoinSymbol,
                        filteringSourceJoinSymbol,
                        semiJoinOutputSymbol,
                        Optional.empty(),
                        Optional.empty()),
                subPlan.getSampleWeight());
    }

    private PlanNode distinct(PlanNode node)
    {
        return new AggregationNode(idAllocator.getNextId(),
                node,
                node.getOutputSymbols(),
                ImmutableMap.<Symbol, FunctionCall>of(),
                ImmutableMap.<Symbol, Signature>of(),
                ImmutableMap.<Symbol, Symbol>of(),
                ImmutableList.of(node.getOutputSymbols()),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                1.0,
                Optional.empty());
    }
}
