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
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.EquiJoinClause;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.LongLiteral;
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
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.analyzer.EquiJoinClause.rightGetter;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;
import static com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

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
        Preconditions.checkNotNull(analysis, "analysis is null");
        Preconditions.checkNotNull(symbolAllocator, "symbolAllocator is null");
        Preconditions.checkNotNull(idAllocator, "idAllocator is null");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(session, "session is null");

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
        if (namedQuery != null) {
            RelationPlan subPlan = process(namedQuery, null);
            return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node), subPlan.getOutputSymbols(), subPlan.getSampleWeight());
        }

        TupleDescriptor descriptor = analysis.getOutputDescriptor(node);
        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : descriptor.getAllFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field.getName().get(), field.getType());

            outputSymbolsBuilder.add(symbol);
            columns.put(symbol, analysis.getColumn(field));
        }

        List<Symbol> planOutputSymbols = outputSymbolsBuilder.build();
        Optional<ColumnHandle> sampleWeightColumn = metadata.getSampleWeightColumnHandle(handle);
        Symbol sampleWeightSymbol = null;
        if (sampleWeightColumn.isPresent()) {
            sampleWeightSymbol = symbolAllocator.newSymbol("$sampleWeight", BIGINT);
            outputSymbolsBuilder.add(sampleWeightSymbol);
            columns.put(sampleWeightSymbol, sampleWeightColumn.get());
        }

        List<Symbol> nodeOutputSymbols = outputSymbolsBuilder.build();
        PlanNode root = new TableScanNode(idAllocator.getNextId(), handle, nodeOutputSymbols, columns.build(), null, Optional.<GeneratedPartitions>absent());
        return new RelationPlan(root, descriptor, planOutputSymbols, Optional.fromNullable(sampleWeightSymbol));
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        TupleDescriptor outputDescriptor = analysis.getOutputDescriptor(node);

        return new RelationPlan(subPlan.getRoot(), outputDescriptor, subPlan.getOutputSymbols(), subPlan.getSampleWeight());
    }

    @Override
    protected RelationPlan visitSampledRelation(SampledRelation node, Void context)
    {
        if (node.getColumnsToStratifyOn().isPresent()) {
            throw new UnsupportedOperationException("STRATIFY ON is not yet implemented");
        }

        RelationPlan subPlan = process(node.getRelation(), context);

        TupleDescriptor outputDescriptor = analysis.getOutputDescriptor(node);
        double ratio = analysis.getSampleRatio(node);
        Symbol sampleWeightSymbol = null;
        if (node.getType() == SampledRelation.Type.POISSONIZED) {
            sampleWeightSymbol = symbolAllocator.newSymbol("$sampleWeight", BigintType.BIGINT);
        }
        PlanNode planNode = new SampleNode(idAllocator.getNextId(), subPlan.getRoot(), ratio, SampleNode.Type.fromType(node.getType()), node.isRescaled(), Optional.fromNullable(sampleWeightSymbol));
        return new RelationPlan(planNode, outputDescriptor, subPlan.getOutputSymbols(), Optional.fromNullable(sampleWeightSymbol));
    }

    @Override
    protected RelationPlan visitJoin(Join node, Void context)
    {
        // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
        RelationPlan leftPlan = process(node.getLeft(), context);
        RelationPlan rightPlan = process(node.getRight(), context);

        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        TupleDescriptor outputDescriptor = analysis.getOutputDescriptor(node);

        // NOTE: symbols must be in the same order as the outputDescriptor
        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getOutputSymbols())
                .addAll(rightPlan.getOutputSymbols())
                .build();

        ImmutableList.Builder<JoinNode.EquiJoinClause> clauses = ImmutableList.builder();
        if (node.getType() != Join.Type.CROSS) {
            List<EquiJoinClause> criteria = analysis.getJoinCriteria(node);
            Analysis.JoinInPredicates joinInPredicates = analysis.getJoinInPredicates(node);

            // Add semi joins if necessary
            if (joinInPredicates != null) {
                leftPlanBuilder = appendSemiJoins(leftPlanBuilder, joinInPredicates.getLeftInPredicates());
                rightPlanBuilder = appendSemiJoins(rightPlanBuilder, joinInPredicates.getRightInPredicates());
            }

            // Add projections for join criteria
            leftPlanBuilder = appendProjections(leftPlanBuilder, Iterables.transform(criteria, leftGetter()));
            rightPlanBuilder = appendProjections(rightPlanBuilder, Iterables.transform(criteria, rightGetter()));

            for (EquiJoinClause clause : criteria) {
                Symbol leftSymbol = leftPlanBuilder.translate(clause.getLeft());
                Symbol rightSymbol = rightPlanBuilder.translate(clause.getRight());

                clauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
            }
        }

        PlanNode root = new JoinNode(idAllocator.getNextId(),
                JoinNode.Type.typeConvert(node.getType()),
                leftPlanBuilder.getRoot(),
                rightPlanBuilder.getRoot(),
                clauses.build());
        Optional<Symbol> sampleWeight = Optional.absent();
        if (leftPlanBuilder.getSampleWeight().isPresent() || rightPlanBuilder.getSampleWeight().isPresent()) {
            Expression expression = new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, oneIfNull(leftPlanBuilder.getSampleWeight()), oneIfNull(rightPlanBuilder.getSampleWeight()));
            sampleWeight = Optional.of(symbolAllocator.newSymbol(expression, BIGINT));
            root = new ProjectNode(idAllocator.getNextId(), root, ImmutableMap.of(sampleWeight.get(), expression));
        }

        return new RelationPlan(root, outputDescriptor, outputSymbols, sampleWeight);
    }

    private Expression oneIfNull(Optional<Symbol> symbol)
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
        PlanBuilder subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session).process(node, null);

        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        for (FieldOrExpression fieldOrExpression : analysis.getOutputExpressions(node)) {
            outputSymbols.add(subPlan.translate(fieldOrExpression));
        }

        return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node), outputSymbols.build(), subPlan.getSampleWeight());
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        PlanBuilder subPlan = new QueryPlanner(analysis, symbolAllocator, idAllocator, metadata, session).process(node, null);

        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        for (FieldOrExpression fieldOrExpression : analysis.getOutputExpressions(node)) {
            outputSymbols.add(subPlan.translate(fieldOrExpression));
        }

        return new RelationPlan(subPlan.getRoot(), analysis.getOutputDescriptor(node), outputSymbols.build(), subPlan.getSampleWeight());
    }

    @Override
    protected RelationPlan visitValues(Values node, Void context)
    {
        TupleDescriptor descriptor = analysis.getOutputDescriptor(node);
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        for (Field field : descriptor.getVisibleFields()) {
            Symbol symbol = symbolAllocator.newSymbol(field);
            outputSymbolsBuilder.add(symbol);
        }

        ImmutableList.Builder<List<Expression>> rows = ImmutableList.builder();
        for (Row row : node.getRows()) {
            ImmutableList.Builder<Expression> values = ImmutableList.builder();
            for (Expression expression : row.getItems()) {
                values.add(evaluateConstantExpression(expression));
            }
            rows.add(values.build());
        }

        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), outputSymbolsBuilder.build(), rows.build());
        return new RelationPlan(valuesNode, descriptor, outputSymbolsBuilder.build(), Optional.<Symbol>absent());
    }

    private Expression evaluateConstantExpression(final Expression expression)
    {
        try {
            // expressionInterpreter/optimizer only understands a subset of expression types
            // TODO: remove this when the new expression tree is implemented
            Expression canonicalized = CanonicalizeExpressions.canonicalizeExpression(expression);

            // verify the expression is constant (has no inputs)
            ExpressionInterpreter.expressionOptimizer(canonicalized, metadata, session, analysis.getTypes()).optimize(new SymbolResolver() {
                @Override
                public Object getValue(Symbol symbol)
                {
                    throw new SemanticException(EXPRESSION_NOT_CONSTANT, expression, "Constant expression cannot contain column references");
                }
            });

            // evaluate the expression
            Object result = ExpressionInterpreter.expressionInterpreter(canonicalized, metadata, session, analysis.getTypes()).evaluate(0);
            checkState(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");

            return LiteralInterpreter.toExpression(result, analysis.getType(expression));
        }
        catch (Exception e) {
            throw new SemanticException(EXPRESSION_NOT_CONSTANT, expression, "Error evaluating constant expression: %s", e.getMessage());
        }
    }

    @Override
    protected RelationPlan visitUnion(Union node, final Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

        List<Symbol> unionOutputSymbols = null;
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.builder();
        List<RelationPlan> subPlans = IterableTransformer.on(node.getRelations()).transform(new Function<Relation, RelationPlan>()
        {
            @Override
            public RelationPlan apply(Relation relation)
            {
                return process(relation, context);
            }
        }).list();

        boolean hasSampleWeight = false;
        for (RelationPlan subPlan : subPlans) {
            if (subPlan.getSampleWeight().isPresent()) {
                hasSampleWeight = true;
                break;
            }
        }

        Optional<Symbol> outputSampleWeight = Optional.absent();
        for (RelationPlan relationPlan : subPlans) {
            if (hasSampleWeight && !relationPlan.getSampleWeight().isPresent()) {
                relationPlan = addConstantSampleWeight(relationPlan);
            }

            List<Symbol> childOutputSymobls = relationPlan.getOutputSymbols();
            if (unionOutputSymbols == null) {
                // Use the first Relation to derive output symbol names
                TupleDescriptor descriptor = relationPlan.getDescriptor();
                ImmutableList.Builder<Symbol> outputSymbolBuilder = ImmutableList.builder();
                for (Field field : descriptor.getVisibleFields()) {
                    int fieldIndex = descriptor.indexOf(field);
                    Symbol symbol = childOutputSymobls.get(fieldIndex);
                    outputSymbolBuilder.add(symbolAllocator.newSymbol(symbol.getName(), symbolAllocator.getTypes().get(symbol)));
                }
                unionOutputSymbols = outputSymbolBuilder.build();
                outputSampleWeight = relationPlan.getSampleWeight();
            }

            TupleDescriptor descriptor = relationPlan.getDescriptor();
            checkArgument(descriptor.getVisibleFieldCount() == unionOutputSymbols.size(),
                    "Expected relation to have %s symbols but has %s symbols",
                    descriptor.getVisibleFieldCount(),
                    unionOutputSymbols.size());

            int unionFieldId = 0;
            for (Field field : descriptor.getVisibleFields()) {
                int fieldIndex = descriptor.indexOf(field);
                symbolMapping.put(unionOutputSymbols.get(unionFieldId), childOutputSymobls.get(fieldIndex));
                unionFieldId++;
            }

            sources.add(relationPlan.getRoot());
        }

        PlanNode planNode = new UnionNode(idAllocator.getNextId(), sources.build(), symbolMapping.build());
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
            Symbol symbol = symbolAllocator.newSymbol(expression, analysis.getType(expression));

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

    private PlanBuilder appendSemiJoins(PlanBuilder subPlan, Set<InPredicate> inPredicates)
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

        Preconditions.checkState(inPredicate.getValueList() instanceof SubqueryExpression);
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
                        semiJoinOutputSymbol),
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
                Optional.<Symbol>absent(),
                1.0);
    }
}
