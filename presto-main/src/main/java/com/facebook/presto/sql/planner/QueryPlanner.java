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
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SortItem.NullOrdering;
import com.facebook.presto.sql.tree.SortItem.Ordering;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;

class QueryPlanner
        extends DefaultTraversalVisitor<PlanBuilder, Void>
{
    private final Analysis analysis;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;

    QueryPlanner(Analysis analysis, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
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
    protected PlanBuilder visitQuery(Query query, Void context)
    {
        PlanBuilder builder = planQueryBody(query);
        Set<InPredicate> inPredicates = analysis.getInPredicates(query);
        builder = appendSemiJoins(builder, inPredicates);

        List<FieldOrExpression> orderBy = analysis.getOrderByExpressions(query);
        List<FieldOrExpression> outputs = analysis.getOutputExpressions(query);
        builder = project(builder, Iterables.concat(orderBy, outputs));

        builder = sort(builder, query);
        builder = project(builder, analysis.getOutputExpressions(query));
        builder = limit(builder, query);

        return builder;
    }

    @Override
    protected PlanBuilder visitQuerySpecification(QuerySpecification node, Void context)
    {
        PlanBuilder builder = planFrom(node);

        Set<InPredicate> inPredicates = analysis.getInPredicates(node);
        builder = appendSemiJoins(builder, inPredicates);

        builder = filter(builder, analysis.getWhere(node));
        builder = aggregate(builder, node);
        builder = filter(builder, analysis.getHaving(node));

        builder = window(builder, node);

        List<FieldOrExpression> orderBy = analysis.getOrderByExpressions(node);
        List<FieldOrExpression> outputs = analysis.getOutputExpressions(node);
        builder = project(builder, Iterables.concat(orderBy, outputs));

        builder = distinct(builder, node, outputs, orderBy);
        builder = sort(builder, node);
        builder = project(builder, analysis.getOutputExpressions(node));
        builder = limit(builder, node);

        return builder;
    }

    private PlanBuilder planQueryBody(Query query)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session)
                .process(query.getQueryBody(), null);

        TranslationMap translations = new TranslationMap(relationPlan, analysis);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the QuerySpecification directly
        translations.setFieldMappings(relationPlan.getOutputSymbols());

        return new PlanBuilder(translations, relationPlan.getRoot(), relationPlan.getSampleWeight());
    }

    private PlanBuilder planFrom(QuerySpecification node)
    {
        RelationPlan relationPlan;

        if (node.getFrom().isPresent()) {
            relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session)
                    .process(node.getFrom().get(), null);
        }
        else {
            relationPlan = planImplicitTable();
        }

        TranslationMap translations = new TranslationMap(relationPlan, analysis);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getOutputSymbols());

        return new PlanBuilder(translations, relationPlan.getRoot(), relationPlan.getSampleWeight());
    }

    private RelationPlan planImplicitTable()
    {
        List<Expression> emptyRow = ImmutableList.of();
        return new RelationPlan(
                new ValuesNode(idAllocator.getNextId(), ImmutableList.<Symbol>of(), ImmutableList.of(emptyRow)),
                new TupleDescriptor(),
                ImmutableList.<Symbol>of(),
                Optional.empty());
    }

    private PlanBuilder filter(PlanBuilder subPlan, Expression predicate)
    {
        if (predicate == null) {
            return subPlan;
        }

        Expression rewritten = subPlan.rewrite(predicate);
        return new PlanBuilder(subPlan.getTranslations(), new FilterNode(idAllocator.getNextId(), subPlan.getRoot(), rewritten), subPlan.getSampleWeight());
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<FieldOrExpression> expressions)
    {
        TranslationMap outputTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);

        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
        for (FieldOrExpression fieldOrExpression : ImmutableSet.copyOf(expressions)) {
            Symbol symbol;

            if (fieldOrExpression.isFieldReference()) {
                Field field = subPlan.getRelationPlan().getDescriptor().getFieldByIndex(fieldOrExpression.getFieldIndex());
                symbol = symbolAllocator.newSymbol(field);
            }
            else {
                Expression expression = fieldOrExpression.getExpression();
                symbol = symbolAllocator.newSymbol(expression, analysis.getType(expression));
            }

            projections.put(symbol, subPlan.rewrite(fieldOrExpression));
            outputTranslations.put(fieldOrExpression, symbol);
        }

        if (subPlan.getSampleWeight().isPresent()) {
            Symbol symbol = subPlan.getSampleWeight().get();
            projections.put(symbol, new QualifiedNameReference(symbol.toQualifiedName()));
        }

        return new PlanBuilder(outputTranslations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()), subPlan.getSampleWeight());
    }

    private Map<Symbol, Expression> coerce(Iterable<? extends Expression> expressions, PlanBuilder subPlan, TranslationMap translations)
    {
        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();

        for (Expression expression : expressions) {
            Type coercion = analysis.getCoercion(expression);
            Symbol symbol = symbolAllocator.newSymbol(expression, firstNonNull(coercion, analysis.getType(expression)));
            Expression rewritten = subPlan.rewrite(expression);
            if (coercion != null) {
                rewritten = new Cast(rewritten, coercion.getTypeSignature().toString());
            }
            projections.put(symbol, rewritten);
            translations.put(expression, symbol);
        }

        return projections.build();
    }

    private PlanBuilder explicitCoercionFields(PlanBuilder subPlan, Iterable<FieldOrExpression> alreadyCoerced, Iterable<? extends Expression> uncoerced)
    {
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);
        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();

        projections.putAll(coerce(uncoerced, subPlan, translations));

        for (FieldOrExpression fieldOrExpression : alreadyCoerced) {
            Symbol symbol;
            if (fieldOrExpression.isFieldReference()) {
                Field field = subPlan.getRelationPlan().getDescriptor().getFieldByIndex(fieldOrExpression.getFieldIndex());
                symbol = symbolAllocator.newSymbol(field);
            }
            else {
                symbol = symbolAllocator.newSymbol(fieldOrExpression.getExpression(), analysis.getType(fieldOrExpression.getExpression()));
            }
            Expression rewritten = subPlan.rewrite(fieldOrExpression);
            projections.put(symbol, rewritten);
            translations.put(fieldOrExpression, symbol);
        }

        return new PlanBuilder(translations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()), subPlan.getSampleWeight());
    }

    private PlanBuilder explicitCoercionSymbols(PlanBuilder subPlan, Iterable<Symbol> alreadyCoerced, Iterable<? extends Expression> uncoerced)
    {
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);
        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();

        projections.putAll(coerce(uncoerced, subPlan, translations));

        for (Symbol symbol : alreadyCoerced) {
            projections.put(symbol, new QualifiedNameReference(symbol.toQualifiedName()));
        }

        return new PlanBuilder(translations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()), subPlan.getSampleWeight());
    }

    private PlanBuilder aggregate(PlanBuilder subPlan, QuerySpecification node)
    {
        if (analysis.getAggregates(node).isEmpty() && analysis.getGroupByExpressions(node).isEmpty()) {
            return subPlan;
        }

        Set<FieldOrExpression> arguments = analysis.getAggregates(node).stream()
                .map(FunctionCall::getArguments)
                .flatMap(List::stream)
                .map(FieldOrExpression::new)
                .collect(toImmutableSet());

        // 1. Pre-project all scalar inputs (arguments and non-trivial group by expressions)
        Iterable<FieldOrExpression> inputs = Iterables.concat(analysis.getGroupByExpressions(node), arguments);
        if (!Iterables.isEmpty(inputs)) { // avoid an empty projection if the only aggregation is COUNT (which has no arguments)
            subPlan = project(subPlan, inputs);
        }

        // 2. Aggregate
        ImmutableMap.Builder<Symbol, FunctionCall> aggregationAssignments = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Signature> functions = ImmutableMap.builder();

        // 2.a. Rewrite aggregates in terms of pre-projected inputs
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);
        boolean needPostProjectionCoercion = false;
        for (FunctionCall aggregate : analysis.getAggregates(node)) {
            Expression rewritten = subPlan.rewrite(aggregate);
            Symbol newSymbol = symbolAllocator.newSymbol(rewritten, analysis.getType(aggregate));

            // TODO: this is a hack, because we apply coercions to the output of expressions, rather than the arguments to expressions.
            // Therefore we can end up with this implicit cast, and have to move it into a post-projection
            if (rewritten instanceof Cast) {
                rewritten = ((Cast) rewritten).getExpression();
                needPostProjectionCoercion = true;
            }
            aggregationAssignments.put(newSymbol, (FunctionCall) rewritten);
            translations.put(aggregate, newSymbol);

            functions.put(newSymbol, analysis.getFunctionInfo(aggregate).getSignature());
        }

        // 2.b. Rewrite group by expressions in terms of pre-projected inputs
        Set<Symbol> groupBySymbols = new LinkedHashSet<>();
        for (FieldOrExpression fieldOrExpression : analysis.getGroupByExpressions(node)) {
            Symbol symbol = subPlan.translate(fieldOrExpression);
            groupBySymbols.add(symbol);
            translations.put(fieldOrExpression, symbol);
        }

        // 2.c. Mark distinct rows for each aggregate that has DISTINCT
        // Map from aggregate function arguments to marker symbols, so that we can reuse the markers, if two aggregates have the same argument
        Map<Set<Expression>, Symbol> argumentMarkers = new HashMap<>();
        // Map from aggregate functions to marker symbols
        Map<Symbol, Symbol> masks = new HashMap<>();
        for (FunctionCall aggregate : Iterables.filter(analysis.getAggregates(node), FunctionCall::isDistinct)) {
            Set<Expression> args = ImmutableSet.copyOf(aggregate.getArguments());
            Symbol marker = argumentMarkers.get(args);
            Symbol aggregateSymbol = translations.get(aggregate);
            if (marker == null) {
                if (args.size() == 1) {
                    marker = symbolAllocator.newSymbol(Iterables.getOnlyElement(args), BOOLEAN, "distinct");
                }
                else {
                    marker = symbolAllocator.newSymbol(aggregateSymbol.getName(), BOOLEAN, "distinct");
                }
                argumentMarkers.put(args, marker);
            }

            masks.put(aggregateSymbol, marker);
        }

        for (Map.Entry<Set<Expression>, Symbol> entry : argumentMarkers.entrySet()) {
            ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
            builder.addAll(groupBySymbols);
            for (Expression expression : entry.getKey()) {
                builder.add(subPlan.translate(expression));
            }
            MarkDistinctNode markDistinct = new MarkDistinctNode(idAllocator.getNextId(),
                    subPlan.getRoot(),
                    entry.getValue(),
                    builder.build(),
                    Optional.empty());
            subPlan = new PlanBuilder(subPlan.getTranslations(), markDistinct, subPlan.getSampleWeight());
        }

        double confidence = 1.0;
        if (analysis.getQuery().getApproximate().isPresent()) {
            confidence = Double.valueOf(analysis.getQuery().getApproximate().get().getConfidence()) / 100.0;
        }

        AggregationNode aggregationNode = new AggregationNode(idAllocator.getNextId(), subPlan.getRoot(), ImmutableList.copyOf(groupBySymbols), aggregationAssignments.build(), functions.build(), new ImmutableMap.Builder<Symbol, Symbol>().putAll(masks).build(), subPlan.getSampleWeight(), confidence, Optional.empty());
        subPlan = new PlanBuilder(translations, aggregationNode, Optional.empty());

        // 3. Post-projection
        // Add back the implicit casts that we removed in 2.a
        // TODO: this is a hack, we should change type coercions to coerce the inputs to functions/operators instead of coercing the output
        if (needPostProjectionCoercion) {
            return explicitCoercionFields(subPlan, analysis.getGroupByExpressions(node), analysis.getAggregates(node));
        }
        return subPlan;
    }

    private PlanBuilder window(PlanBuilder subPlan, QuerySpecification node)
    {
        Set<FunctionCall> windowFunctions = ImmutableSet.copyOf(analysis.getWindowFunctions(node));
        if (windowFunctions.isEmpty()) {
            return subPlan;
        }

        for (FunctionCall windowFunction : windowFunctions) {
            Window window = windowFunction.getWindow().get();

            // Extract frame
            WindowFrame.Type frameType = WindowFrame.Type.RANGE;
            FrameBound.Type frameStartType = FrameBound.Type.UNBOUNDED_PRECEDING;
            FrameBound.Type frameEndType = FrameBound.Type.CURRENT_ROW;
            Expression frameStart = null;
            Expression frameEnd = null;

            if (window.getFrame().isPresent()) {
                WindowFrame frame = window.getFrame().get();
                frameType = frame.getType();

                frameStartType = frame.getStart().getType();
                frameStart = frame.getStart().getValue().orElse(null);

                if (frame.getEnd().isPresent()) {
                    frameEndType = frame.getEnd().get().getType();
                    frameEnd = frame.getEnd().get().getValue().orElse(null);
                }
            }

            // Pre-project inputs
            ImmutableList.Builder<Expression> inputs = ImmutableList.<Expression>builder()
                    .addAll(windowFunction.getArguments())
                    .addAll(window.getPartitionBy())
                    .addAll(Iterables.transform(window.getOrderBy(), SortItem::getSortKey));

            if (frameStart != null) {
                inputs.add(frameStart);
            }
            if (frameEnd != null) {
                inputs.add(frameEnd);
            }

            subPlan = appendProjections(subPlan, inputs.build());

            // Rewrite PARTITION BY in terms of pre-projected inputs
            ImmutableList.Builder<Symbol> partitionBySymbols = ImmutableList.builder();
            for (Expression expression : window.getPartitionBy()) {
                partitionBySymbols.add(subPlan.translate(expression));
            }

            // Rewrite ORDER BY in terms of pre-projected inputs
            ImmutableList.Builder<Symbol> orderBySymbols = ImmutableList.builder();
            Map<Symbol, SortOrder> orderings = new HashMap<>();
            for (SortItem item : window.getOrderBy()) {
                Symbol symbol = subPlan.translate(item.getSortKey());
                orderBySymbols.add(symbol);
                orderings.put(symbol, toSortOrder(item));
            }

            // Rewrite frame bounds in terms of pre-projected inputs
            Optional<Symbol> frameStartSymbol = Optional.empty();
            Optional<Symbol> frameEndSymbol = Optional.empty();
            if (frameStart != null) {
                frameStartSymbol = Optional.of(subPlan.translate(frameStart));
            }
            if (frameEnd != null) {
                frameEndSymbol = Optional.of(subPlan.translate(frameEnd));
            }

            WindowNode.Frame frame = new WindowNode.Frame(frameType,
                    frameStartType, frameStartSymbol,
                    frameEndType, frameEndSymbol);

            TranslationMap outputTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);
            outputTranslations.copyMappingsFrom(subPlan.getTranslations());

            ImmutableMap.Builder<Symbol, FunctionCall> assignments = ImmutableMap.builder();
            Map<Symbol, Signature> signatures = new HashMap<>();

            // Rewrite function call in terms of pre-projected inputs
            Expression rewritten = subPlan.rewrite(windowFunction);
            Symbol newSymbol = symbolAllocator.newSymbol(rewritten, analysis.getType(windowFunction));

            boolean needCoercion = rewritten instanceof Cast;
            // Strip out the cast and add it back as a post-projection
            if (rewritten instanceof Cast) {
                rewritten = ((Cast) rewritten).getExpression();
            }
            assignments.put(newSymbol, (FunctionCall) rewritten);
            outputTranslations.put(windowFunction, newSymbol);

            signatures.put(newSymbol, analysis.getFunctionInfo(windowFunction).getSignature());

            List<Symbol> sourceSymbols = subPlan.getRoot().getOutputSymbols();

            // create window node
            subPlan = new PlanBuilder(outputTranslations,
                    new WindowNode(
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            partitionBySymbols.build(),
                            orderBySymbols.build(),
                            orderings,
                            frame,
                            assignments.build(),
                            signatures,
                            Optional.empty()),
                    subPlan.getSampleWeight());

            if (needCoercion) {
                subPlan = explicitCoercionSymbols(subPlan, sourceSymbols, ImmutableList.of(windowFunction));
            }
        }

        return subPlan;
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

    /**
     * Semijoins are planned as follows:
     * 1) SQL constructs that need to be semijoined are extracted during Analysis phase (currently only InPredicates so far)
     * 2) Create a new SemiJoinNode that connects the semijoin lookup field with the planned subquery and have it output a new boolean
     * symbol for the result of the semijoin.
     * 3) Add an entry to the TranslationMap that notes to map the InPredicate into semijoin output symbol
     * <p/>
     * Currently, we only support semijoins deriving from InPredicates, but we will probably need
     * to add support for more SQL constructs in the future.
     */
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
                        semiJoinOutputSymbol,
                        Optional.empty(),
                        Optional.empty()),
                subPlan.getSampleWeight());
    }

    private PlanBuilder distinct(PlanBuilder subPlan, QuerySpecification node, List<FieldOrExpression> outputs, List<FieldOrExpression> orderBy)
    {
        if (node.getSelect().isDistinct()) {
            checkState(outputs.containsAll(orderBy), "Expected ORDER BY terms to be in SELECT. Broken analysis");

            AggregationNode aggregation = new AggregationNode(idAllocator.getNextId(),
                    subPlan.getRoot(),
                    subPlan.getRoot().getOutputSymbols(),
                    ImmutableMap.<Symbol, FunctionCall>of(),
                    ImmutableMap.<Symbol, Signature>of(),
                    ImmutableMap.<Symbol, Symbol>of(),
                    Optional.empty(),
                    1.0,
                    Optional.empty());

            return new PlanBuilder(subPlan.getTranslations(), aggregation, subPlan.getSampleWeight());
        }

        return subPlan;
    }

    private PlanBuilder sort(PlanBuilder subPlan, Query node)
    {
        return sort(subPlan, node.getOrderBy(), node.getLimit(), analysis.getOrderByExpressions(node));
    }

    private PlanBuilder sort(PlanBuilder subPlan, QuerySpecification node)
    {
        return sort(subPlan, node.getOrderBy(), node.getLimit(), analysis.getOrderByExpressions(node));
    }

    private PlanBuilder sort(PlanBuilder subPlan, List<SortItem> orderBy, Optional<String> limit, List<FieldOrExpression> orderByExpressions)
    {
        if (orderBy.isEmpty()) {
            return subPlan;
        }

        Iterator<SortItem> sortItems = orderBy.iterator();

        ImmutableList.Builder<Symbol> orderBySymbols = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, SortOrder> orderings = ImmutableMap.builder();
        for (FieldOrExpression fieldOrExpression : orderByExpressions) {
            Symbol symbol = subPlan.translate(fieldOrExpression);
            orderBySymbols.add(symbol);

            orderings.put(symbol, toSortOrder(sortItems.next()));
        }

        PlanNode planNode;
        if (limit.isPresent()) {
            planNode = new TopNNode(idAllocator.getNextId(), subPlan.getRoot(), Long.valueOf(limit.get()), orderBySymbols.build(), orderings.build(), false);
        }
        else {
            planNode = new SortNode(idAllocator.getNextId(), subPlan.getRoot(), orderBySymbols.build(), orderings.build());
        }

        return new PlanBuilder(subPlan.getTranslations(), planNode, subPlan.getSampleWeight());
    }

    private PlanBuilder limit(PlanBuilder subPlan, Query node)
    {
        return limit(subPlan, node.getOrderBy(), node.getLimit());
    }

    private PlanBuilder limit(PlanBuilder subPlan, QuerySpecification node)
    {
        return limit(subPlan, node.getOrderBy(), node.getLimit());
    }

    private PlanBuilder limit(PlanBuilder subPlan, List<SortItem> orderBy, Optional<String> limit)
    {
        if (orderBy.isEmpty() && limit.isPresent()) {
            long limitValue = Long.valueOf(limit.get());
            return new PlanBuilder(subPlan.getTranslations(), new LimitNode(idAllocator.getNextId(), subPlan.getRoot(), limitValue), subPlan.getSampleWeight());
        }

        return subPlan;
    }

    private SortOrder toSortOrder(SortItem sortItem)
    {
        if (sortItem.getOrdering() == Ordering.ASCENDING) {
            if (sortItem.getNullOrdering() == NullOrdering.FIRST) {
                return SortOrder.ASC_NULLS_FIRST;
            }
            else {
                return SortOrder.ASC_NULLS_LAST;
            }
        }
        else {
            if (sortItem.getNullOrdering() == NullOrdering.FIRST) {
                return SortOrder.DESC_NULLS_FIRST;
            }
            else {
                return SortOrder.DESC_NULLS_LAST;
            }
        }
    }
}
