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

import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;
import static com.facebook.presto.sql.tree.FunctionCall.argumentsGetter;
import static com.facebook.presto.sql.tree.FunctionCall.distinctPredicate;
import static com.facebook.presto.sql.tree.SortItem.sortKeyGetter;
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

        return new PlanBuilder(translations, relationPlan.getRoot());
    }

    private PlanBuilder planFrom(QuerySpecification node)
    {
        RelationPlan relationPlan;

        if (node.getFrom() == null || node.getFrom().isEmpty()) {
            relationPlan = planImplicitTable();
        }
        else {
            relationPlan = new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session)
                    .process(Iterables.getOnlyElement(node.getFrom()), null);
        }

        TranslationMap translations = new TranslationMap(relationPlan, analysis);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getOutputSymbols());

        return new PlanBuilder(translations, relationPlan.getRoot());
    }

    private RelationPlan planImplicitTable()
    {
        // TODO: replace this with a table-generating operator that produces 1 row with no columns

        QualifiedTableName name = MetadataUtil.createQualifiedTableName(session, QualifiedName.of("dual"));
        Optional<TableHandle> optionalHandle = metadata.getTableHandle(name);
        checkState(optionalHandle.isPresent(), "Dual table provider not installed");
        TableHandle table = optionalHandle.get();
        TableMetadata tableMetadata = metadata.getTableMetadata(table);
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(table);

        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            Symbol symbol = symbolAllocator.newSymbol(column.getName(), Type.fromRaw(column.getType()));
            columns.put(symbol, columnHandles.get(column.getName()));
        }

        ImmutableMap<Symbol, ColumnHandle> assignments = columns.build();
        TableScanNode tableScan = new TableScanNode(idAllocator.getNextId(), table, ImmutableList.copyOf(assignments.keySet()), assignments, null, Optional.<GeneratedPartitions>absent());

        return new RelationPlan(tableScan, new TupleDescriptor(), ImmutableList.<Symbol>of());
    }

    private PlanBuilder filter(PlanBuilder subPlan, Expression predicate)
    {
        if (predicate == null) {
            return subPlan;
        }

        Expression rewritten = subPlan.rewrite(predicate);
        return new PlanBuilder(subPlan.getTranslations(), new FilterNode(idAllocator.getNextId(), subPlan.getRoot(), rewritten));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<FieldOrExpression> expressions)
    {
        TranslationMap outputTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);

        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
        for (FieldOrExpression fieldOrExpression : ImmutableSet.copyOf(expressions)) {
            Symbol symbol;

            if (fieldOrExpression.isFieldReference()) {
                Field field = subPlan.getRelationPlan().getDescriptor().getFields().get(fieldOrExpression.getFieldIndex());
                symbol = symbolAllocator.newSymbol(field);
            }
            else {
                Expression expression = fieldOrExpression.getExpression();
                symbol = symbolAllocator.newSymbol(expression, analysis.getType(expression));
            }

            projections.put(symbol, subPlan.rewrite(fieldOrExpression));
            outputTranslations.put(fieldOrExpression, symbol);
        }

        return new PlanBuilder(outputTranslations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()));
    }

    private PlanBuilder aggregate(PlanBuilder subPlan, QuerySpecification node)
    {
        if (analysis.getAggregates(node).isEmpty() && analysis.getGroupByExpressions(node).isEmpty()) {
            return subPlan;
        }

        Set<FieldOrExpression> arguments = IterableTransformer.on(analysis.getAggregates(node))
                .transformAndFlatten(argumentsGetter())
                .transform(toFieldOrExpression())
                .set();

        // 1. Pre-project all scalar inputs (arguments and non-trivial group by expressions)
        Iterable<FieldOrExpression> inputs = Iterables.concat(analysis.getGroupByExpressions(node), arguments);
        if (!Iterables.isEmpty(inputs)) { // avoid an empty projection if the only aggregation is COUNT (which has no arguments)
            subPlan = project(subPlan, inputs);
        }

        // 1.a Rewrite DISTINCT aggregates as a group by
        // All DISTINCT argument lists must match see TupleAnalyzer::analyzeAggregations
        if (Iterables.any(analysis.getAggregates(node), distinctPredicate())) {
            AggregationNode aggregation = new AggregationNode(idAllocator.getNextId(),
                    subPlan.getRoot(),
                    subPlan.getRoot().getOutputSymbols(),
                    ImmutableMap.<Symbol, FunctionCall>of(),
                    ImmutableMap.<Symbol, FunctionHandle>of());

            subPlan =  new PlanBuilder(subPlan.getTranslations(), aggregation);
        }

        // 2. Aggregate
        ImmutableMap.Builder<Symbol, FunctionCall> aggregationAssignments = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, FunctionHandle> functions = ImmutableMap.builder();

        // 2.a. Rewrite aggregates in terms of pre-projected inputs
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis);
        for (FunctionCall aggregate : analysis.getAggregates(node)) {
            FunctionCall rewritten = (FunctionCall) subPlan.rewrite(aggregate);
            Symbol newSymbol = symbolAllocator.newSymbol(rewritten, analysis.getType(aggregate));

            aggregationAssignments.put(newSymbol, rewritten);
            translations.put(aggregate, newSymbol);

            functions.put(newSymbol, analysis.getFunctionInfo(aggregate).getHandle());
        }

        // 2.b. Rewrite group by expressions in terms of pre-projected inputs
        Set<Symbol> groupBySymbols = new LinkedHashSet<>();
        for (FieldOrExpression fieldOrExpression : analysis.getGroupByExpressions(node)) {
            Symbol symbol = subPlan.translate(fieldOrExpression);
            groupBySymbols.add(symbol);
            translations.put(fieldOrExpression, symbol);
        }

        return new PlanBuilder(translations, new AggregationNode(idAllocator.getNextId(), subPlan.getRoot(), ImmutableList.copyOf(groupBySymbols), aggregationAssignments.build(), functions.build()));
    }

    private PlanBuilder window(PlanBuilder subPlan, QuerySpecification node)
    {
        Set<FunctionCall> windowFunctions = ImmutableSet.copyOf(analysis.getWindowFunctions(node));
        if (windowFunctions.isEmpty()) {
            return subPlan;
        }

        for (FunctionCall windowFunction : windowFunctions) {
            // Pre-project inputs
            ImmutableList<Expression> inputs = ImmutableList.<Expression>builder()
                    .addAll(windowFunction.getArguments())
                    .addAll(windowFunction.getWindow().get().getPartitionBy())
                    .addAll(Iterables.transform(windowFunction.getWindow().get().getOrderBy(), sortKeyGetter()))
                    .build();

            subPlan = appendProjections(subPlan, inputs);

            // Rewrite PARTITION BY in terms of pre-projected inputs
            ImmutableList.Builder<Symbol> partitionBySymbols = ImmutableList.builder();
            for (Expression expression : windowFunction.getWindow().get().getPartitionBy()) {
                partitionBySymbols.add(subPlan.translate(expression));
            }

            // Rewrite ORDER BY in terms of pre-projected inputs
            ImmutableList.Builder<Symbol> orderBySymbols = ImmutableList.builder();
            Map<Symbol, SortItem.Ordering> orderings = new HashMap<>();
            for (SortItem item : windowFunction.getWindow().get().getOrderBy()) {
                Symbol symbol = subPlan.translate(item.getSortKey());
                orderBySymbols.add(symbol);
                orderings.put(symbol, item.getOrdering());
            }

            TranslationMap outputTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis);
            outputTranslations.copyMappingsFrom(subPlan.getTranslations());

            ImmutableMap.Builder<Symbol, FunctionCall> assignments = ImmutableMap.builder();
            Map<Symbol, FunctionHandle> functionHandles = new HashMap<>();

            // Rewrite function call in terms of pre-projected inputs
            FunctionCall rewritten = (FunctionCall) subPlan.rewrite(windowFunction);
            Symbol newSymbol = symbolAllocator.newSymbol(rewritten, analysis.getType(windowFunction));

            assignments.put(newSymbol, rewritten);
            outputTranslations.put(windowFunction, newSymbol);

            functionHandles.put(newSymbol, analysis.getFunctionInfo(windowFunction).getHandle());

            // create window node
            subPlan = new PlanBuilder(outputTranslations,
                    new WindowNode(idAllocator.getNextId(), subPlan.getRoot(), partitionBySymbols.build(), orderBySymbols.build(), orderings, assignments.build(), functionHandles));
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

        return new PlanBuilder(translations, new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), projections.build()));
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

        Symbol semiJoinOutputSymbol = symbolAllocator.newSymbol("semijoinresult", Type.BOOLEAN);

        translations.put(inPredicate, semiJoinOutputSymbol);

        return new PlanBuilder(translations,
                new SemiJoinNode(idAllocator.getNextId(),
                        subPlan.getRoot(),
                        valueListRelation.getRoot(),
                        sourceJoinSymbol,
                        filteringSourceJoinSymbol,
                        semiJoinOutputSymbol));
    }

    private PlanBuilder distinct(PlanBuilder subPlan, QuerySpecification node, List<FieldOrExpression> outputs, List<FieldOrExpression> orderBy)
    {
        if (node.getSelect().isDistinct()) {
            checkState(outputs.containsAll(orderBy), "Expected ORDER BY terms to be in SELECT. Broken analysis");

            AggregationNode aggregation = new AggregationNode(idAllocator.getNextId(),
                    subPlan.getRoot(),
                    subPlan.getRoot().getOutputSymbols(),
                    ImmutableMap.<Symbol, FunctionCall>of(),
                    ImmutableMap.<Symbol, FunctionHandle>of());

            return new PlanBuilder(subPlan.getTranslations(), aggregation);
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
        ImmutableMap.Builder<Symbol, SortItem.Ordering> orderings = ImmutableMap.builder();
        for (FieldOrExpression fieldOrExpression : orderByExpressions) {
            Symbol symbol = subPlan.translate(fieldOrExpression);
            orderBySymbols.add(symbol);
            orderings.put(symbol, sortItems.next().getOrdering());
        }

        PlanNode planNode;
        if (limit.isPresent()) {
            planNode = new TopNNode(idAllocator.getNextId(), subPlan.getRoot(), Long.valueOf(limit.get()), orderBySymbols.build(), orderings.build(), false);
        }
        else {
            planNode = new SortNode(idAllocator.getNextId(), subPlan.getRoot(), orderBySymbols.build(), orderings.build());
        }

        return new PlanBuilder(subPlan.getTranslations(), planNode);
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
            return new PlanBuilder(subPlan.getTranslations(), new LimitNode(idAllocator.getNextId(), subPlan.getRoot(), limitValue));
        }

        return subPlan;
    }

    public static Function<Expression, FieldOrExpression> toFieldOrExpression()
    {
        return new Function<Expression, FieldOrExpression>()
        {
            @Nullable
            @Override
            public FieldOrExpression apply(Expression input)
            {
                return new FieldOrExpression(input);
            }
        };
    }
}
