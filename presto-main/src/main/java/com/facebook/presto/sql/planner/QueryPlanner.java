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
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.ExpressionTreeUtils;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldId;
import com.facebook.presto.sql.analyzer.RelationId;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.plan.AssignmentUtils;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Offset;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isSkipRedundantSort;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.plan.AggregationNode.groupingSets;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.NodeUtils.getSortItemsFromOrderBy;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;
import static com.facebook.presto.sql.planner.PlannerUtils.toOrderingScheme;
import static com.facebook.presto.sql.planner.PlannerUtils.toSortOrder;
import static com.facebook.presto.sql.planner.optimizations.WindowNodeUtil.toBoundType;
import static com.facebook.presto.sql.planner.optimizations.WindowNodeUtil.toWindowType;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

class QueryPlanner
{
    private final Analysis analysis;
    private final PlanVariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaDeclarationToVariableMap;
    private final Metadata metadata;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;

    QueryPlanner(
            Analysis analysis,
            PlanVariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaDeclarationToVariableMap,
            Metadata metadata,
            Session session)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToVariableMap, "lambdaDeclarationToVariableMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToVariableMap = lambdaDeclarationToVariableMap;
        this.metadata = metadata;
        this.session = session;
        this.subqueryPlanner = new SubqueryPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session);
    }

    public RelationPlan plan(Query query)
    {
        PlanBuilder builder = planQueryBody(query);

        List<Expression> orderBy = analysis.getOrderByExpressions(query);
        builder = handleSubqueries(builder, query, orderBy);
        List<Expression> outputs = analysis.getOutputExpressions(query);
        builder = handleSubqueries(builder, query, outputs);
        builder = project(builder, Iterables.concat(orderBy, outputs));
        builder = sort(builder, query);
        builder = offset(builder, query.getOffset());
        builder = limit(builder, query);
        builder = project(builder, analysis.getOutputExpressions(query));

        return new RelationPlan(builder.getRoot(), analysis.getScope(query), computeOutputs(builder, analysis.getOutputExpressions(query)));
    }

    public RelationPlan plan(QuerySpecification node)
    {
        PlanBuilder builder = planFrom(node);
        RelationPlan fromRelationPlan = builder.getRelationPlan();

        builder = filter(builder, analysis.getWhere(node), node);
        builder = aggregate(builder, node);
        builder = filter(builder, analysis.getHaving(node), node);

        builder = window(builder, node);

        List<Expression> outputs = analysis.getOutputExpressions(node);
        builder = handleSubqueries(builder, node, outputs);

        if (node.getOrderBy().isPresent()) {
            if (!analysis.isAggregation(node)) {
                // ORDER BY requires both output and source fields to be visible if there are no aggregations
                builder = project(builder, outputs, fromRelationPlan);
                outputs = toSymbolReferences(computeOutputs(builder, outputs));
                builder = planBuilderFor(builder, analysis.getScope(node.getOrderBy().get()));
            }
            else {
                // ORDER BY requires output fields, groups and translated aggregations to be visible for queries with aggregation
                List<Expression> orderByAggregates = analysis.getOrderByAggregates(node.getOrderBy().get());
                builder = project(builder, Iterables.concat(outputs, orderByAggregates));
                outputs = toSymbolReferences(computeOutputs(builder, outputs));
                List<Expression> complexOrderByAggregatesToRemap = orderByAggregates.stream()
                        .filter(expression -> !analysis.isColumnReference(expression))
                        .collect(toImmutableList());
                builder = planBuilderFor(builder, analysis.getScope(node.getOrderBy().get()), complexOrderByAggregatesToRemap);
            }

            builder = window(builder, node.getOrderBy().get());
        }

        List<Expression> orderBy = analysis.getOrderByExpressions(node);
        builder = handleSubqueries(builder, node, orderBy);
        builder = project(builder, Iterables.concat(orderBy, outputs));

        builder = distinct(builder, node);
        builder = sort(builder, node);
        builder = offset(builder, node.getOffset());
        builder = limit(builder, node);
        builder = project(builder, outputs);

        return new RelationPlan(builder.getRoot(), analysis.getScope(node), computeOutputs(builder, outputs));
    }

    public DeleteNode plan(Delete node)
    {
        RelationType descriptor = analysis.getOutputDescriptor(node.getTable());
        TableHandle handle = analysis.getTableHandle(node.getTable());
        ColumnHandle rowIdHandle = metadata.getUpdateRowIdColumnHandle(session, handle);
        Type rowIdType = metadata.getColumnMetadata(session, handle, rowIdHandle).getType();

        // add table columns
        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columns = ImmutableMap.builder();
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (Field field : descriptor.getAllFields()) {
            VariableReferenceExpression variable = variableAllocator.newVariable(getSourceLocation(field.getNodeLocation()), field.getName().get(), field.getType());
            outputVariablesBuilder.add(variable);
            columns.put(variable, analysis.getColumn(field));
            fields.add(field);
        }

        // add rowId column
        Field rowIdField = Field.newUnqualified(node.getLocation(), Optional.empty(), rowIdType);
        VariableReferenceExpression rowIdVariable = variableAllocator.newVariable(getSourceLocation(node), "$rowId", rowIdField.getType());
        outputVariablesBuilder.add(rowIdVariable);
        columns.put(rowIdVariable, rowIdHandle);
        fields.add(rowIdField);

        // create table scan
        List<VariableReferenceExpression> outputVariables = outputVariablesBuilder.build();
        PlanNode tableScan = new TableScanNode(getSourceLocation(node), idAllocator.getNextId(), handle, outputVariables, columns.build(), TupleDomain.all(), TupleDomain.all());
        Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields.build())).build();
        RelationPlan relationPlan = new RelationPlan(tableScan, scope, outputVariables);

        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToVariableMap);
        translations.setFieldMappings(relationPlan.getFieldMappings());

        PlanBuilder builder = new PlanBuilder(translations, relationPlan.getRoot());

        if (node.getWhere().isPresent()) {
            builder = filter(builder, node.getWhere().get(), node);
        }

        // create delete node
        VariableReferenceExpression rowId = new VariableReferenceExpression(Optional.empty(), builder.translate(new FieldReference(relationPlan.getDescriptor().indexOf(rowIdField))).getName(), rowIdField.getType());
        List<VariableReferenceExpression> deleteNodeOutputVariables = ImmutableList.of(
                variableAllocator.newVariable("partialrows", BIGINT),
                variableAllocator.newVariable("fragment", VARBINARY));

        return new DeleteNode(getSourceLocation(node), idAllocator.getNextId(), builder.getRoot(), rowId, deleteNodeOutputVariables);
    }

    private static List<VariableReferenceExpression> computeOutputs(PlanBuilder builder, List<Expression> outputExpressions)
    {
        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
        for (Expression expression : outputExpressions) {
            outputs.add(builder.translate(expression));
        }
        return outputs.build();
    }

    private PlanBuilder planQueryBody(Query query)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session)
                .process(query.getQueryBody(), null);

        return planBuilderFor(relationPlan);
    }

    private PlanBuilder planFrom(QuerySpecification node)
    {
        RelationPlan relationPlan;

        if (node.getFrom().isPresent()) {
            relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session)
                    .process(node.getFrom().get(), null);
        }
        else {
            relationPlan = planImplicitTable();
        }

        return planBuilderFor(relationPlan);
    }

    private PlanBuilder planBuilderFor(PlanBuilder builder, Scope scope, Iterable<? extends Expression> expressionsToRemap)
    {
        PlanBuilder newBuilder = planBuilderFor(builder, scope);
        // We can't deduplicate expressions here because even if two expressions are equal,
        // the TranslationMap maps sql names to symbols, and any lambda expressions will be
        // resolved differently since the lambdaDeclarationToVariableMap is identity based.
        stream(expressionsToRemap)
                .forEach(expression -> newBuilder.getTranslations().put(expression, builder.translate(expression)));
        return newBuilder;
    }

    private PlanBuilder planBuilderFor(PlanBuilder builder, Scope scope)
    {
        return planBuilderFor(new RelationPlan(builder.getRoot(), scope, builder.getRoot().getOutputVariables()));
    }

    private PlanBuilder planBuilderFor(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToVariableMap);

        // Make field->variable mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        return new PlanBuilder(translations, relationPlan.getRoot());
    }

    private RelationPlan planImplicitTable()
    {
        Scope scope = Scope.create();
        return new RelationPlan(
                new ValuesNode(Optional.empty(), idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of(ImmutableList.of()), Optional.empty()),
                scope,
                ImmutableList.of());
    }

    private PlanBuilder filter(PlanBuilder subPlan, Expression predicate, Node node)
    {
        if (predicate == null) {
            return subPlan;
        }

        // rewrite expressions which contain already handled subqueries
        Expression rewrittenBeforeSubqueries = subPlan.rewrite(predicate);
        subPlan = subqueryPlanner.handleSubqueries(subPlan, rewrittenBeforeSubqueries, node);
        Expression rewrittenAfterSubqueries = subPlan.rewrite(predicate);

        return subPlan.withNewRoot(new FilterNode(getSourceLocation(node), idAllocator.getNextId(), subPlan.getRoot(), castToRowExpression(rewrittenAfterSubqueries)));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<Expression> expressions, RelationPlan parentRelationPlan)
    {
        return project(subPlan, Iterables.concat(expressions, toSymbolReferences(parentRelationPlan.getFieldMappings())));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<Expression> expressions)
    {
        TranslationMap outputTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);

        Assignments.Builder projections = Assignments.builder();
        for (Expression expression : expressions) {
            if (expression instanceof SymbolReference) {
                VariableReferenceExpression variable = variableAllocator.toVariableReference(expression);
                projections.put(variable, castToRowExpression(expression));
                outputTranslations.put(expression, variable);
                continue;
            }

            VariableReferenceExpression variable = variableAllocator.newVariable(expression, analysis.getTypeWithCoercions(expression));
            projections.put(variable, castToRowExpression(subPlan.rewrite(expression)));
            outputTranslations.put(expression, variable);
        }

        return new PlanBuilder(outputTranslations, new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                projections.build()));
    }

    private Map<VariableReferenceExpression, RowExpression> coerce(Iterable<? extends Expression> expressions, PlanBuilder subPlan, TranslationMap translations)
    {
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> projections = ImmutableMap.builder();

        for (Expression expression : expressions) {
            Type type = analysis.getType(expression);
            Type coercion = analysis.getCoercion(expression);
            VariableReferenceExpression variable = variableAllocator.newVariable(expression, firstNonNull(coercion, type));
            Expression rewritten = subPlan.rewrite(expression);
            if (coercion != null) {
                rewritten = new Cast(
                        rewritten,
                        coercion.getTypeSignature().toString(),
                        false,
                        metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(type, coercion));
            }
            projections.put(variable, castToRowExpression(rewritten));
            translations.put(expression, variable);
        }

        return projections.build();
    }

    private PlanBuilder explicitCoercionFields(PlanBuilder subPlan, Iterable<Expression> alreadyCoerced, Iterable<? extends Expression> uncoerced)
    {
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);
        Assignments.Builder projections = Assignments.builder();

        projections.putAll(coerce(uncoerced, subPlan, translations));

        for (Expression expression : alreadyCoerced) {
            if (expression instanceof SymbolReference) {
                // If this is an identity projection, no need to rewrite it
                // This is needed because certain synthetic identity expressions such as "group id" introduced when planning GROUPING
                // don't have a corresponding analysis, so the code below doesn't work for them
                projections.put(variableAllocator.toVariableReference(expression), castToRowExpression(expression));
                continue;
            }

            VariableReferenceExpression variable = variableAllocator.newVariable(expression, analysis.getType(expression));
            Expression rewritten = subPlan.rewrite(expression);
            projections.put(variable, castToRowExpression(rewritten));
            translations.put(expression, variable);
        }

        return new PlanBuilder(translations, new ProjectNode(
                subPlan.getRoot().getSourceLocation(),
                idAllocator.getNextId(),
                subPlan.getRoot(),
                projections.build(),
                LOCAL));
    }

    private PlanBuilder explicitCoercionVariables(PlanBuilder subPlan, List<VariableReferenceExpression> alreadyCoerced, Iterable<? extends Expression> uncoerced)
    {
        TranslationMap translations = subPlan.copyTranslations();

        Assignments assignments = Assignments.builder()
                .putAll(coerce(uncoerced, subPlan, translations))
                .putAll(identitiesAsSymbolReferences(alreadyCoerced))
                .build();

        return new PlanBuilder(translations, new ProjectNode(
                subPlan.getRoot().getSourceLocation(),
                idAllocator.getNextId(),
                subPlan.getRoot(),
                assignments,
                LOCAL));
    }

    private PlanBuilder aggregate(PlanBuilder subPlan, QuerySpecification node)
    {
        if (!analysis.isAggregation(node)) {
            return subPlan;
        }

        // 1. Pre-project all scalar inputs (arguments and non-trivial group by expressions)
        Set<Expression> groupByExpressions = ImmutableSet.copyOf(analysis.getGroupByExpressions(node));

        ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getArguments)
                .flatMap(List::stream)
                .filter(exp -> !(exp instanceof LambdaExpression)) // lambda expression is generated at execution time
                .forEach(arguments::add);

        analysis.getAggregates(node).stream()
                .map(FunctionCall::getOrderBy)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(OrderBy::getSortItems)
                .flatMap(List::stream)
                .map(SortItem::getSortKey)
                .forEach(arguments::add);

        // filter expressions need to be projected first
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getFilter)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(arguments::add);

        Iterable<Expression> inputs = Iterables.concat(groupByExpressions, arguments.build());
        subPlan = handleSubqueries(subPlan, node, inputs);

        if (!Iterables.isEmpty(inputs)) { // avoid an empty projection if the only aggregation is COUNT (which has no arguments)
            subPlan = project(subPlan, inputs);
        }

        // 2. Aggregate

        // 2.a. Rewrite aggregate arguments
        TranslationMap argumentTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);

        ImmutableList.Builder<VariableReferenceExpression> aggregationArgumentsBuilder = ImmutableList.builder();
        for (Expression argument : arguments.build()) {
            VariableReferenceExpression variable = subPlan.translate(argument);
            argumentTranslations.put(argument, variable);
            aggregationArgumentsBuilder.add(variable);
        }
        List<VariableReferenceExpression> aggregationArguments = aggregationArgumentsBuilder.build();

        // 2.b. Rewrite grouping columns
        TranslationMap groupingTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);
        Map<VariableReferenceExpression, VariableReferenceExpression> groupingSetMappings = new LinkedHashMap<>();

        for (Expression expression : groupByExpressions) {
            VariableReferenceExpression input = subPlan.translate(expression);
            VariableReferenceExpression output = variableAllocator.newVariable(expression, analysis.getTypeWithCoercions(expression), "gid");
            groupingTranslations.put(expression, output);
            groupingSetMappings.put(output, input);
        }

        // This tracks the grouping sets before complex expressions are considered (see comments below)
        // It's also used to compute the descriptors needed to implement grouping()
        List<Set<FieldId>> columnOnlyGroupingSets = ImmutableList.of(ImmutableSet.of());
        List<List<VariableReferenceExpression>> groupingSets = ImmutableList.of(ImmutableList.of());

        if (node.getGroupBy().isPresent()) {
            // For the purpose of "distinct", we need to canonicalize column references that may have varying
            // syntactic forms (e.g., "t.a" vs "a"). Thus we need to enumerate grouping sets based on the underlying
            // fieldId associated with each column reference expression.

            // The catch is that simple group-by expressions can be arbitrary expressions (this is a departure from the SQL specification).
            // But, they don't affect the number of grouping sets or the behavior of "distinct" . We can compute all the candidate
            // grouping sets in terms of fieldId, dedup as appropriate and then cross-join them with the complex expressions.
            Analysis.GroupingSetAnalysis groupingSetAnalysis = analysis.getGroupingSets(node);
            columnOnlyGroupingSets = enumerateGroupingSets(groupingSetAnalysis);

            if (node.getGroupBy().get().isDistinct()) {
                columnOnlyGroupingSets = columnOnlyGroupingSets.stream()
                        .distinct()
                        .collect(toImmutableList());
            }

            // add in the complex expressions an turn materialize the grouping sets in terms of plan columns
            ImmutableList.Builder<List<VariableReferenceExpression>> groupingSetBuilder = ImmutableList.builder();
            for (Set<FieldId> groupingSet : columnOnlyGroupingSets) {
                ImmutableList.Builder<VariableReferenceExpression> columns = ImmutableList.builder();
                groupingSetAnalysis.getComplexExpressions().stream()
                        .map(groupingTranslations::get)
                        .forEach(columns::add);

                groupingSet.stream()
                        .map(field -> groupingTranslations.get(new FieldReference(field.getFieldIndex())))
                        .forEach(columns::add);

                groupingSetBuilder.add(columns.build());
            }

            groupingSets = groupingSetBuilder.build();
        }

        // 2.c. Generate GroupIdNode (multiple grouping sets) or ProjectNode (single grouping set)
        Optional<VariableReferenceExpression> groupIdVariable = Optional.empty();
        if (groupingSets.size() > 1) {
            groupIdVariable = Optional.of(variableAllocator.newVariable("groupId", BIGINT));
            GroupIdNode groupId = new GroupIdNode(subPlan.getRoot().getSourceLocation(), idAllocator.getNextId(), subPlan.getRoot(), groupingSets, groupingSetMappings, aggregationArguments, groupIdVariable.get());
            subPlan = new PlanBuilder(groupingTranslations, groupId);
        }
        else {
            Assignments.Builder assignments = Assignments.builder();
            aggregationArguments.stream().map(AssignmentUtils::identityAsSymbolReference).forEach(assignments::put);
            groupingSetMappings.forEach((key, value) -> assignments.put(key, castToRowExpression(asSymbolReference(value))));

            ProjectNode project = new ProjectNode(subPlan.getRoot().getSourceLocation(), idAllocator.getNextId(), subPlan.getRoot(), assignments.build(), LOCAL);
            subPlan = new PlanBuilder(groupingTranslations, project);
        }

        TranslationMap aggregationTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);
        aggregationTranslations.copyMappingsFrom(groupingTranslations);

        // 2.d. Rewrite aggregates
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregationsBuilder = ImmutableMap.builder();
        boolean needPostProjectionCoercion = false;
        for (FunctionCall aggregate : analysis.getAggregates(node)) {
            Expression rewritten = argumentTranslations.rewrite(aggregate);
            VariableReferenceExpression newVariable = variableAllocator.newVariable(rewritten, analysis.getType(aggregate));

            // TODO: this is a hack, because we apply coercions to the output of expressions, rather than the arguments to expressions.
            // Therefore we can end up with this implicit cast, and have to move it into a post-projection
            if (rewritten instanceof Cast) {
                rewritten = ((Cast) rewritten).getExpression();
                needPostProjectionCoercion = true;
            }
            aggregationTranslations.put(aggregate, newVariable);
            FunctionCall rewrittenFunction = (FunctionCall) rewritten;

            aggregationsBuilder.put(newVariable,
                    new Aggregation(
                            new CallExpression(
                                    getSourceLocation(rewrittenFunction),
                                    aggregate.getName().getSuffix(),
                                    analysis.getFunctionHandle(aggregate),
                                    analysis.getType(aggregate),
                                    rewrittenFunction.getArguments().stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList())),
                            rewrittenFunction.getFilter().map(OriginalExpressionUtils::castToRowExpression),
                            rewrittenFunction.getOrderBy().map(orderBy -> toOrderingScheme(orderBy, variableAllocator.getTypes())),
                            rewrittenFunction.isDistinct(),
                            Optional.empty()));
        }
        Map<VariableReferenceExpression, Aggregation> aggregations = aggregationsBuilder.build();

        ImmutableSet.Builder<Integer> globalGroupingSets = ImmutableSet.builder();
        for (int i = 0; i < groupingSets.size(); i++) {
            if (groupingSets.get(i).isEmpty()) {
                globalGroupingSets.add(i);
            }
        }

        ImmutableList.Builder<VariableReferenceExpression> groupingKeys = ImmutableList.builder();
        groupingSets.stream()
                .flatMap(List::stream)
                .distinct()
                .forEach(groupingKeys::add);
        groupIdVariable.ifPresent(groupingKeys::add);

        AggregationNode aggregationNode = new AggregationNode(
                subPlan.getRoot().getSourceLocation(),
                idAllocator.getNextId(),
                subPlan.getRoot(),
                aggregations,
                groupingSets(
                        groupingKeys.build(),
                        groupingSets.size(),
                        globalGroupingSets.build()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                groupIdVariable);

        subPlan = new PlanBuilder(aggregationTranslations, aggregationNode);

        // 3. Post-projection
        // Add back the implicit casts that we removed in 2.a
        // TODO: this is a hack, we should change type coercions to coerce the inputs to functions/operators instead of coercing the output
        if (needPostProjectionCoercion) {
            ImmutableList.Builder<Expression> alreadyCoerced = ImmutableList.builder();
            alreadyCoerced.addAll(groupByExpressions);
            groupIdVariable.map(ExpressionTreeUtils::createSymbolReference).ifPresent(alreadyCoerced::add);

            subPlan = explicitCoercionFields(subPlan, alreadyCoerced.build(), analysis.getAggregates(node));
        }

        // 4. Project and re-write all grouping functions
        return handleGroupingOperations(subPlan, node, groupIdVariable, columnOnlyGroupingSets);
    }

    private List<Set<FieldId>> enumerateGroupingSets(Analysis.GroupingSetAnalysis groupingSetAnalysis)
    {
        List<List<Set<FieldId>>> partialSets = new ArrayList<>();

        for (Set<FieldId> cube : groupingSetAnalysis.getCubes()) {
            partialSets.add(ImmutableList.copyOf(Sets.powerSet(cube)));
        }

        for (List<FieldId> rollup : groupingSetAnalysis.getRollups()) {
            List<Set<FieldId>> sets = IntStream.rangeClosed(0, rollup.size())
                    .mapToObj(i -> ImmutableSet.copyOf(rollup.subList(0, i)))
                    .collect(toImmutableList());

            partialSets.add(sets);
        }

        partialSets.addAll(groupingSetAnalysis.getOrdinarySets());

        if (partialSets.isEmpty()) {
            return ImmutableList.of(ImmutableSet.of());
        }

        // compute the cross product of the partial sets
        List<Set<FieldId>> allSets = new ArrayList<>();
        partialSets.get(0)
                .stream()
                .map(ImmutableSet::copyOf)
                .forEach(allSets::add);

        for (int i = 1; i < partialSets.size(); i++) {
            List<Set<FieldId>> groupingSets = partialSets.get(i);
            List<Set<FieldId>> oldGroupingSetsCrossProduct = ImmutableList.copyOf(allSets);
            allSets.clear();
            for (Set<FieldId> existingSet : oldGroupingSetsCrossProduct) {
                for (Set<FieldId> groupingSet : groupingSets) {
                    Set<FieldId> concatenatedSet = ImmutableSet.<FieldId>builder()
                            .addAll(existingSet)
                            .addAll(groupingSet)
                            .build();
                    allSets.add(concatenatedSet);
                }
            }
        }

        return allSets;
    }

    private PlanBuilder handleGroupingOperations(PlanBuilder subPlan, QuerySpecification node, Optional<VariableReferenceExpression> groupIdVariable, List<Set<FieldId>> groupingSets)
    {
        if (analysis.getGroupingOperations(node).isEmpty()) {
            return subPlan;
        }

        TranslationMap newTranslations = subPlan.copyTranslations();

        Assignments.Builder projections = Assignments.builder();
        projections.putAll(identitiesAsSymbolReferences(subPlan.getRoot().getOutputVariables()));

        List<Set<Integer>> descriptor = groupingSets.stream()
                .map(set -> set.stream()
                        .map(FieldId::getFieldIndex)
                        .collect(toImmutableSet()))
                .collect(toImmutableList());

        for (GroupingOperation groupingOperation : analysis.getGroupingOperations(node)) {
            Expression rewritten = GroupingOperationRewriter.rewriteGroupingOperation(groupingOperation, descriptor, analysis.getColumnReferenceFields(), groupIdVariable);
            Type coercion = analysis.getCoercion(groupingOperation);
            VariableReferenceExpression variable = variableAllocator.newVariable(rewritten, analysis.getTypeWithCoercions(groupingOperation));
            if (coercion != null) {
                rewritten = new Cast(
                        rewritten,
                        coercion.getTypeSignature().toString(),
                        false,
                        metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(analysis.getType(groupingOperation), coercion));
            }
            projections.put(variable, castToRowExpression(rewritten));
            newTranslations.put(groupingOperation, variable);
        }

        return new PlanBuilder(newTranslations, new ProjectNode(subPlan.getRoot().getSourceLocation(), idAllocator.getNextId(), subPlan.getRoot(), projections.build(), LOCAL));
    }

    private PlanBuilder window(PlanBuilder subPlan, OrderBy node)
    {
        return window(subPlan, ImmutableList.copyOf(analysis.getOrderByWindowFunctions(node)));
    }

    private PlanBuilder window(PlanBuilder subPlan, QuerySpecification node)
    {
        return window(subPlan, ImmutableList.copyOf(analysis.getWindowFunctions(node)));
    }

    private PlanBuilder window(PlanBuilder subPlan, List<FunctionCall> windowFunctions)
    {
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
                    .addAll(Iterables.transform(getSortItemsFromOrderBy(window.getOrderBy()), SortItem::getSortKey));

            if (frameStart != null) {
                inputs.add(frameStart);
            }
            if (frameEnd != null) {
                inputs.add(frameEnd);
            }

            subPlan = subPlan.appendProjections(inputs.build(), variableAllocator, idAllocator);

            // Rewrite PARTITION BY in terms of pre-projected inputs
            ImmutableList.Builder<VariableReferenceExpression> partitionByVariables = ImmutableList.builder();
            for (Expression expression : window.getPartitionBy()) {
                partitionByVariables.add(subPlan.translateToVariable(expression));
            }

            // Rewrite ORDER BY in terms of pre-projected inputs
            LinkedHashMap<VariableReferenceExpression, SortOrder> orderings = new LinkedHashMap<>();
            for (SortItem item : getSortItemsFromOrderBy(window.getOrderBy())) {
                VariableReferenceExpression variable = subPlan.translateToVariable(item.getSortKey());
                // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
                orderings.putIfAbsent(variable, toSortOrder(item));
            }

            // Rewrite frame bounds in terms of pre-projected inputs
            Optional<VariableReferenceExpression> frameStartVariable = Optional.empty();
            Optional<VariableReferenceExpression> frameEndVariable = Optional.empty();
            if (frameStart != null) {
                frameStartVariable = Optional.of(subPlan.translate(frameStart));
            }
            if (frameEnd != null) {
                frameEndVariable = Optional.of(subPlan.translate(frameEnd));
            }

            WindowNode.Frame frame = new WindowNode.Frame(
                    toWindowType(frameType),
                    toBoundType(frameStartType),
                    frameStartVariable,
                    toBoundType(frameEndType),
                    frameEndVariable,
                    Optional.ofNullable(frameStart).map(Expression::toString),
                    Optional.ofNullable(frameEnd).map(Expression::toString));

            TranslationMap outputTranslations = subPlan.copyTranslations();

            // Rewrite function call in terms of pre-projected inputs
            Expression rewritten = subPlan.rewrite(windowFunction);

            boolean needCoercion = rewritten instanceof Cast;
            // Strip out the cast and add it back as a post-projection
            if (rewritten instanceof Cast) {
                rewritten = ((Cast) rewritten).getExpression();
            }

            // If refers to existing variable, don't create another PlanNode
            if (rewritten instanceof SymbolReference) {
                if (needCoercion) {
                    subPlan = explicitCoercionVariables(subPlan, subPlan.getRoot().getOutputVariables(), ImmutableList.of(windowFunction));
                }

                continue;
            }

            Type returnType = analysis.getType(windowFunction);
            VariableReferenceExpression newVariable = variableAllocator.newVariable(rewritten, returnType);
            outputTranslations.put(windowFunction, newVariable);

            // TODO: replace arguments with RowExpression once we introduce subquery expression for RowExpression (#12745).
            // Wrap all arguments in CallExpression to be RawExpression.
            // The utility that work on the CallExpression should be aware of the RawExpression handling.
            // The interface will be dirty until we introduce subquery expression for RowExpression.
            // With subqueries, the translation from Expression to RowExpression can happen here.
            WindowNode.Function function = new WindowNode.Function(
                    call(
                            windowFunction.getName().toString(),
                            analysis.getFunctionHandle(windowFunction),
                            returnType,
                            ((FunctionCall) rewritten).getArguments().stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList())),
                    frame,
                    windowFunction.isIgnoreNulls());

            ImmutableList.Builder<VariableReferenceExpression> orderByVariables = ImmutableList.builder();
            orderByVariables.addAll(orderings.keySet());
            Optional<OrderingScheme> orderingScheme = Optional.empty();
            if (!orderings.isEmpty()) {
                orderingScheme = Optional.of(new OrderingScheme(orderByVariables.build().stream().map(variable -> new Ordering(variable, orderings.get(variable))).collect(toImmutableList())));
            }

            // create window node
            subPlan = new PlanBuilder(outputTranslations,
                    new WindowNode(
                            subPlan.getRoot().getSourceLocation(),
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            new WindowNode.Specification(
                                    partitionByVariables.build(),
                                    orderingScheme),
                            ImmutableMap.of(newVariable, function),
                            Optional.empty(),
                            ImmutableSet.of(),
                            0));

            if (needCoercion) {
                subPlan = explicitCoercionVariables(subPlan, subPlan.getRoot().getOutputVariables(), ImmutableList.of(windowFunction));
            }
        }

        return subPlan;
    }

    private PlanBuilder handleSubqueries(PlanBuilder subPlan, Node node, Iterable<Expression> inputs)
    {
        for (Expression input : inputs) {
            subPlan = subqueryPlanner.handleSubqueries(subPlan, subPlan.rewrite(input), node);
        }
        return subPlan;
    }

    private PlanBuilder distinct(PlanBuilder subPlan, QuerySpecification node)
    {
        if (node.getSelect().isDistinct()) {
            return subPlan.withNewRoot(
                    new AggregationNode(
                            getSourceLocation(node),
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            ImmutableMap.of(),
                            singleGroupingSet(subPlan.getRoot().getOutputVariables()),
                            ImmutableList.of(),
                            AggregationNode.Step.SINGLE,
                            Optional.empty(),
                            Optional.empty()));
        }

        return subPlan;
    }

    private PlanBuilder sort(PlanBuilder subPlan, Query node)
    {
        return sort(subPlan, node.getOrderBy(), analysis.getOrderByExpressions(node));
    }

    private PlanBuilder sort(PlanBuilder subPlan, QuerySpecification node)
    {
        return sort(subPlan, node.getOrderBy(), analysis.getOrderByExpressions(node));
    }

    private PlanBuilder sort(PlanBuilder subPlan, Optional<OrderBy> orderBy, List<Expression> orderByExpressions)
    {
        if (!orderBy.isPresent() || (isSkipRedundantSort(session)) && analysis.isOrderByRedundant(orderBy.get())) {
            return subPlan;
        }

        PlanNode planNode;
        OrderingScheme orderingScheme = toOrderingScheme(
                orderByExpressions.stream().map(subPlan::translate).collect(toImmutableList()),
                orderBy.get().getSortItems().stream().map(PlannerUtils::toSortOrder).collect(toImmutableList()));
        planNode = new SortNode(getSourceLocation(orderBy.get()), idAllocator.getNextId(), subPlan.getRoot(), orderingScheme, false);

        return subPlan.withNewRoot(planNode);
    }

    private PlanBuilder offset(PlanBuilder subPlan, Optional<Offset> offset)
    {
        if (!offset.isPresent()) {
            return subPlan;
        }

        return subPlan.withNewRoot(
                new OffsetNode(
                        getSourceLocation(offset.get()),
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        analysis.getOffset(offset.get())));
    }

    private PlanBuilder limit(PlanBuilder subPlan, Query node)
    {
        return limit(subPlan, node.getLimit());
    }

    private PlanBuilder limit(PlanBuilder subPlan, QuerySpecification node)
    {
        return limit(subPlan, node.getLimit());
    }

    private PlanBuilder limit(PlanBuilder subPlan, Optional<String> limit)
    {
        if (!limit.isPresent()) {
            return subPlan;
        }

        if (!limit.get().equalsIgnoreCase("all")) {
            long limitValue = Long.parseLong(limit.get());
            subPlan = subPlan.withNewRoot(new LimitNode(subPlan.getRoot().getSourceLocation(), idAllocator.getNextId(), subPlan.getRoot(), limitValue, FINAL));
        }

        return subPlan;
    }

    private static List<Expression> toSymbolReferences(List<VariableReferenceExpression> variables)
    {
        return variables.stream()
                .map(variable -> new SymbolReference(
                        variable.getSourceLocation().map(location -> new NodeLocation(location.getLine(), location.getColumn())),
                        variable.getName()))
                .collect(toImmutableList());
    }
}
