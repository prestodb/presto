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

import com.facebook.presto.Session;
import com.facebook.presto.common.TransformationSubtype;
import com.facebook.presto.metadata.BuiltInFunctionHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransform;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformArgument;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProperties;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProvider;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.MaterializedViewDefinition.ColumnProvenance;
import static com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import static com.facebook.presto.sql.MaterializedViewUtils.transitiveClosure;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.planner.LiteralInterpreter.evaluate;
import static com.facebook.presto.sql.tree.Join.Type.INNER;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MaterializedViewColumnMappingExtractor
        extends MaterializedViewPlanValidator
{
    private final Analysis analysis;
    private final Session session;
    private final Metadata metadata;

    /**
     * We create a undirected graph where each node corresponds to a base table column.
     * Edges represent mapping by JOIN criteria, UNION, INTERSECT etc.
     * <p>
     * For example, given SELECT column_a AS column_x FROM table_a JOIN table_b ON (table_a.column_a = table_b.column_b),
     * (table_a.column_a, table_b.column_b) is an edge in the graph.
     * </p>
     */
    private Map<TableColumn, Set<TableColumn>> mappedBaseColumns;

    /**
     * Similar to `mappedBaseColumns`, but ignores EQ clauses from outer joins.
     * <p>
     * For example, given SELECT column_a FROM table_a LEFT JOIN table_b ON (table_a.column_a = table_b.column_b),
     * produces no mappings.
     * </p>
     */
    private Map<TableColumn, Set<TableColumn>> directMappedBaseColumns;

    /**
     * We create a list of tables where all columns can possibly become null,
     * rather than being 1-1 mapped to MV outputs. This implies that these tables
     * are on the outer side of a join.
     * <p>
     * For example, A LEFT JOIN (B LEFT JOIN C) has [B, C] as base tables
     * on the outer join side
     * </p>
     */
    private List<SchemaTableName> baseTablesOnOuterJoinSide;

    public MaterializedViewColumnMappingExtractor(Analysis analysis, Session session, Metadata metadata)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.mappedBaseColumns = new HashMap<>();
        this.directMappedBaseColumns = new HashMap<>();
        this.baseTablesOnOuterJoinSide = new ArrayList<>();

        checkState(
                analysis.getStatement() instanceof CreateMaterializedView,
                "Only support extracting of mapped columns from create materialized view query");

        Query viewQuery = ((CreateMaterializedView) analysis.getStatement()).getQuery();
        process(viewQuery, new MaterializedViewPlanValidatorContext());
        this.mappedBaseColumns = transitiveClosure(mappedBaseColumns);
        this.directMappedBaseColumns = transitiveClosure(directMappedBaseColumns);
    }

    /**
     * Compute the 1-to-N column mapping from a materialized view to its base tables.
     * <p>
     * From {@code analysis}, we could derive only one base table column that one materialized view column maps to.
     * In case of 1 materialized view defined on N base tables via join, union, etc, this method helps compute
     * all the N base table columns that one materialized view column maps to.
     * <p>
     * For example, given SELECT column_a AS column_x FROM table_a JOIN table_b ON (table_a.column_a = table_b.column_b),
     * the 1-to-1 column mapping from {@code analysis} is column_x -> table_a.column_a. Mapped base table columns are
     * [table_a.column_a, table_b.column_b]. Then it will return a 1-to-N column mapping column_x -> {table_a -> column_a, table_b -> column_b}.
     */
    public Map<String, Map<SchemaTableName, String>> getMaterializedViewColumnMappings()
    {
        return getColumnMappings(analysis, mappedBaseColumns);
    }

    /**
     * Similar to getMaterializedViewColumnMappings, but only includes direct column mappings. It excludes columns mappings
     * derived from outer join EQ clauses.
     * <p>
     * For example, given SELECT t1_a as t1.a, t2_a as t2.a FROM t1 LEFT JOIN t2 ON t1.a = t2.a
     * Column mappings are:
     * t1_a -> {t1 -> t1.a}
     * t2_a -> {t2 -> t2.a}
     */
    public Map<String, Map<SchemaTableName, String>> getMaterializedViewDirectColumnMappings()
    {
        return getColumnMappings(analysis, directMappedBaseColumns);
    }

    public List<SchemaTableName> getBaseTablesOnOuterJoinSide()
    {
        return Collections.unmodifiableList(baseTablesOnOuterJoinSide);
    }

    // Mappings for derived (expression/aggregation) view columns, from DIRECT column lineage.
    // These are absent from getMaterializedViewColumnMappings(), which only covers 1:1 references.
    public Map<String, List<TableColumn>> getMaterializedViewDerivedColumnMappings()
    {
        Query viewQuery = ((CreateMaterializedView) analysis.getStatement()).getQuery();
        Set<String> mappedColumns = getMaterializedViewColumnMappings().keySet();
        Map<String, Expression> outputExpressions = getOutputExpressions();

        Map<String, List<TableColumn>> derivedColumnMappings = new HashMap<>();
        for (Field field : analysis.getOutputDescriptor(viewQuery).getVisibleFields()) {
            if (!field.getName().isPresent() || mappedColumns.contains(field.getName().get())) {
                continue;
            }
            String viewColumn = field.getName().get();
            Expression outputExpression = outputExpressions.get(viewColumn);
            if (outputExpression != null) {
                validatePartitionTransformIsDirect(outputExpression, viewColumn);
            }
            Optional<MaterializedViewPartitionTransform> partitionTransform = tryExtractPartitionTransform(outputExpression);
            List<TableColumn> baseColumns = analysis.getDirectLineageEntries(field).stream()
                    .map(entry -> new TableColumn(toSchemaTableName(entry.getTableName()), entry.getColumnName(), false))
                    .distinct()
                    .collect(toImmutableList());
            if (partitionTransform.isPresent()) {
                if (baseColumns.size() != 1) {
                    throw new SemanticException(
                            NOT_SUPPORTED,
                            outputExpression,
                            "Partition transform producing column %s reads %s source columns. It must read exactly one direct source column.",
                            viewColumn,
                            baseColumns.size());
                }
                FunctionCall functionCall = (FunctionCall) outputExpression;
                int sourceArgumentIndex = partitionTransform.get().getProperties().getSourceArgumentIndex();
                if (!analysis.isColumnReference(functionCall.getArguments().get(sourceArgumentIndex))) {
                    throw new SemanticException(
                            NOT_SUPPORTED,
                            functionCall.getArguments().get(sourceArgumentIndex),
                            "Partition transform producing column %s must use a direct source column.",
                            viewColumn);
                }
                TableColumn sourceColumn = baseColumns.get(0);
                baseColumns = ImmutableList.of(new TableColumn(
                        sourceColumn.getTableName(),
                        sourceColumn.getColumnName(),
                        false,
                        partitionTransform.get().encode()));
            }
            if (!baseColumns.isEmpty()) {
                derivedColumnMappings.put(viewColumn, baseColumns);
            }
        }
        return derivedColumnMappings;
    }

    /**
     * How each output column was produced, keyed by view column name. Unlike the lineage maps
     * this covers every named output column, including aggregates with no source column such as
     * {@code COUNT(*)} — which is precisely the case the merge-partner check at the next tier
     * exists to reject.
     */
    public Map<String, ColumnProvenance> getMaterializedViewColumnProvenance()
    {
        Query viewQuery = ((CreateMaterializedView) analysis.getStatement()).getQuery();
        Map<String, Expression> outputExpressions = getOutputExpressions();

        ImmutableMap.Builder<String, ColumnProvenance> provenance = ImmutableMap.builder();
        for (Field field : analysis.getOutputDescriptor(viewQuery).getVisibleFields()) {
            if (!field.getName().isPresent()) {
                continue;
            }
            String outputName = field.getName().get();
            Expression expression = outputExpressions.get(outputName);
            Optional<String> outputType = Optional.ofNullable(field.getType()).map(type -> type.getTypeSignature().toString());
            if (expression == null) {
                if (!analysis.getDirectLineageEntries(field).isEmpty()) {
                    provenance.put(outputName, new ColumnProvenance(
                            Optional.empty(),
                            determinePropagatedSubtype(field),
                            outputType,
                            false));
                }
                continue;
            }

            Optional<String> functionName = Optional.empty();
            boolean distinct = false;
            if (expression instanceof FunctionCall) {
                FunctionCall functionCall = (FunctionCall) expression;
                functionName = getBuiltInFunctionName(functionCall);
                distinct = functionCall.isDistinct();
            }
            provenance.put(outputName, new ColumnProvenance(
                    functionName,
                    determineDirectSubtype(expression),
                    outputType,
                    distinct));
        }
        return provenance.build();
    }

    private TransformationSubtype determinePropagatedSubtype(Field field)
    {
        Set<TransformationSubtype> subtypes = analysis.getDirectLineageEntries(field).stream()
                .map(entry -> entry.getTransformationSubtype())
                .collect(toImmutableSet());
        if (subtypes.contains(TransformationSubtype.AGGREGATION)) {
            return TransformationSubtype.AGGREGATION;
        }
        if (subtypes.contains(TransformationSubtype.TRANSFORMATION)) {
            return TransformationSubtype.TRANSFORMATION;
        }
        return TransformationSubtype.IDENTITY;
    }

    /**
     * Select-list expressions keyed by the output column name they produce. Only named single
     * columns are represented; {@code SELECT *} expands to fields with no select-item of their
     * own and is skipped, which is safe because a wildcard cannot introduce an aggregate.
     */
    private Map<String, Expression> getOutputExpressions()
    {
        Query viewQuery = ((CreateMaterializedView) analysis.getStatement()).getQuery();
        if (!(viewQuery.getQueryBody() instanceof QuerySpecification)) {
            if (viewQuery.getQueryBody() instanceof SetOperation) {
                List<FunctionCall> partitionTransforms = extractExpressions(ImmutableList.of(viewQuery), FunctionCall.class).stream()
                        .filter(this::isRegisteredPartitionTransform)
                        .collect(toImmutableList());
                if (!partitionTransforms.isEmpty()) {
                    throw new SemanticException(
                            NOT_SUPPORTED,
                            partitionTransforms.get(0),
                            "Partition transforms are not supported in set-operation materialized views");
                }
            }
            return ImmutableMap.of();
        }

        Map<String, Expression> outputExpressions = new HashMap<>();
        for (SelectItem selectItem : ((QuerySpecification) viewQuery.getQueryBody()).getSelect().getSelectItems()) {
            if (!(selectItem instanceof SingleColumn)) {
                continue;
            }
            SingleColumn singleColumn = (SingleColumn) selectItem;
            Optional<String> name = singleColumn.getAlias().map(Identifier::getValue);
            if (!name.isPresent() && singleColumn.getExpression() instanceof Identifier) {
                name = Optional.of(((Identifier) singleColumn.getExpression()).getValue());
            }
            name.ifPresent(columnName -> outputExpressions.put(columnName, singleColumn.getExpression()));
        }
        return outputExpressions;
    }

    private Optional<MaterializedViewPartitionTransform> tryExtractPartitionTransform(Expression expression)
    {
        if (!(expression instanceof FunctionCall)) {
            return Optional.empty();
        }
        FunctionCall functionCall = (FunctionCall) expression;
        FunctionMetadata function = getFunctionMetadata(functionCall);
        Optional<MaterializedViewPartitionTransformProvider> provider = getPartitionTransformProvider(functionCall, function);
        if (!provider.isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<MaterializedViewPartitionTransformArgument> arguments = ImmutableList.builder();
        for (Expression argument : functionCall.getArguments()) {
            if (analysis.isColumnReference(argument)) {
                arguments.add(MaterializedViewPartitionTransformArgument.sourceColumn(analysis.getType(argument)));
            }
            else if (argument instanceof Literal) {
                Object value = evaluate(metadata, session.toConnectorSession(), (Literal) argument);
                arguments.add(MaterializedViewPartitionTransformArgument.constant(
                        new ConstantExpression(Optional.empty(), value, analysis.getType(argument))));
            }
            else {
                throw new SemanticException(
                        NOT_SUPPORTED,
                        argument,
                        "Partition transform %s requires one direct source column and constant literal arguments",
                        functionCall.getName());
            }
        }
        try {
            List<MaterializedViewPartitionTransformArgument> transformArguments = arguments.build();
            MaterializedViewPartitionTransform transform = provider.get().analyze(function, transformArguments)
                    .orElseThrow(() -> new IllegalArgumentException("Function call is not supported as a materialized view partition transform"));
            validateTransformArguments(transformArguments, transform.getProperties());
            return Optional.of(transform);
        }
        catch (IllegalArgumentException e) {
            throw new SemanticException(
                    NOT_SUPPORTED,
                    functionCall,
                    e.getMessage());
        }
    }

    private void validateTransformArguments(
            List<MaterializedViewPartitionTransformArgument> arguments,
            MaterializedViewPartitionTransformProperties properties)
    {
        int argumentCount = arguments.size();
        if (properties.getSourceArgumentIndex() >= argumentCount ||
                properties.getConstantArgumentIndexes().stream().anyMatch(index -> index >= argumentCount) ||
                properties.getConstantArgumentIndexes().size() + 1 != argumentCount) {
            throw new IllegalArgumentException("Partition transform properties do not match the function arguments");
        }
        if (!arguments.get(properties.getSourceArgumentIndex()).isSourceColumn()) {
            throw new IllegalArgumentException("Partition transform source argument is not a direct column");
        }
        for (int index : properties.getConstantArgumentIndexes()) {
            if (!arguments.get(index).getConstant().isPresent()) {
                throw new IllegalArgumentException("Partition transform configuration argument is not constant");
            }
        }
    }

    private void validatePartitionTransformIsDirect(Expression expression, String viewColumn)
    {
        List<FunctionCall> partitionTransforms = extractExpressions(ImmutableList.of(expression), FunctionCall.class).stream()
                .filter(this::isRegisteredPartitionTransform)
                .collect(toImmutableList());
        if (!partitionTransforms.isEmpty() && (!(expression instanceof FunctionCall) || partitionTransforms.size() != 1 || !partitionTransforms.get(0).equals(expression))) {
            throw new SemanticException(
                    NOT_SUPPORTED,
                    expression,
                    "Partition transform producing column %s must be selected directly, not wrapped in another expression.",
                    viewColumn);
        }
    }

    private boolean isRegisteredPartitionTransform(FunctionCall functionCall)
    {
        return getPartitionTransformProvider(functionCall, getFunctionMetadata(functionCall)).isPresent();
    }

    private FunctionMetadata getFunctionMetadata(FunctionCall functionCall)
    {
        return metadata.getFunctionAndTypeManager()
                .getFunctionAndTypeResolver()
                .getFunctionMetadata(analysis.getFunctionHandle(functionCall));
    }

    Optional<MaterializedViewPartitionTransformProvider> getPartitionTransformProvider(
            FunctionCall functionCall,
            FunctionMetadata function)
    {
        try {
            return metadata.getFunctionAndTypeManager()
                    .getMaterializedViewPartitionTransformRegistry()
                    .getProvider(function);
        }
        catch (IllegalArgumentException e) {
            throw new SemanticException(NOT_SUPPORTED, functionCall, e.getMessage());
        }
    }

    private Optional<String> getBuiltInFunctionName(FunctionCall functionCall)
    {
        return getCanonicalBuiltInFunctionName(analysis.getFunctionHandle(functionCall));
    }

    static Optional<String> getCanonicalBuiltInFunctionName(FunctionHandle functionHandle)
    {
        if (!(functionHandle instanceof BuiltInFunctionHandle)) {
            return Optional.empty();
        }
        return Optional.of(((BuiltInFunctionHandle) functionHandle)
                .getSignature()
                .getName()
                .getObjectName()
                .toLowerCase(ENGLISH));
    }

    private TransformationSubtype determineDirectSubtype(Expression expression)
    {
        if (!extractAggregateFunctions(analysis.getFunctionHandles(), ImmutableList.of(expression), metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()).isEmpty()) {
            return TransformationSubtype.AGGREGATION;
        }
        if (expression instanceof Identifier || expression instanceof DereferenceExpression) {
            return TransformationSubtype.IDENTITY;
        }
        return TransformationSubtype.TRANSFORMATION;
    }

    @Override
    protected Void visitSetOperation(SetOperation node, MaterializedViewPlanValidatorContext context)
    {
        super.visitSetOperation(node, context);

        List<RelationType> outputDescriptorList = node.getRelations().stream()
                .map(relation -> analysis.getOutputDescriptor(relation).withOnlyVisibleFields())
                .collect(toImmutableList());

        int numRelations = outputDescriptorList.size();
        int numFields = outputDescriptorList.get(0).getVisibleFieldCount();
        for (int fieldIndex = 0; fieldIndex < numFields; fieldIndex++) {
            for (int firstRelationIndex = 1; firstRelationIndex < numRelations; firstRelationIndex++) {
                Optional<TableColumn> firstBaseColumn = tryGetOriginalTableColumn(outputDescriptorList.get(firstRelationIndex).getFieldByIndex(fieldIndex));
                Optional<TableColumn> secondBaseColumn = tryGetOriginalTableColumn(outputDescriptorList.get(firstRelationIndex - 1).getFieldByIndex(fieldIndex));
                if (firstBaseColumn.isPresent() && secondBaseColumn.isPresent()) {
                    mappedBaseColumns.computeIfAbsent(firstBaseColumn.get(), k -> new HashSet<>()).add(secondBaseColumn.get());
                    mappedBaseColumns.computeIfAbsent(secondBaseColumn.get(), k -> new HashSet<>()).add(firstBaseColumn.get());

                    directMappedBaseColumns.computeIfAbsent(firstBaseColumn.get(), k -> new HashSet<>()).add(secondBaseColumn.get());
                    directMappedBaseColumns.computeIfAbsent(secondBaseColumn.get(), k -> new HashSet<>()).add(firstBaseColumn.get());
                }
            }
        }

        return null;
    }

    @Override
    protected Void visitTable(Table node, MaterializedViewPlanValidatorContext context)
    {
        super.visitTable(node, context);

        if (context.isWithinOuterJoin()) {
            baseTablesOnOuterJoinSide.add(toSchemaTableName(createQualifiedObjectName(session, node, node.getName(), metadata)));
        }

        return null;
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, MaterializedViewPlanValidatorContext context)
    {
        super.visitComparisonExpression(node, context);

        if (!context.isWithinJoinOn()) {
            return null;
        }

        // We assume only EQ comparisons, which is verified by MaterializedViewPlanValidator

        Field left = analysis.getScope(context.getTopJoinNode()).tryResolveField(node.getLeft())
                .orElseThrow(() -> new SemanticException(
                        NOT_SUPPORTED,
                        node.getLeft(),
                        "%s in join criteria is not supported for materialized view.", node.getLeft().getClass().getSimpleName()))
                .getField();
        Field right = analysis.getScope(context.getTopJoinNode()).tryResolveField(node.getRight())
                .orElseThrow(() -> new SemanticException(
                        NOT_SUPPORTED,
                        node.getRight(),
                        "%s in join criteria is not supported for materialized view.", node.getRight().getClass().getSimpleName()))
                .getField();

        Optional<TableColumn> leftBaseColumn = tryGetOriginalTableColumn(left);
        Optional<TableColumn> rightBaseColumn = tryGetOriginalTableColumn(right);
        if (leftBaseColumn.isPresent() && rightBaseColumn.isPresent()) {
            mappedBaseColumns.computeIfAbsent(leftBaseColumn.get(), k -> new HashSet<>()).add(rightBaseColumn.get());
            mappedBaseColumns.computeIfAbsent(rightBaseColumn.get(), k -> new HashSet<>()).add(leftBaseColumn.get());

            if (context.getTopJoinNode().getType().equals(INNER)) {
                directMappedBaseColumns.computeIfAbsent(leftBaseColumn.get(), k -> new HashSet<>()).add(rightBaseColumn.get());
                directMappedBaseColumns.computeIfAbsent(rightBaseColumn.get(), k -> new HashSet<>()).add(leftBaseColumn.get());
            }
        }

        return null;
    }

    private static Map<String, TableColumn> getOriginalColumnsFromAnalysis(Analysis analysis)
    {
        Query viewQuery = ((CreateMaterializedView) analysis.getStatement()).getQuery();

        return analysis.getOutputDescriptor(viewQuery).getVisibleFields().stream()
                .filter(field -> field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent())
                .collect(toImmutableMap(
                        field -> field.getName().get(),
                        field -> new TableColumn(toSchemaTableName(field.getOriginTable().get()), field.getOriginColumnName().get(), true)));
    }

    private static Map<String, Map<SchemaTableName, String>> getColumnMappings(Analysis analysis, Map<TableColumn, Set<TableColumn>> columnMappings)
    {
        checkState(
                analysis.getStatement() instanceof CreateMaterializedView,
                "Only support the computation of column mappings when analyzing CreateMaterializedView");

        ImmutableMap.Builder<String, Map<SchemaTableName, String>> fullColumnMappings = ImmutableMap.builder();

        Map<String, TableColumn> originalColumnMappings = getOriginalColumnsFromAnalysis(analysis);

        for (Map.Entry<String, TableColumn> columnMapping : originalColumnMappings.entrySet()) {
            String viewColumn = columnMapping.getKey();
            TableColumn originalBaseColumn = columnMapping.getValue();

            Map<SchemaTableName, String> fullBaseColumns =
                    columnMappings.getOrDefault(originalBaseColumn, ImmutableSet.of(originalBaseColumn))
                            .stream()
                            .collect(toImmutableMap(e -> e.getTableName(), e -> e.getColumnName()));

            fullColumnMappings.put(viewColumn, fullBaseColumns);
        }

        return fullColumnMappings.build();
    }

    private static Optional<TableColumn> tryGetOriginalTableColumn(Field field)
    {
        if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
            SchemaTableName table = toSchemaTableName(field.getOriginTable().get());
            String column = field.getOriginColumnName().get();
            return Optional.of(new TableColumn(table, column, true));
        }
        return Optional.empty();
    }
}
