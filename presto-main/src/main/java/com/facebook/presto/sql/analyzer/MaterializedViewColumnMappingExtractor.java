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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.SetOperation;
import com.facebook.presto.sql.tree.Table;
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
import static com.facebook.presto.spi.ConnectorMaterializedViewDefinition.TableColumn;
import static com.facebook.presto.sql.MaterializedViewUtils.transitiveClosure;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.tree.Join.Type.INNER;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class MaterializedViewColumnMappingExtractor
        extends MaterializedViewPlanValidator
{
    private final Analysis analysis;
    private final Session session;

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

    public MaterializedViewColumnMappingExtractor(Analysis analysis, Session session)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.session = requireNonNull(session, "session is null");
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
            baseTablesOnOuterJoinSide.add(toSchemaTableName(createQualifiedObjectName(session, node, node.getName())));
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
