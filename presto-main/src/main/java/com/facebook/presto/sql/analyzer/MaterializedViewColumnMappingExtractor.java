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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.SetOperation;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.toSchemaTableName;
import static com.facebook.presto.spi.ConnectorMaterializedViewDefinition.TableColumn;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MaterializedViewColumnMappingExtractor
        extends MaterializedViewPlanValidator
{
    private final Analysis analysis;

    private ImmutableList.Builder<List<TableColumn>> mappedBaseColumns;

    private MaterializedViewColumnMappingExtractor(Analysis analysis)
    {
        this.analysis = requireNonNull(analysis, "analysis is null");

        this.mappedBaseColumns = ImmutableList.builder();
    }

    /**
     * Call MaterializedViewColumnMappingExtractor to traverse the materialized view query,
     * and return possible pairs of the base table columns that are mapped by join criteria,
     * union, intersect, etc.
     * <p>
     * For example, given SELECT column_a AS column_x FROM table_a JOIN table_b ON (table_a.column_a = table_b.column_b),
     * one possible pair of mapped base table columns is (table_a.column_a, table_b.column_b).
     */
    public static List<List<TableColumn>> extractMappedBaseColumns(Analysis analysis)
    {
        checkState(
                analysis.getStatement() instanceof CreateMaterializedView,
                "Only support extracting of mapped columns from create materialized view query");

        MaterializedViewColumnMappingExtractor extractor = new MaterializedViewColumnMappingExtractor(analysis);
        Query viewQuery = ((CreateMaterializedView) analysis.getStatement()).getQuery();
        extractor.process(viewQuery, new MaterializedViewPlanValidatorContext());

        return extractor.getMappedBaseColumns();
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
            for (int firstRelationIndex = 0; firstRelationIndex < numRelations; firstRelationIndex++) {
                Optional<TableColumn> firstBaseColumn = tryGetOriginalTableColumn(outputDescriptorList.get(firstRelationIndex).getFieldByIndex(fieldIndex));
                for (int secondRelationIndex = firstRelationIndex + 1; secondRelationIndex < numRelations; secondRelationIndex++) {
                    Optional<TableColumn> secondBaseColumn = tryGetOriginalTableColumn(outputDescriptorList.get(secondRelationIndex).getFieldByIndex(fieldIndex));
                    if (firstBaseColumn.isPresent() && secondBaseColumn.isPresent() && !firstBaseColumn.get().equals(secondBaseColumn.get())) {
                        mappedBaseColumns.add(ImmutableList.of(firstBaseColumn.get(), secondBaseColumn.get()));
                    }
                }
            }
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
        if (leftBaseColumn.isPresent() && rightBaseColumn.isPresent() && !leftBaseColumn.get().equals(rightBaseColumn.get())) {
            mappedBaseColumns.add(ImmutableList.of(leftBaseColumn.get(), rightBaseColumn.get()));
        }

        return null;
    }

    private static Optional<TableColumn> tryGetOriginalTableColumn(Field field)
    {
        if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
            SchemaTableName table = toSchemaTableName(field.getOriginTable().get());
            String column = field.getOriginColumnName().get();
            return Optional.of(new TableColumn(table, column));
        }
        return Optional.empty();
    }

    private List<List<TableColumn>> getMappedBaseColumns()
    {
        return mappedBaseColumns.build();
    }
}
