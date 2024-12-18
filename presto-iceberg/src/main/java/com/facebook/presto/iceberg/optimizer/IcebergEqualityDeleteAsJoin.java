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
package com.facebook.presto.iceberg.optimizer;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.ColumnIdentity;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergMetadataColumn;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergTableName;
import com.facebook.presto.iceberg.IcebergTableType;
import com.facebook.presto.iceberg.IcebergTransactionManager;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Sets;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.iceberg.FileContent.EQUALITY_DELETES;
import static com.facebook.presto.iceberg.FileContent.fromIcebergFileContent;
import static com.facebook.presto.iceberg.IcebergColumnHandle.DATA_SEQUENCE_NUMBER_COLUMN_HANDLE;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.DATA_SEQUENCE_NUMBER;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isDeleteToJoinPushdownEnabled;
import static com.facebook.presto.iceberg.IcebergUtil.getDeleteFiles;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

/**
 * <p>This optimizer implements equality deletes as a join, rather than having each split read the delete files and apply them.
 * This approach significantly enhances performance for equality deletes, as most delete files will apply to most splits,
 * and opening the delete file in each split incurs considerable overhead. Usually, the delete files are relatively small
 * and can be broadcast easily. Each delete file may have a different schema, though typically there will be only a few delete
 * schemas, often just one (the primary key).</p>
 *
 * <p>For example, consider the following query:
 * <code>SELECT * FROM table;</code>
 * With 2 delete schemas: (pk), (orderid), the query will be transformed into:
 * <pre>
 * SELECT "$data_sequence_number", * FROM table
 * LEFT JOIN "table$equality_deletes1" d1 ON left.pk = d1.pk AND left."$data_sequence_number" < d1."$data_sequence_number" -- Find deletes by schema 1
 * LEFT JOIN "table$equality_deletes2" d2 ON left.orderid = d1.orderid AND left."$data_sequence_number" < d2."$data_sequence_number" -- Find deletes by schema 2
 * WHERE COALESCE(d1."$data_sequence_number", d2."data_sequence_number") IS NULL -- None of the delete files had a delete for this row
 * </pre>
 * Note that table$equality_deletes1 and table$equality_deletes2 are different tables, each containing only the delete files with the schema for this join.</p>
 */

public class IcebergEqualityDeleteAsJoin
        implements ConnectorPlanOptimizer
{
    private final StandardFunctionResolution functionResolution;
    private final IcebergTransactionManager transactionManager;
    private final TypeManager typeManager;

    IcebergEqualityDeleteAsJoin(StandardFunctionResolution functionResolution,
            IcebergTransactionManager transactionManager,
            TypeManager typeManager)
    {
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!isDeleteToJoinPushdownEnabled(session)) {
            return maxSubplan;
        }
        return rewriteWith(new DeleteAsJoinRewriter(functionResolution,
                transactionManager, idAllocator, session, typeManager, variableAllocator), maxSubplan);
    }

    private static class DeleteAsJoinRewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final StandardFunctionResolution functionResolution;
        private final PlanNodeIdAllocator idAllocator;
        private final IcebergTransactionManager transactionManager;
        private final TypeManager typeManager;
        private final VariableAllocator variableAllocator;

        public DeleteAsJoinRewriter(
                StandardFunctionResolution functionResolution,
                IcebergTransactionManager transactionManager,
                PlanNodeIdAllocator idAllocator,
                ConnectorSession session,
                TypeManager typeManager,
                VariableAllocator variableAllocator)
        {
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.session = requireNonNull(session, "session is null");
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            TableHandle table = node.getTable();
            IcebergTableHandle icebergTableHandle = (IcebergTableHandle) table.getConnectorHandle();
            Optional<IcebergTableLayoutHandle> icebergTableLayoutHandle = table.getLayout().map(IcebergTableLayoutHandle.class::cast);
            IcebergTableName tableName = icebergTableHandle.getIcebergTableName();
            if (!tableName.getSnapshotId().isPresent() || tableName.getTableType() != IcebergTableType.DATA) {
                // Node is already optimized or not ready for planning
                return node;
            }

            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(table.getTransaction());
            Table icebergTable = getIcebergTable(metadata, session, icebergTableHandle.getSchemaTableName());

            TupleDomain<IcebergColumnHandle> predicate = icebergTableLayoutHandle
                    .map(IcebergTableLayoutHandle::getValidPredicate)
                    .map(IcebergUtil::getNonMetadataColumnConstraints)
                    .orElseGet(TupleDomain::all);

            // Collect info about each unique delete schema to join by
            ImmutableMap<Set<Integer>, DeleteSetInfo> deleteSchemas = collectDeleteInformation(icebergTable, predicate, tableName.getSnapshotId().get());

            if (deleteSchemas.isEmpty()) {
                // no equality deletes
                return node;
            }

            // Add all the fields required by the join that were not added by the user's query
            ImmutableMap<VariableReferenceExpression, ColumnHandle> unselectedAssignments = createAssignmentsForUnselectedFields(node, deleteSchemas, icebergTable);
            TableScanNode updatedTableScan = createNewRoot(node, icebergTableHandle, tableName, unselectedAssignments, table);

            Map<Integer, VariableReferenceExpression> reverseAssignmentsMap = updatedTableScan
                    .getAssignments()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(assignment -> ((IcebergColumnHandle) (assignment.getValue())).getId(), Map.Entry::getKey));

            List<RowExpression> deleteVersionColumns = new ArrayList<>();
            PlanNode parentNode = updatedTableScan;
            // For each unique delete schema add a join that applies those equality deletes
            for (Map.Entry<Set<Integer>, DeleteSetInfo> entry : deleteSchemas.entrySet()) {
                DeleteSetInfo deleteGroupInfo = entry.getValue();

                List<Types.NestedField> deleteFields = deleteGroupInfo
                        .equalityFieldIds
                        .stream()
                        .map(fieldId -> icebergTable.schema().findField(fieldId))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                VariableReferenceExpression joinSequenceNumber = toVariableReference(DATA_SEQUENCE_NUMBER_COLUMN_HANDLE);
                deleteVersionColumns.add(joinSequenceNumber);
                ImmutableMap<VariableReferenceExpression, ColumnHandle> deleteColumnAssignments = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                        .putAll(deleteGroupInfo.allFields(icebergTable.schema()).stream().collect(Collectors.toMap(this::toVariableReference, this::toIcebergColumnHandle)))
                        .put(joinSequenceNumber, DATA_SEQUENCE_NUMBER_COLUMN_HANDLE)
                        .build();

                // ON source.delete_column = deletes.delete_column, ...
                Set<EquiJoinClause> clauses = deleteColumnAssignments
                        .entrySet()
                        .stream()
                        .filter(assignment -> !IcebergMetadataColumn.isMetadataColumnId(((IcebergColumnHandle) (assignment.getValue())).getId()))
                        .map(assignment -> {
                            VariableReferenceExpression left = reverseAssignmentsMap.get(((IcebergColumnHandle) (assignment.getValue())).getId());
                            VariableReferenceExpression right = assignment.getKey();
                            return new EquiJoinClause(left, right);
                        }).collect(Collectors.toSet());

                FunctionHandle lessThan = functionResolution.comparisonFunction(OperatorType.LESS_THAN, BigintType.BIGINT, BigintType.BIGINT);

                // AND source.$data_sequence_number < deletes.$data_sequence_number
                RowExpression versionFilter = new CallExpression(lessThan.getName(),
                        lessThan,
                        BooleanType.BOOLEAN,
                        Collections.unmodifiableList(Arrays.asList(reverseAssignmentsMap.get(DATA_SEQUENCE_NUMBER.getId()), joinSequenceNumber)));

                TableScanNode deleteTableScan = createDeletesTableScan(deleteColumnAssignments,
                        icebergTableHandle,
                        tableName,
                        deleteFields,
                        table,
                        deleteGroupInfo);

                parentNode = new JoinNode(
                        Optional.empty(),
                        idAllocator.getNextId(),
                        JoinType.LEFT,
                        parentNode,
                        deleteTableScan,
                        ImmutableList.copyOf(clauses),
                        Stream.concat(parentNode.getOutputVariables().stream(), deleteTableScan.getOutputVariables().stream()).collect(Collectors.toList()),
                        Optional.of(versionFilter),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(), // Allow stats to determine join distribution,
                        ImmutableMap.of());
            }

            FilterNode filter = new FilterNode(Optional.empty(), idAllocator.getNextId(), Optional.empty(), parentNode,
                    new SpecialFormExpression(SpecialFormExpression.Form.IS_NULL, BooleanType.BOOLEAN,
                            new SpecialFormExpression(SpecialFormExpression.Form.COALESCE, BigintType.BIGINT, deleteVersionColumns)));

            Assignments.Builder assignmentsBuilder = Assignments.builder();
            filter.getOutputVariables().stream()
                    .filter(variableReferenceExpression -> !variableReferenceExpression.getName().startsWith(DATA_SEQUENCE_NUMBER_COLUMN_HANDLE.getName()))
                    .forEach(variableReferenceExpression -> assignmentsBuilder.put(variableReferenceExpression, variableReferenceExpression));
            return new ProjectNode(Optional.empty(), idAllocator.getNextId(), filter, assignmentsBuilder.build(), ProjectNode.Locality.LOCAL);
        }

        private static ImmutableMap<Set<Integer>, DeleteSetInfo> collectDeleteInformation(Table icebergTable,
                TupleDomain<IcebergColumnHandle> predicate,
                long snapshotId)
        {
            // Delete schemas can repeat, so using a normal hashmap to dedup, will be converted to immutable at the end of the function.
            HashMap<Set<Integer>, DeleteSetInfo> deleteInformations = new HashMap<>();
            try (CloseableIterator<DeleteFile> files =
                    getDeleteFiles(icebergTable, snapshotId, predicate, Optional.empty(), Optional.empty()).iterator()) {
                files.forEachRemaining(delete -> {
                    if (fromIcebergFileContent(delete.content()) == EQUALITY_DELETES) {
                        ImmutableMap.Builder<Integer, PartitionFieldInfo> partitionFieldsBuilder = new ImmutableMap.Builder<>();
                        ImmutableSet.Builder<Integer> identityPartitionFieldSourceIdsBuilder = new Builder<>();
                        PartitionSpec partitionSpec = icebergTable.specs().get(delete.specId());
                        Types.StructType partitionType = partitionSpec.partitionType();
                        // PartitionField ids are unique across the entire table in v2. We can assume we are in v2 since v1 doesn't have equality deletes
                        partitionSpec.fields().forEach(field -> {
                            if (field.transform().isIdentity()) {
                                identityPartitionFieldSourceIdsBuilder.add(field.sourceId());
                            }
                            partitionFieldsBuilder.put(field.fieldId(), new PartitionFieldInfo(partitionType.field(field.fieldId()), field));
                        });
                        ImmutableMap<Integer, PartitionFieldInfo> partitionFields = partitionFieldsBuilder.build();
                        ImmutableSet<Integer> identityPartitionFieldSourceIds = identityPartitionFieldSourceIdsBuilder.build();
                        HashSet<Integer> result = new HashSet<>();
                        result.addAll(partitionFields.keySet());

                        // Filter out identity partition columns from delete file's `equalityFieldIds` to support `delete-schema-merging` within the same partition spec.
                        List<Integer> equalityFieldIdsExcludeIdentityPartitionField = delete.equalityFieldIds().stream()
                                .filter(fieldId -> !identityPartitionFieldSourceIds.contains(fieldId))
                                .collect(Collectors.toList());
                        result.addAll(equalityFieldIdsExcludeIdentityPartitionField);
                        deleteInformations.put(ImmutableSet.copyOf(result), new DeleteSetInfo(partitionFields, equalityFieldIdsExcludeIdentityPartitionField));
                    }
                });
            }
            catch (IOException e) {
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Failed to read equality delete information", e);
            }
            return ImmutableMap.copyOf(deleteInformations);
        }

        private TableScanNode createDeletesTableScan(ImmutableMap<VariableReferenceExpression, ColumnHandle> deleteColumnAssignments,
                IcebergTableHandle icebergTableHandle,
                IcebergTableName tableName,
                List<Types.NestedField> deleteFields,
                TableHandle table,
                DeleteSetInfo deleteInfo)
        {
            List<VariableReferenceExpression> outputs = deleteColumnAssignments.keySet().asList();
            IcebergTableHandle deletesTableHandle = new IcebergTableHandle(icebergTableHandle.getSchemaName(),
                    new IcebergTableName(tableName.getTableName(),
                            IcebergTableType.EQUALITY_DELETES, // Read equality deletes instead of data
                            tableName.getSnapshotId(),
                            Optional.empty()),
                    icebergTableHandle.isSnapshotSpecified(),
                    icebergTableHandle.getOutputPath(),
                    icebergTableHandle.getStorageProperties(),
                    icebergTableHandle.getTableSchemaJson(),
                    Optional.of(deleteInfo.partitionFields.keySet()),                // Enforce reading only delete files that match this schema
                    Optional.ofNullable(deleteInfo.equalityFieldIds.isEmpty() ? null : deleteInfo.equalityFieldIds),
                    icebergTableHandle.getSortOrder(),
                    icebergTableHandle.getUpdatedColumns());

            return new TableScanNode(Optional.empty(),
                    idAllocator.getNextId(),
                    new TableHandle(table.getConnectorId(), deletesTableHandle, table.getTransaction(), table.getLayout(), table.getDynamicFilter()),
                    outputs,
                    deleteColumnAssignments,
                    TupleDomain.all(),
                    TupleDomain.all(),
                    Optional.empty());
        }

        /**
         * - Updates table handle to DATA_WITHOUT_EQUALITY_DELETES since the page source for this node should now not apply equality deletes.
         * - Adds extra assignments and outputs that are needed by the join
         */
        private TableScanNode createNewRoot(TableScanNode node, IcebergTableHandle icebergTableHandle, IcebergTableName tableName, ImmutableMap<VariableReferenceExpression, ColumnHandle> unselectedAssignments, TableHandle table)
        {
            IcebergTableHandle updatedHandle = new IcebergTableHandle(icebergTableHandle.getSchemaName(),
                    new IcebergTableName(tableName.getTableName(),
                            IcebergTableType.DATA_WITHOUT_EQUALITY_DELETES, // Don't apply equality deletes in the split
                            tableName.getSnapshotId(),
                            tableName.getChangelogEndSnapshot()),
                    icebergTableHandle.isSnapshotSpecified(),
                    icebergTableHandle.getOutputPath(),
                    icebergTableHandle.getStorageProperties(),
                    icebergTableHandle.getTableSchemaJson(),
                    icebergTableHandle.getPartitionSpecId(),
                    icebergTableHandle.getEqualityFieldIds(),
                    icebergTableHandle.getSortOrder(),
                    icebergTableHandle.getUpdatedColumns());

            VariableReferenceExpression dataSequenceNumberVariableReference = toVariableReference(DATA_SEQUENCE_NUMBER_COLUMN_HANDLE);
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignmentsBuilder = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                    .put(dataSequenceNumberVariableReference, DATA_SEQUENCE_NUMBER_COLUMN_HANDLE)
                    .putAll(unselectedAssignments)
                    .putAll(node.getAssignments());
            ImmutableList.Builder<VariableReferenceExpression> outputsBuilder = ImmutableList.builder();
            outputsBuilder.addAll(node.getOutputVariables());
            if (!node.getAssignments().containsKey(dataSequenceNumberVariableReference)) {
                outputsBuilder.add(dataSequenceNumberVariableReference);
            }
            outputsBuilder.addAll(unselectedAssignments.keySet());

            return new TableScanNode(node.getSourceLocation(),
                    node.getId(),
                    Optional.of(node),
                    new TableHandle(table.getConnectorId(), updatedHandle, table.getTransaction(), table.getLayout(), table.getDynamicFilter()),
                    outputsBuilder.build(),
                    assignmentsBuilder.build(),
                    node.getTableConstraints(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    node.getCteMaterializationInfo());
        }

        /**
         * Calculate required fields that the user didn't include in his select statement and add assignments for them to add to the table scan
         */
        private ImmutableMap<VariableReferenceExpression, ColumnHandle> createAssignmentsForUnselectedFields(TableScanNode node,
                ImmutableMap<Set<Integer>, DeleteSetInfo> deleteInfos,
                Table icebergTable)
        {
            Set<Integer> selectedFields = node.getAssignments().values().stream().map(f -> ((IcebergColumnHandle) f).getId()).collect(Collectors.toSet());
            Set<Integer> unselectedFields = Sets.difference(deleteInfos.keySet().stream().reduce(Sets::union).orElseGet(Collections::emptySet), selectedFields);
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> unselectedAssignmentsBuilder = ImmutableMap.builder();
            Map<Integer, PartitionFieldInfo> partitionFields = deleteInfos.values().stream()
                    .flatMap(info -> info.getPartitionFields().entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (existing, replacement) -> existing));
            unselectedFields
                    .forEach(fieldId -> {
                        if (partitionFields.containsKey(fieldId)) {
                            PartitionFieldInfo partitionFieldInfo = partitionFields.get(fieldId);
                            PartitionField partitionField = partitionFieldInfo.getPartitionField();
                            Types.NestedField sourceField = icebergTable.schema().findField(partitionField.sourceId());
                            if (!partitionField.transform().isIdentity()) {
                                Type partitionFieldType = partitionField.transform().getResultType(sourceField.type());
                                VariableReferenceExpression variableReference = variableAllocator.newVariable(partitionField.name(), toPrestoType(partitionFieldType, typeManager));
                                IcebergColumnHandle columnHandle = new IcebergColumnHandle(
                                        ColumnIdentity.createColumnIdentity(partitionField.name(), partitionField.fieldId(), partitionFieldType),
                                        toPrestoType(partitionFieldType, typeManager),
                                        Optional.empty(),
                                        PARTITION_KEY);
                                unselectedAssignmentsBuilder.put(variableReference, columnHandle);
                            }
                            else if (!selectedFields.contains(sourceField.fieldId())) {
                                unselectedAssignmentsBuilder.put(
                                        variableAllocator.newVariable(sourceField.name(), toPrestoType(sourceField.type(), typeManager)),
                                        IcebergColumnHandle.create(sourceField, typeManager, REGULAR));
                            }
                        }
                        else {
                            Types.NestedField schemaField = icebergTable.schema().findField(fieldId);
                            unselectedAssignmentsBuilder.put(
                                    variableAllocator.newVariable(schemaField.name(), toPrestoType(schemaField.type(), typeManager)),
                                    IcebergColumnHandle.create(schemaField, typeManager, REGULAR));
                        }
                    });
            return unselectedAssignmentsBuilder.build();
        }

        private VariableReferenceExpression toVariableReference(IcebergColumnHandle columnHandle)
        {
            return variableAllocator.newVariable(columnHandle.getName(), columnHandle.getType());
        }

        private IcebergColumnHandle toIcebergColumnHandle(Types.NestedField field)
        {
            ColumnIdentity columnIdentity = new ColumnIdentity(field.fieldId(), field.name(), ColumnIdentity.TypeCategory.PRIMITIVE, Collections.emptyList());
            return new IcebergColumnHandle(columnIdentity, toPrestoType(field.type(), typeManager), Optional.empty(), REGULAR);
        }

        private VariableReferenceExpression toVariableReference(Types.NestedField field)
        {
            return variableAllocator.newVariable(field.name(), toPrestoType(field.type(), typeManager));
        }

        private static class PartitionFieldInfo
        {
            private final Types.NestedField nestedField;
            private final PartitionField partitionField;

            private PartitionFieldInfo(Types.NestedField nestedField, PartitionField partitionField)
            {
                this.nestedField = nestedField;
                this.partitionField = partitionField;
            }

            public PartitionField getPartitionField()
            {
                return partitionField;
            }
        }

        private static class DeleteSetInfo
        {
            private final ImmutableMap<Integer, PartitionFieldInfo> partitionFields;
            private final Set<Integer> equalityFieldIds;

            private DeleteSetInfo(ImmutableMap<Integer, PartitionFieldInfo> partitionFields,
                    List<Integer> equalityFieldIds)
            {
                this.partitionFields = requireNonNull(partitionFields, "partitionFields is null");
                this.equalityFieldIds = ImmutableSet.copyOf(requireNonNull(equalityFieldIds, "equalityFieldIds is null"));
            }

            public ImmutableMap<Integer, PartitionFieldInfo> getPartitionFields()
            {
                return partitionFields;
            }

            public List<Types.NestedField> allFields(Schema schema)
            {
                return Stream.concat(equalityFieldIds
                                        .stream()
                                        .map(schema::findField),
                                partitionFields
                                        .values()
                                        .stream()
                                        .map(partitionFieldInfo -> {
                                            if (partitionFieldInfo.partitionField.transform().isIdentity()) {
                                                return schema.findField(partitionFieldInfo.partitionField.sourceId());
                                            }
                                            return partitionFieldInfo.nestedField;
                                        }))
                        .collect(Collectors.toList());
            }
        }
    }
}
