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
package com.facebook.presto.iceberg.procedure;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.CommitTaskData;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergDistributedProcedureHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.iceberg.RuntimeStatsMetricsReporter;
import com.facebook.presto.iceberg.SortField;
import com.facebook.presto.iceberg.procedure.context.IcebergRewriteDataFilesProcedureContext;
import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.DistributedProcedure;
import com.facebook.presto.spi.procedure.DistributedProcedure.Argument;
import com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergAbstractMetadata.getSupportedSortFields;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergUtil.filterByFile;
import static com.facebook.presto.iceberg.IcebergUtil.filterByGroup;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.IcebergUtil.parseMaxFileSize;
import static com.facebook.presto.iceberg.IcebergUtil.parseMinFileSize;
import static com.facebook.presto.iceberg.IcebergUtil.parseMinInputFiles;
import static com.facebook.presto.iceberg.IcebergUtil.parseRewriteAll;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;
import static com.facebook.presto.iceberg.SortFieldUtils.parseSortFields;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.SCHEMA;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.TABLE_NAME;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RewriteDataFilesProcedure
        implements Provider<DistributedProcedure>
{
    TypeManager typeManager;
    JsonCodec<CommitTaskData> commitTaskCodec;

    @Inject
    public RewriteDataFilesProcedure(
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
    }

    @Override
    public DistributedProcedure get()
    {
        return new TableDataRewriteDistributedProcedure(
                "system",
                "rewrite_data_files",
                ImmutableList.of(
                        new Argument(SCHEMA, VARCHAR),
                        new Argument(TABLE_NAME, VARCHAR),
                        new Argument("filter", VARCHAR, false, "TRUE"),
                        new Argument("sorted_by", "array(varchar)", false, null),
                        new Argument("options", "map(varchar, varchar)", false, null)),
                (session, procedureContext, tableLayoutHandle, arguments, sortOrderIndex) -> beginCallDistributedProcedure(session, (IcebergRewriteDataFilesProcedureContext) procedureContext, (IcebergTableLayoutHandle) tableLayoutHandle, arguments, sortOrderIndex),
                ((session, procedureContext, tableHandle, fragments) -> finishCallDistributedProcedure(session, (IcebergRewriteDataFilesProcedureContext) procedureContext, tableHandle, fragments)),
                arguments -> {
                    // Context provider receives [Table, Transaction, procedureArguments]
                    checkArgument(arguments.length >= 2, format("invalid number of arguments: %s (should have at least %s)", arguments.length, 2));
                    checkArgument(arguments[0] instanceof Table && arguments[1] instanceof IcebergAbstractMetadata, "Invalid arguments, required: [Table, IcebergAbstractMetadata]");

                    // Extract and validate options from procedure arguments if present
                    Map<String, String> options = extractAndValidateOptions(arguments);

                    return new IcebergRewriteDataFilesProcedureContext((Table) arguments[0], (IcebergAbstractMetadata) arguments[1], options);
                });
    }

    private static Map<String, String> extractAndValidateOptions(Object[] contextProviderArgs)
    {
        Map<String, String> options = ImmutableMap.of();
        if (contextProviderArgs.length > 2 && contextProviderArgs[2] instanceof Object[]) {
            Object[] procedureArgs = (Object[]) contextProviderArgs[2];
            // Options is the 5th procedure parameter (index 4)
            if (procedureArgs.length > 4 && procedureArgs[4] != null && procedureArgs[4] instanceof Map) {
                options = (Map<String, String>) procedureArgs[4];

                // Validate options if present using utility methods
                parseMinInputFiles(options);
                parseMinFileSize(options);
                parseMaxFileSize(options);
                parseRewriteAll(options);
            }
        }
        return options;
    }

    private ConnectorDistributedProcedureHandle beginCallDistributedProcedure(ConnectorSession session, IcebergRewriteDataFilesProcedureContext procedureContext, IcebergTableLayoutHandle layoutHandle, Object[] arguments, OptionalInt sortOrderIndex)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            Table icebergTable = procedureContext.getTable();
            IcebergTableHandle tableHandle = layoutHandle.getTable();

            SortOrder sortOrder = icebergTable.sortOrder();
            List<String> sortFieldStrings = ImmutableList.of();
            if (sortOrderIndex.isPresent()) {
                Object value = arguments[sortOrderIndex.getAsInt()];
                if (value == null) {
                    sortFieldStrings = ImmutableList.of();
                }
                else if (value instanceof List<?>) {
                    sortFieldStrings = ((List<?>) value).stream()
                            .map(String.class::cast)
                            .collect(toImmutableList());
                }
                else {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "sorted_by must be an array(varchar)");
                }
            }
            if (sortFieldStrings != null && !sortFieldStrings.isEmpty()) {
                SortOrder specifiedSortOrder = parseSortFields(icebergTable.schema(), sortFieldStrings);
                if (specifiedSortOrder.satisfies(sortOrder)) {
                    // If the specified sort order satisfies the target table's internal sort order, use the specified sort order
                    sortOrder = specifiedSortOrder;
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, "Specified sort order is incompatible with the target table's internal sort order");
                }
            }

            List<SortField> sortFields = getSupportedSortFields(icebergTable.schema(), sortOrder);
            return new IcebergDistributedProcedureHandle(
                    tableHandle.getSchemaName(),
                    tableHandle.getIcebergTableName(),
                    toPrestoSchema(icebergTable.schema(), typeManager),
                    toPrestoPartitionSpec(icebergTable.spec(), typeManager),
                    getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                    icebergTable.location(),
                    getFileFormat(icebergTable),
                    getCompressionCodec(session),
                    icebergTable.properties(),
                    layoutHandle,
                    sortFields,
                    ImmutableMap.of());
        }
    }

    private void finishCallDistributedProcedure(ConnectorSession session, IcebergRewriteDataFilesProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            IcebergDistributedProcedureHandle handle = (IcebergDistributedProcedureHandle) procedureHandle;
            Table icebergTable = procedureContext.getTable();

            List<CommitTaskData> commitTasks = fragments.stream()
                    .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                    .collect(toImmutableList());

            org.apache.iceberg.types.Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                    .map(field -> field.transform().getResultType(
                            icebergTable.schema().findType(field.sourceId())))
                    .toArray(Type[]::new);

            Set<DataFile> newFiles = new HashSet<>();
            for (CommitTaskData task : commitTasks) {
                DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                        .withPath(task.getPath())
                        .withFileSizeInBytes(task.getFileSizeInBytes())
                        .withFormat(handle.getFileFormat().name())
                        .withMetrics(task.getMetrics().metrics());

                if (!icebergTable.spec().fields().isEmpty()) {
                    String partitionDataJson = task.getPartitionDataJson()
                            .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                    builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                }
                newFiles.add(builder.build());
            }

            IcebergTableLayoutHandle layoutHandle = handle.getTableLayoutHandle();
            IcebergTableHandle tableHandle = layoutHandle.getTable();
            final Set<DataFile> scannedDataFiles = new HashSet<>();
            final Set<DeleteFile> fullyAppliedDeleteFiles = new HashSet<>();
            if (tableHandle.getIcebergTableName().getSnapshotId().isPresent()) {
                TupleDomain<IcebergColumnHandle> predicate = layoutHandle.getValidPredicate();

                TableScan tableScan = procedureContext.getTable().newScan()
                        .metricsReporter(new RuntimeStatsMetricsReporter(session.getRuntimeStats()))
                        .filter(toIcebergExpression(predicate))
                        .useSnapshot(tableHandle.getIcebergTableName().getSnapshotId().get());

                Map<String, String> options = procedureContext.getOptions();
                // Apply filtering using options
                try (CloseableIterable<FileScanTask> fileScanTaskIterable = filterByGroup(filterByFile(tableScan.planFiles(), options), options)) {
                    // Collect files and delete files from filtered tasks
                    for (FileScanTask task : fileScanTaskIterable) {
                        scannedDataFiles.add(task.file());
                        if (!task.deletes().isEmpty()) {
                            task.deletes().forEach(deleteFile -> {
                                if (deleteFile.content() == FileContent.EQUALITY_DELETES &&
                                        !icebergTable.specs().get(deleteFile.specId()).isPartitioned() &&
                                        !predicate.isAll()) {
                                    // Equality files with an unpartitioned spec are applied as global deletes
                                    //  So they should not be cleaned up unless the whole table is optimized
                                    return;
                                }
                                fullyAppliedDeleteFiles.add(deleteFile);
                            });
                        }
                    }
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            if (fragments.isEmpty() &&
                    scannedDataFiles.isEmpty() &&
                    fullyAppliedDeleteFiles.isEmpty()) {
                return;
            }

            RewriteFiles rewriteFiles = icebergTable.newRewrite()
                    .rewriteFiles(scannedDataFiles, fullyAppliedDeleteFiles, newFiles, ImmutableSet.of());

            // Table.snapshot method returns null if there is no matching snapshot
            Snapshot snapshot = requireNonNull(
                    handle.getTableName()
                            .getSnapshotId()
                            .map(icebergTable::snapshot)
                            .orElse(null),
                    "snapshot is null");
            if (icebergTable.currentSnapshot() != null) {
                rewriteFiles.validateFromSnapshot(snapshot.snapshotId());
            }
            rewriteFiles.commit();
        }
    }
}
