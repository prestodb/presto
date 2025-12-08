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
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergDistributedProcedureHandle;
import com.facebook.presto.iceberg.IcebergProcedureContext;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.iceberg.RuntimeStatsMetricsReporter;
import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.facebook.presto.spi.ConnectorSession;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.SCHEMA;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.TABLE_NAME;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
                        new Argument("options", "map(varchar, varchar)", false, null)),
                (session, procedureContext, tableLayoutHandle, arguments) -> beginCallDistributedProcedure(session, (IcebergProcedureContext) procedureContext, (IcebergTableLayoutHandle) tableLayoutHandle, arguments),
                ((session, procedureContext, tableHandle, fragments) -> finishCallDistributedProcedure(session, (IcebergProcedureContext) procedureContext, tableHandle, fragments)),
                arguments -> {
                    checkArgument(arguments.length == 2, "invalid arguments count: " + arguments.length);
                    checkArgument(arguments[0] instanceof Table && arguments[1] instanceof Transaction, "Invalid arguments, required: [Table, Transaction]");
                    return new IcebergProcedureContext((Table) arguments[0], (Transaction) arguments[1]);
                });
    }

    private ConnectorDistributedProcedureHandle beginCallDistributedProcedure(ConnectorSession session, IcebergProcedureContext procedureContext, IcebergTableLayoutHandle layoutHandle, Object[] arguments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            Table icebergTable = procedureContext.getTable();
            IcebergTableHandle tableHandle = layoutHandle.getTable();

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
                    ImmutableMap.of());
        }
    }

    private void finishCallDistributedProcedure(ConnectorSession session, IcebergProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            IcebergDistributedProcedureHandle handle = (IcebergDistributedProcedureHandle) procedureHandle;
            Table icebergTable = procedureContext.getTransaction().table();

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

                Consumer<FileScanTask> fileScanTaskConsumer = (task) -> {
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
                };

                TableScan tableScan = procedureContext.getTable().newScan()
                        .metricsReporter(new RuntimeStatsMetricsReporter(session.getRuntimeStats()))
                        .filter(toIcebergExpression(predicate))
                        .useSnapshot(tableHandle.getIcebergTableName().getSnapshotId().get());
                CloseableIterable<FileScanTask> fileScanTaskIterable = tableScan.planFiles();
                CloseableIterator<FileScanTask> fileScanTaskIterator = fileScanTaskIterable.iterator();
                fileScanTaskIterator.forEachRemaining(fileScanTaskConsumer);
                try {
                    fileScanTaskIterable.close();
                    fileScanTaskIterator.close();
                    // TODO: remove this after org.apache.iceberg.io.CloseableIterator'withClose
                    //  correct release resources holds by iterator.
                    fileScanTaskIterator = CloseableIterator.empty();
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

            RewriteFiles rewriteFiles = procedureContext.getTransaction().newRewrite()
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
