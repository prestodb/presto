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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.IcebergDistributedProcedureHandle;
import com.facebook.presto.iceberg.IcebergProcedureContext;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.spi.ConnectorDistributedProcedureHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorProcedureContext;
import com.facebook.presto.spi.procedure.DistributedProcedure;
import com.facebook.presto.spi.procedure.DistributedProcedure.Argument;
import com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.SCHEMA;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.TABLE_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MigrateTableBucketProcedure
        implements Provider<DistributedProcedure>
{
    private final TypeManager typeManager;

    @Inject
    public MigrateTableBucketProcedure(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public DistributedProcedure get()
    {
        return new TableDataRewriteDistributedProcedure(
                "system",
                "migrate_table_bucket",
                ImmutableList.of(
                        new Argument(SCHEMA, VARCHAR),
                        new Argument(TABLE_NAME, VARCHAR),
                        new Argument("new_base_path", VARCHAR)),
                this::beginCallDistributedProcedure,
                this::finishCallDistributedProcedure,
                arguments -> {
                    checkArgument(arguments.length == 2,
                            format(
                                    "invalid number of arguments: %s (should have %s)",
                                    arguments.length,
                                    2));
                    checkArgument(
                            arguments[0] instanceof Table && arguments[1] instanceof Transaction,
                            "Invalid arguments, required: [Table, Transaction]");

                    Table table = (Table) arguments[0];
                    Transaction transaction = (Transaction) arguments[1];

                    return new IcebergProcedureContext(table, transaction);
                });
    }

    private ConnectorDistributedProcedureHandle beginCallDistributedProcedure(
            ConnectorSession session,
            ConnectorProcedureContext procedureContext,
            ConnectorTableLayoutHandle tableLayoutHandle,
            Object[] arguments,
            OptionalInt sortOrderIndex)
    {
        IcebergProcedureContext icebergContext = (IcebergProcedureContext) procedureContext;
        IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) tableLayoutHandle;
        String newBasePath = (String) arguments[2];
        Map<String, String> relevantData = ImmutableMap.of("new_base_path", newBasePath);
        IcebergTableHandle tableHandle = layoutHandle.getTable();
        Table icebergTable = icebergContext.getTable();

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
                ImmutableList.of(),
                relevantData); }

    private void finishCallDistributedProcedure(
            ConnectorSession session,
            ConnectorProcedureContext procedureContext,
            ConnectorDistributedProcedureHandle handle,
            Collection<Slice> fragments)
    {
        IcebergProcedureContext icebergContext = (IcebergProcedureContext) procedureContext;
        IcebergDistributedProcedureHandle icebergHandle = (IcebergDistributedProcedureHandle) handle;
        Table icebergTable = icebergContext.getTransaction().table();
        String newBasePath = icebergHandle.getRelevantData().get("new_base_path");
        Set<DataFile> existingDataFiles = new HashSet<>();
        TableScan tableScan = icebergTable.newScan().useSnapshot(icebergTable.currentSnapshot().snapshotId());

        try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
            for (FileScanTask task : tasks) {
                existingDataFiles.add(task.file());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Set<DeleteFile> deleteFiles = new HashSet<>();
        TableScan deleteScan = icebergTable.newScan().useSnapshot(icebergTable.currentSnapshot().snapshotId());

        try (CloseableIterable<FileScanTask> tasks = deleteScan.planFiles()) {
            for (FileScanTask task : tasks) {
                if (!task.deletes().isEmpty()) {
                    deleteFiles.addAll(task.deletes());
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Set<DataFile> newDataFiles = new HashSet<>();

        FileIO fileIO = icebergTable.io();

        for (DataFile oldFile : existingDataFiles) {
            String oldPath = oldFile.path().toString();
            String fileName = oldPath.substring(oldPath.lastIndexOf('/') + 1);
            String newPath = newBasePath + "/" + fileName;

            InputFile inputFile = fileIO.newInputFile(oldPath);
            OutputFile outputFile = fileIO.newOutputFile(newPath);

            try (SeekableInputStream in = inputFile.newStream();
                    PositionOutputStream out = outputFile.create()) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec()).copy(oldFile).withPath(newPath);

            if (oldFile.partition() != null) {
                builder.withPartition(oldFile.partition());
            }

            DataFile newFile = builder.build();

            newDataFiles.add(newFile);
        }
        RewriteFiles rewrite = icebergContext.getTransaction().newRewrite().rewriteFiles(existingDataFiles, deleteFiles, newDataFiles, ImmutableSet.of());

        Snapshot snapshot = icebergTable.currentSnapshot();
        if (snapshot != null) {
            rewrite.validateFromSnapshot(snapshot.snapshotId());
        }

        rewrite.commit();
    }
}
