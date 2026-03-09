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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorProcedureContext;
import com.facebook.presto.spi.procedure.DistributedProcedure;
import com.facebook.presto.spi.procedure.DistributedProcedure.Argument;
import com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
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
import java.nio.charset.StandardCharsets;
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
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.SCHEMA;
import static com.facebook.presto.spi.procedure.TableDataRewriteDistributedProcedure.TABLE_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MigrateTableBucketProcedure
        implements Provider<DistributedProcedure>
{
    private static final String FRAGMENT_DELIMITER = "\u0000";

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
        IcebergTableHandle tableHandle = layoutHandle.getTable();
        Table icebergTable = icebergContext.getTable();
        Snapshot currentSnapshot = icebergTable.currentSnapshot();
        if (currentSnapshot != null) {
            TableScan deleteScan = icebergTable.newScan().useSnapshot(currentSnapshot.snapshotId());
            try (CloseableIterable<FileScanTask> tasks = deleteScan.planFiles()) {
                for (FileScanTask task : tasks) {
                    if (!task.deletes().isEmpty()) {
                        throw new PrestoException(
                                NOT_SUPPORTED,
                                "migrate_table_bucket does not support tables with delete files. " +
                                        "Please compact or merge delete files before migrating.");
                    }
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        String normalizedNewBasePath = stripTrailingSlash(newBasePath);

        Map<String, String> relevantData = ImmutableMap.of(
                "new_base_path", normalizedNewBasePath,
                "table_location", stripTrailingSlash(icebergTable.location()));

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
                relevantData);
    }

    public static Slice copyFileAndBuildFragment(
            FileIO fileIO,
            String tableLocation,
            String normalizedNewBasePath,
            DataFile dataFile)
    {
        String oldPath = dataFile.path().toString();
        String normalizedTableLocation = stripTrailingSlash(tableLocation);
        String relativePath = oldPath.startsWith(normalizedTableLocation)
                ? oldPath.substring(normalizedTableLocation.length() + 1)
                : oldPath.substring(oldPath.lastIndexOf('/') + 1);
        String newPath = normalizedNewBasePath + "/" + relativePath;

        if (newPath.equals(oldPath)) {
            String payload = oldPath + FRAGMENT_DELIMITER + oldPath;
            return Slices.wrappedBuffer(payload.getBytes(StandardCharsets.UTF_8));
        }

        InputFile inputFile = fileIO.newInputFile(oldPath);
        OutputFile outputFile = fileIO.newOutputFile(newPath);

        try (SeekableInputStream in = inputFile.newStream();
                PositionOutputStream out = outputFile.create()) {
            byte[] buffer = new byte[64 * 1024];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) > 0) {
                out.write(buffer, 0, bytesRead);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    new IOException(format("Failed to copy file from %s to %s", oldPath, newPath), e));
        }

        String payload = oldPath + FRAGMENT_DELIMITER + newPath;
        return Slices.wrappedBuffer(payload.getBytes(StandardCharsets.UTF_8));
    }

    private void finishCallDistributedProcedure(
            ConnectorSession session,
            ConnectorProcedureContext procedureContext,
            ConnectorDistributedProcedureHandle handle,
            Collection<Slice> fragments)
    {
        IcebergProcedureContext icebergContext = (IcebergProcedureContext) procedureContext;
        IcebergDistributedProcedureHandle icebergHandle = (IcebergDistributedProcedureHandle) handle;
        Table icebergTable = icebergContext.getTransaction().table();
        Map<String, String> pathRemapping = decodeFragments(fragments);
        Set<DataFile> existingDataFiles = new HashSet<>();
        Set<DataFile> newDataFiles = new HashSet<>();
        Snapshot currentSnapshot = icebergTable.currentSnapshot();
        if (currentSnapshot == null) {
            return;
        }
        long validationSnapshotId = currentSnapshot.snapshotId();

        TableScan tableScan = icebergTable.newScan().useSnapshot(currentSnapshot.snapshotId());

        try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile oldFile = task.file();
                String oldPath = oldFile.path().toString();
                String newPath = pathRemapping.get(oldPath);

                if (newPath == null) {
                    throw new IllegalStateException(
                            format("No fragment received for data file: %s. " +
                                    "The distributed copy may be incomplete.", oldPath));
                }

                existingDataFiles.add(oldFile);

                DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                        .copy(oldFile)
                        .withPath(newPath);

                if (oldFile.partition() != null) {
                    builder.withPartition(oldFile.partition());
                }

                newDataFiles.add(builder.build());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        RewriteFiles rewrite = icebergContext.getTransaction().newRewrite().rewriteFiles(existingDataFiles, ImmutableSet.of(), newDataFiles, ImmutableSet.of());
        rewrite.validateFromSnapshot(currentSnapshot.snapshotId());
        Snapshot snapshot = icebergTable.currentSnapshot();
        rewrite.commit();
    }

    private static Map<String, String> decodeFragments(Collection<Slice> fragments)
    {
        ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
        for (Slice fragment : fragments) {
            String payload = new String(fragment.getBytes(), StandardCharsets.UTF_8);
            int delimIndex = payload.indexOf(FRAGMENT_DELIMITER);
            if (delimIndex < 0) {
                throw new IllegalArgumentException(
                        format("Malformed fragment (missing delimiter): %s", payload));
            }
            String oldPath = payload.substring(0, delimIndex);
            String newPath = payload.substring(delimIndex + 1);
            map.put(oldPath, newPath);
        }
        return map.build();
    }

    private static String stripTrailingSlash(String path)
    {
        requireNonNull(path, "path is null");
        return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }
}
