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

import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergMetadataFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.util.ContentFileUtil;

import javax.inject.Provider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.iceberg.IcebergUtil.getIcebergTable;
import static com.google.common.base.Preconditions.checkState;

import static java.util.Objects.requireNonNull;

/**
 * Procedure to compact deletion vectors (DVs) on V3 Iceberg tables.
 *
 * When multiple DELETE operations target rows in the same data file, each produces
 * a separate DV (Puffin file). This procedure merges all DVs per data file into
 * a single consolidated DV, reducing metadata overhead and improving read performance.
 *
 * Usage: CALL iceberg.system.rewrite_delete_files('schema', 'table')
 */
public class RewriteDeleteFilesProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REWRITE_DELETE_FILES = methodHandle(
            RewriteDeleteFilesProcedure.class,
            "rewriteDeleteFiles",
            ConnectorSession.class,
            String.class,
            String.class);

    private final IcebergMetadataFactory metadataFactory;

    @Inject
    public RewriteDeleteFilesProcedure(IcebergMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "rewrite_delete_files",
                ImmutableList.of(
                        new Argument("schema", VARCHAR),
                        new Argument("table_name", VARCHAR)),
                REWRITE_DELETE_FILES.bindTo(this));
    }

    public void rewriteDeleteFiles(ConnectorSession clientSession, String schemaName, String tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) metadataFactory.create();
            Table icebergTable = getIcebergTable(metadata, clientSession, schemaTableName);

            int formatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
            if (formatVersion < 3) {
                return;
            }

            // Group delete files by their referenced data file. Capture the partition tuple
            // alongside so we can hand it to BaseDVFileWriter when writing the merged DV.
            Map<String, List<DeleteFile>> dvsByDataFile = new HashMap<>();
            Map<String, FileScanTask> taskByDataFile = new HashMap<>();

            try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
                CloseableIterator<FileScanTask> iterator = tasks.iterator();
                while (iterator.hasNext()) {
                    FileScanTask task = iterator.next();
                    String dataFilePath = task.file().path().toString();
                    for (DeleteFile deleteFile : task.deletes()) {
                        if (ContentFileUtil.isDV(deleteFile)) {
                            dvsByDataFile.computeIfAbsent(dataFilePath, k -> new ArrayList<>()).add(deleteFile);
                            taskByDataFile.putIfAbsent(dataFilePath, task);
                        }
                    }
                }
                iterator.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            // Find data files with multiple DVs that need compaction.
            Set<DeleteFile> filesToRemove = new HashSet<>();
            Set<DeleteFile> filesToAdd = new HashSet<>();

            for (Map.Entry<String, List<DeleteFile>> entry : dvsByDataFile.entrySet()) {
                List<DeleteFile> dvs = entry.getValue();
                if (dvs.size() <= 1) {
                    continue;
                }

                String dataFilePath = entry.getKey();
                FileScanTask task = taskByDataFile.get(dataFilePath);
                DeleteFile mergedDv = writeMergedDeletionVector(icebergTable, task, dataFilePath, dvs);
                filesToRemove.addAll(dvs);
                filesToAdd.add(mergedDv);
            }

            if (filesToRemove.isEmpty()) {
                metadata.commit();
                return;
            }

            // Commit the rewrite: remove old DVs, add merged DVs.
            RewriteFiles rewriteFiles = icebergTable.newRewrite()
                    .rewriteFiles(ImmutableSet.of(), filesToRemove, ImmutableSet.of(), filesToAdd);
            rewriteFiles.commit();
            metadata.commit();
        }
    }

    private static DeleteFile writeMergedDeletionVector(
            Table icebergTable,
            FileScanTask task,
            String dataFilePath,
            List<DeleteFile> dvs)
    {
        PartitionSpec partitionSpec = task.spec();
        StructLike partition = task.file().partition();

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(icebergTable, 0, 0L)
                .format(FileFormat.PUFFIN)
                .operationId(UUID.randomUUID().toString())
                .build();

        try (BaseDVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null)) {
            // Feed every existing position into the writer; it dedupes internally.
            for (DeleteFile dv : dvs) {
                PositionDeleteIndex index = readDeletionVectorIndex(icebergTable, dv);
                index.forEach((long pos) -> writer.delete(dataFilePath, pos, partitionSpec, partition));
            }
            writer.close();
            List<DeleteFile> produced = writer.result().deleteFiles();
            checkState(!produced.isEmpty(),
                    "BaseDVFileWriter produced no DeleteFile when compacting %s DVs for %s",
                    dvs.size(), dataFilePath);
            return produced.get(0);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static PositionDeleteIndex readDeletionVectorIndex(Table icebergTable, DeleteFile dv)
    {
        try (PuffinReader reader = Puffin.read(icebergTable.io().newInputFile(dv.path().toString())).build()) {
            List<BlobMetadata> blobs = reader.fileMetadata().blobs();
            if (blobs.isEmpty()) {
                return PositionDeleteIndex.empty();
            }
            for (org.apache.iceberg.util.Pair<BlobMetadata, ByteBuffer> pair : reader.readAll(blobs)) {
                ByteBuffer blobData = pair.second();
                byte[] bytes = new byte[blobData.remaining()];
                blobData.get(bytes);
                return PositionDeleteIndex.deserialize(bytes, dv);
            }
            return PositionDeleteIndex.empty();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
