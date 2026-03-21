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
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;

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

            // Group delete files by their referenced data file
            Map<String, List<DeleteFile>> dvsByDataFile = new HashMap<>();
            Set<DeleteFile> allDeleteFiles = new HashSet<>();

            try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
                CloseableIterator<FileScanTask> iterator = tasks.iterator();
                while (iterator.hasNext()) {
                    FileScanTask task = iterator.next();
                    String dataFilePath = task.file().path().toString();
                    for (DeleteFile deleteFile : task.deletes()) {
                        if (deleteFile.format() == FileFormat.PUFFIN &&
                                deleteFile.content() == FileContent.POSITION_DELETES) {
                            dvsByDataFile.computeIfAbsent(dataFilePath, k -> new ArrayList<>()).add(deleteFile);
                            allDeleteFiles.add(deleteFile);
                        }
                    }
                }
                iterator.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            // Find data files with multiple DVs that need compaction
            Set<DeleteFile> filesToRemove = new HashSet<>();
            Set<DeleteFile> filesToAdd = new HashSet<>();

            for (Map.Entry<String, List<DeleteFile>> entry : dvsByDataFile.entrySet()) {
                List<DeleteFile> dvs = entry.getValue();
                if (dvs.size() <= 1) {
                    continue;
                }

                String dataFilePath = entry.getKey();

                // Merge roaring bitmaps from all DVs for this data file
                Set<Integer> mergedPositions = new HashSet<>();
                for (DeleteFile dv : dvs) {
                    readDeletionVectorPositions(icebergTable, dv, mergedPositions);
                    filesToRemove.add(dv);
                }

                // Write consolidated DV
                DeleteFile mergedDv = writeMergedDeletionVector(
                        icebergTable,
                        dvs.get(0),
                        dataFilePath,
                        mergedPositions);
                filesToAdd.add(mergedDv);
            }

            if (filesToRemove.isEmpty()) {
                metadata.commit();
                return;
            }

            // Commit the rewrite: remove old DVs, add merged DVs
            RewriteFiles rewriteFiles = icebergTable.newRewrite()
                    .rewriteFiles(ImmutableSet.of(), filesToRemove, ImmutableSet.of(), filesToAdd);
            rewriteFiles.commit();
            metadata.commit();
        }
    }

    private void readDeletionVectorPositions(Table table, DeleteFile dv, Set<Integer> positions)
    {
        InputFile inputFile = table.io().newInputFile(dv.path().toString());
        try (PuffinReader reader = Puffin.read(inputFile).build()) {
            List<BlobMetadata> blobMetadataList = reader.fileMetadata().blobs();
            if (blobMetadataList.isEmpty()) {
                return;
            }
            for (org.apache.iceberg.util.Pair<BlobMetadata, ByteBuffer> pair : reader.readAll(blobMetadataList)) {
                ByteBuffer blobData = pair.second();
                deserializeRoaringBitmap(blobData, positions);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DeleteFile writeMergedDeletionVector(
            Table table,
            DeleteFile templateDv,
            String dataFilePath,
            Set<Integer> mergedPositions)
    {
        List<Integer> sortedPositions = new ArrayList<>(mergedPositions);
        sortedPositions.sort(Integer::compareTo);
        byte[] roaringBytes = serializeRoaringBitmap(sortedPositions);

        String fileName = "dv-" + UUID.randomUUID() + ".puffin";
        String dvPath = table.location() + "/data/" + fileName;
        OutputFile outputFile = table.io().newOutputFile(dvPath);

        long puffinFileSize;
        long blobOffset;
        long blobLength;
        try {
            PuffinWriter writer = Puffin.write(outputFile).createdBy("presto").build();
            writer.add(new Blob(
                    "deletion-vector-v2",
                    ImmutableList.of(),
                    0,
                    0,
                    ByteBuffer.wrap(roaringBytes)));
            writer.finish();
            puffinFileSize = writer.fileSize();
            blobOffset = writer.writtenBlobsMetadata().get(0).offset();
            blobLength = writer.writtenBlobsMetadata().get(0).length();
            writer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return FileMetadata.deleteFileBuilder(table.specs().get(templateDv.specId()))
                .ofPositionDeletes()
                .withPath(dvPath)
                .withFileSizeInBytes(puffinFileSize)
                .withFormat(FileFormat.PUFFIN)
                .withRecordCount(sortedPositions.size())
                .withContentSizeInBytes(blobLength)
                .withContentOffset(blobOffset)
                .withReferencedDataFile(dataFilePath)
                .build();
    }

    private static void deserializeRoaringBitmap(ByteBuffer buffer, Set<Integer> positions)
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);

        int cookie = buf.getInt();
        boolean isRunContainer = (cookie & 0xFFFF) == 12347;
        int numContainers;
        if (isRunContainer) {
            numContainers = (cookie >>> 16) + 1;
            // skip run bitmap
            int runBitmapBytes = (numContainers + 7) / 8;
            buf.position(buf.position() + runBitmapBytes);
        }
        else if ((cookie & 0xFFFF) == 12346) {
            numContainers = (cookie >>> 16) + 1;
        }
        else {
            return;
        }

        int[] keys = new int[numContainers];
        int[] cardinalities = new int[numContainers];
        for (int i = 0; i < numContainers; i++) {
            keys[i] = Short.toUnsignedInt(buf.getShort());
            cardinalities[i] = Short.toUnsignedInt(buf.getShort()) + 1;
        }

        for (int i = 0; i < numContainers; i++) {
            int highBits = keys[i] << 16;
            if (cardinalities[i] <= 4096) {
                // Array container
                for (int j = 0; j < cardinalities[i]; j++) {
                    positions.add(highBits | Short.toUnsignedInt(buf.getShort()));
                }
            }
            else {
                // Bitmap container
                for (int wordIdx = 0; wordIdx < 1024; wordIdx++) {
                    long word = buf.getLong();
                    while (word != 0) {
                        int bit = Long.numberOfTrailingZeros(word);
                        positions.add(highBits | (wordIdx * 64 + bit));
                        word &= word - 1;
                    }
                }
            }
        }
    }

    static byte[] serializeRoaringBitmap(List<Integer> sortedPositions)
    {
        // Group positions into containers (each container covers 2^16 values)
        Map<Integer, List<Integer>> containers = new HashMap<>();
        for (int pos : sortedPositions) {
            int key = pos >>> 16;
            int low = pos & 0xFFFF;
            containers.computeIfAbsent(key, k -> new ArrayList<>()).add(low);
        }

        List<Integer> sortedKeys = new ArrayList<>(containers.keySet());
        sortedKeys.sort(Integer::compareTo);

        // Calculate size
        int numContainers = sortedKeys.size();
        // Cookie (4 bytes) + key-cardinality pairs (4 bytes each)
        int headerSize = 4 + numContainers * 4;
        int dataSize = 0;
        for (int key : sortedKeys) {
            int card = containers.get(key).size();
            if (card <= 4096) {
                dataSize += card * 2; // array container
            }
            else {
                dataSize += 1024 * 8; // bitmap container
            }
        }

        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(headerSize + dataSize)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN);

        // Cookie: SERIAL_COOKIE_NO_RUNCONTAINER (12346) | (numContainers - 1) << 16
        buf.putInt(12346 | ((numContainers - 1) << 16));

        // Key-cardinality pairs
        for (int key : sortedKeys) {
            buf.putShort((short) key);
            buf.putShort((short) (containers.get(key).size() - 1));
        }

        // Container data
        for (int key : sortedKeys) {
            List<Integer> values = containers.get(key);
            values.sort(Integer::compareTo);
            if (values.size() <= 4096) {
                for (int val : values) {
                    buf.putShort((short) val);
                }
            }
            else {
                long[] bitmap = new long[1024];
                for (int val : values) {
                    bitmap[val >>> 6] |= 1L << (val & 63);
                }
                for (long word : bitmap) {
                    buf.putLong(word);
                }
            }
        }

        return buf.array();
    }
}
