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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_STORAGE_ERROR;
import static com.facebook.presto.raptorx.util.OrcUtil.createOrcFileWriter;
import static com.facebook.presto.raptorx.util.OrcUtil.createOrcReader;
import static com.facebook.presto.raptorx.util.OrcUtil.createOrcRecordReader;
import static com.facebook.presto.raptorx.util.OrcUtil.fileOrcDataSource;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.Long.parseLong;
import static java.lang.Math.toIntExact;

public final class OrcFileRewriter
{
    private static final Logger log = Logger.get(OrcFileRewriter.class);

    private OrcFileRewriter() {}

    public static OrcFileInfo rewriteOrcFile(
            long newChunkId,
            File input,
            File output,
            Map<Long, Type> columns,
            CompressionType compressionType,
            BitSet rowsToDelete,
            ReaderAttributes readerAttributes,
            TypeManager typeManager,
            OrcWriterStats stats)
    {
        long start = System.nanoTime();
        try (OrcDataSource dataSource = fileOrcDataSource(input, readerAttributes)) {
            OrcReader reader = createOrcReader(dataSource, readerAttributes);

            ImmutableMap.Builder<Integer, Type> columnBuilder = ImmutableMap.builder();
            ImmutableList.Builder<Long> columnIdBuilder = ImmutableList.builder();
            for (int i = 0; i < reader.getColumnNames().size(); i++) {
                long columnId = parseLong(reader.getColumnNames().get(i));
                Type type = columns.get(columnId);
                if (type != null) {
                    columnBuilder.put(i, type);
                    columnIdBuilder.add(columnId);
                }
            }
            Map<Integer, Type> includedColumns = columnBuilder.build();
            List<Long> columnIds = columnIdBuilder.build();
            List<Type> columnTypes = includedColumns.values().stream().collect(toImmutableList());

            StorageTypeConverter converter = new StorageTypeConverter(typeManager);
            List<Type> storageTypes = columnTypes.stream()
                    .map(converter::toStorageType)
                    .collect(toImmutableList());

            OrcRecordReader recordReader = createOrcRecordReader(reader, includedColumns);

            long fileRowCount = recordReader.getFileRowCount();
            if (fileRowCount < rowsToDelete.length()) {
                throw new IOException("File has fewer rows than deletion vector");
            }
            int deleteRowCount = rowsToDelete.cardinality();
            if (fileRowCount == deleteRowCount) {
                return new OrcFileInfo(0, 0);
            }
            if (fileRowCount >= Integer.MAX_VALUE) {
                throw new IOException("File has too many rows");
            }

            OrcFileInfo fileInfo;
            Map<String, String> metadata = OrcFileMetadata.from(newChunkId, columnIds, columnTypes).toMap();
            try (OrcWriter writer = createOrcFileWriter(output, columnIds, storageTypes, compressionType, metadata, stats)) {
                fileInfo = rewrite(recordReader, writer, rowsToDelete, columnTypes);
            }
            log.debug("Rewrote file %s in %s (input rows: %s, output rows: %s)", input.getName(), nanosSince(start), fileRowCount, fileRowCount - deleteRowCount);
            return fileInfo;
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_STORAGE_ERROR, "Failed to rewrite file: " + input, e);
        }
    }

    private static OrcFileInfo rewrite(OrcRecordReader reader, OrcWriter writer, BitSet rowsToDelete, List<Type> types)
            throws IOException
    {
        int row = rowsToDelete.nextClearBit(0);
        long rowCount = 0;
        long uncompressedSize = 0;

        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }

            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }

            if (row >= (reader.getFilePosition() + batchSize)) {
                continue;
            }

            Block[] blocks = new Block[types.size()];
            for (int i = 0; i < types.size(); i++) {
                blocks[i] = reader.readBlock(types.get(i), i);
            }

            row = toIntExact(reader.getFilePosition());

            Page page = maskedPage(blocks, rowsToDelete, row, batchSize);
            writer.write(page);

            rowCount += page.getPositionCount();
            uncompressedSize += page.getLogicalSizeInBytes();

            row = rowsToDelete.nextClearBit(row + batchSize);
        }

        return new OrcFileInfo(rowCount, uncompressedSize);
    }

    private static Page maskedPage(Block[] blocks, BitSet rowsToDelete, int start, int count)
    {
        int[] ids = new int[count];
        int size = 0;
        for (int i = 0; i < count; i++) {
            if (!rowsToDelete.get(start + i)) {
                ids[size] = i;
                size++;
            }
        }

        Block[] maskedBlocks = new Block[blocks.length];
        for (int i = 0; i < blocks.length; i++) {
            maskedBlocks[i] = new DictionaryBlock(size, blocks[i], ids);
        }
        return new Page(maskedBlocks);
    }

    public static class OrcFileInfo
    {
        private final long rowCount;
        private final long uncompressedSize;

        public OrcFileInfo(long rowCount, long uncompressedSize)
        {
            this.rowCount = rowCount;
            this.uncompressedSize = uncompressedSize;
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public long getUncompressedSize()
        {
            return uncompressedSize;
        }
    }
}
