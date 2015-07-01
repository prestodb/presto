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
package com.facebook.presto.hive.parquet.reader;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.ColumnDescriptor;
import parquet.column.page.PageReadStore;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetReader
{
    public static final int MAX_VECTOR_LENGTH = 1024;

    private final Path file;
    private final MessageType fileSchema;
    private final Map<String, String> extraMetadata;
    private final MessageType requestedSchema;
    private final ParquetFileReader fileReader;
    private final List<BlockMetaData> blocks;
    private final Configuration configuration;

    private PageReadStore readerStore;
    private long fileRowCount;
    private long currentPosition;
    private long currentGroupRowCount;
    private long nextRowInGroup;
    private Map<ColumnDescriptor, ParquetColumnReader> columnReadersMap = new HashMap<>();

    public ParquetReader(MessageType fileSchema,
            Map<String, String> extraMetadata,
            MessageType requestedSchema,
            Path file,
            List<BlockMetaData> blocks,
            Configuration configuration)
            throws IOException
    {
        this.fileSchema = fileSchema;
        this.extraMetadata = extraMetadata;
        this.requestedSchema = requestedSchema;
        this.file = file;
        this.blocks = blocks;
        this.configuration = configuration;
        this.fileReader = new ParquetFileReader(configuration, file, blocks, requestedSchema.getColumns());
        for (BlockMetaData block : blocks) {
            fileRowCount += block.getRowCount();
        }
    }

    public void close()
            throws IOException
    {
        if (fileReader != null) {
            fileReader.close();
        }
    }

    public float getProgress()
            throws IOException, InterruptedException
    {
        if (fileRowCount == 0) {
            return 0.0f;
        }
        return (float) currentPosition / fileRowCount;
    }

    public long getPosition()
    {
        return currentPosition;
    }

    public long getFileRowCount()
    {
        return fileRowCount;
    }

    public int nextBatch()
            throws IOException, InterruptedException
    {
        if (nextRowInGroup >= currentGroupRowCount) {
            if (!advanceToNextRowGroup()) {
                return -1;
            }
        }

        int batchSize = Ints.checkedCast(Math.min(MAX_VECTOR_LENGTH, currentGroupRowCount - nextRowInGroup));

        nextRowInGroup += batchSize;
        currentPosition += batchSize;
        return batchSize;
    }

    private boolean advanceToNextRowGroup()
            throws InterruptedException
    {
        long rowCount = fileReader.readNextRowGroup();
        if (rowCount == -1) {
            return false;
        }
        nextRowInGroup = 0;
        currentGroupRowCount = rowCount;
        return true;
    }

    public Block readBlock(ColumnDescriptor columnDescriptor, int vectorSize, Type type)
            throws IOException
    {
        ParquetColumnReader columnReader;
        if (columnReadersMap.containsKey(columnDescriptor)) {
            columnReader = columnReadersMap.get(columnDescriptor);
        }
        else {
            columnReader = new ParquetColumnReader(columnDescriptor, fileReader.readColumn(columnDescriptor));
            columnReadersMap.put(columnDescriptor, columnReader);
        }
        return columnReader.readBlock(vectorSize, type);
    }
}
