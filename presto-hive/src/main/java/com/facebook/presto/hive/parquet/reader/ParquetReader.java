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

import com.facebook.presto.hive.parquet.ParquetCorruptionException;
import com.facebook.presto.hive.parquet.ParquetDataSource;
import com.facebook.presto.hive.parquet.RichColumnDescriptor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.parquet.ParquetValidationUtils.validateParquet;
import static com.google.common.primitives.Ints.checkedCast;
import static java.lang.Math.min;

public class ParquetReader
        implements Closeable
{
    private static final int MAX_VECTOR_LENGTH = 1024;

    private final MessageType requestedSchema;
    private final List<BlockMetaData> blocks;
    private final ParquetDataSource dataSource;

    private int currentBlock;
    private BlockMetaData currentBlockMetadata;
    private long currentPosition;
    private long currentGroupRowCount;
    private long nextRowInGroup;
    private final Map<ColumnDescriptor, ParquetColumnReader> columnReadersMap = new HashMap<>();

    public ParquetReader(
            MessageType requestedSchema,
            List<BlockMetaData> blocks,
            ParquetDataSource dataSource)
    {
        this.requestedSchema = requestedSchema;
        this.blocks = blocks;
        this.dataSource = dataSource;
        initializeColumnReaders();
    }

    @Override
    public void close()
            throws IOException
    {
        dataSource.close();
    }

    public long getPosition()
    {
        return currentPosition;
    }

    public int nextBatch()
    {
        if (nextRowInGroup >= currentGroupRowCount && !advanceToNextRowGroup()) {
            return -1;
        }

        int batchSize = checkedCast(min(MAX_VECTOR_LENGTH, currentGroupRowCount - nextRowInGroup));

        nextRowInGroup += batchSize;
        currentPosition += batchSize;
        for (ColumnDescriptor column : getColumns(requestedSchema)) {
            ParquetColumnReader columnReader = columnReadersMap.get(column);
            columnReader.prepareNextRead(batchSize);
        }
        return batchSize;
    }

    private boolean advanceToNextRowGroup()
    {
        if (currentBlock == blocks.size()) {
            return false;
        }
        currentBlockMetadata = blocks.get(currentBlock);
        currentBlock = currentBlock + 1;

        nextRowInGroup = 0L;
        currentGroupRowCount = currentBlockMetadata.getRowCount();
        columnReadersMap.clear();
        initializeColumnReaders();
        return true;
    }

    public Block readBlock(ColumnDescriptor columnDescriptor, Type type)
            throws IOException
    {
        ParquetColumnReader columnReader = columnReadersMap.get(columnDescriptor);
        if (columnReader.getPageReader() == null) {
            validateParquet(currentBlockMetadata.getRowCount() > 0, "Row group has 0 rows");
            ColumnChunkMetaData metadata = getColumnChunkMetaData(columnDescriptor);
            long startingPosition = metadata.getStartingPos();
            int totalSize = checkedCast(metadata.getTotalSize());
            byte[] buffer = new byte[totalSize];
            dataSource.readFully(startingPosition, buffer);
            ParquetColumnChunkDescriptor descriptor = new ParquetColumnChunkDescriptor(columnDescriptor, metadata, totalSize);
            ParquetColumnChunk columnChunk = new ParquetColumnChunk(descriptor, buffer, 0);
            columnReader.setPageReader(columnChunk.readAllPages());
        }
        return columnReader.readBlock(type);
    }

    private ColumnChunkMetaData getColumnChunkMetaData(ColumnDescriptor columnDescriptor)
            throws IOException
    {
        for (ColumnChunkMetaData metadata : currentBlockMetadata.getColumns()) {
            if (metadata.getPath().equals(ColumnPath.get(columnDescriptor.getPath()))) {
                return metadata;
            }
        }
        throw new ParquetCorruptionException("Metadata is missing for column: %s", columnDescriptor);
    }

    private void initializeColumnReaders()
    {
        for (RichColumnDescriptor column : getColumns(requestedSchema)) {
            columnReadersMap.put(column, ParquetColumnReader.createReader(column));
        }
    }

    private static List<RichColumnDescriptor> getColumns(MessageType schema)
    {
        List<String[]> paths = schema.getPaths();
        List<RichColumnDescriptor> columns = new ArrayList<>(paths.size());
        for (String[] path : paths) {
            PrimitiveType primitiveType = schema.getType(path).asPrimitiveType();
            columns.add(new RichColumnDescriptor(
                    path,
                    primitiveType,
                    schema.getMaxRepetitionLevel(path),
                    schema.getMaxDefinitionLevel(path)));
        }
        return columns;
    }
}
