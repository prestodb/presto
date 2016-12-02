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
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.InterleavedBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
import parquet.io.PrimitiveColumnIO;
import parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getColumns;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getDescriptor;
import static com.facebook.presto.hive.parquet.ParquetValidationUtils.validateParquet;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.google.common.primitives.Ints.checkedCast;
import static java.lang.Math.min;

public class ParquetReader
        implements Closeable
{
    private static final int MAX_VECTOR_LENGTH = 1024;

    private final MessageType fileSchema;
    private final MessageType requestedSchema;
    private final List<BlockMetaData> blocks;
    private final ParquetDataSource dataSource;
    private final TypeManager typeManager;

    private int currentBlock;
    private BlockMetaData currentBlockMetadata;
    private long currentPosition;
    private long currentGroupRowCount;
    private long nextRowInGroup;
    private int batchSize;
    private final Map<ColumnDescriptor, ParquetColumnReader> columnReadersMap = new HashMap<>();

    public ParquetReader(MessageType fileSchema,
            MessageType requestedSchema,
            List<BlockMetaData> blocks,
            ParquetDataSource dataSource,
            TypeManager typeManager)
    {
        this.fileSchema = fileSchema;
        this.requestedSchema = requestedSchema;
        this.blocks = blocks;
        this.dataSource = dataSource;
        this.typeManager = typeManager;
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

        batchSize = checkedCast(min(MAX_VECTOR_LENGTH, currentGroupRowCount - nextRowInGroup));

        nextRowInGroup += batchSize;
        currentPosition += batchSize;
        for (PrimitiveColumnIO columnIO : getColumns(fileSchema, requestedSchema)) {
            ColumnDescriptor descriptor = columnIO.getColumnDescriptor();
            RichColumnDescriptor column = new RichColumnDescriptor(descriptor.getPath(), columnIO.getType().asPrimitiveType(), descriptor.getMaxRepetitionLevel(), descriptor.getMaxDefinitionLevel());
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

    public Block readStruct(Type type, List<String> path)
            throws IOException
    {
        List<TypeSignatureParameter> parameters = type.getTypeSignature().getParameters();
        Block[] blocks = new Block[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            NamedTypeSignature namedTypeSignature = parameters.get(i).getNamedTypeSignature();
            Type fieldType = typeManager.getType(namedTypeSignature.getTypeSignature());
            String name = namedTypeSignature.getName();
            path.add(name);
            Optional<RichColumnDescriptor> columnDescriptor = getDescriptor(fileSchema, requestedSchema, path);
            if (!columnDescriptor.isPresent()) {
                path.remove(name);
                blocks[i] = RunLengthEncodedBlock.create(type, null, batchSize);
                continue;
            }

            if (ROW.equals(fieldType.getTypeSignature().getBase())) {
                blocks[i] = readStruct(fieldType, path);
            }
            else {
                blocks[i] = readPrimitive(columnDescriptor.get(), fieldType);
            }
            path.remove(name);
        }

        InterleavedBlock interleavedBlock = new InterleavedBlock(blocks);
        int[] offsets = new int[batchSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            offsets[i] = i * parameters.size();
        }
        return new ArrayBlock(batchSize, new boolean[batchSize], offsets, interleavedBlock);
    }

    public Block readPrimitive(ColumnDescriptor columnDescriptor, Type type)
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
        return columnReader.readPrimitive(type);
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
        for (PrimitiveColumnIO columnIO : getColumns(fileSchema, requestedSchema)) {
            ColumnDescriptor descriptor = columnIO.getColumnDescriptor();
            RichColumnDescriptor column = new RichColumnDescriptor(descriptor.getPath(), columnIO.getType().asPrimitiveType(), descriptor.getMaxRepetitionLevel(), descriptor.getMaxDefinitionLevel());
            columnReadersMap.put(column, ParquetColumnReader.createReader(column));
        }
    }
}
