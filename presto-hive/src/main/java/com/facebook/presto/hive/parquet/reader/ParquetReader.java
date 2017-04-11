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
import com.facebook.presto.hive.parquet.memory.AggregatedMemoryContext;
import com.facebook.presto.hive.parquet.memory.LocalMemoryContext;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.InterleavedBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
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
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ParquetReader
        implements Closeable
{
    private static final int MAX_VECTOR_LENGTH = 1024;
    private static final String MAP_TYPE_NAME = "map";
    private static final String MAP_KEY_NAME = "key";
    private static final String MAP_VALUE_NAME = "value";
    private static final String ARRAY_TYPE_NAME = "bag";
    private static final String ARRAY_ELEMENT_NAME = "array_element";

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

    private AggregatedMemoryContext currentRowGroupMemoryContext;
    private final AggregatedMemoryContext systemMemoryContext;

    public ParquetReader(MessageType fileSchema,
            MessageType requestedSchema,
            List<BlockMetaData> blocks,
            ParquetDataSource dataSource,
            TypeManager typeManager,
            AggregatedMemoryContext systemMemoryContext)
    {
        this.fileSchema = fileSchema;
        this.requestedSchema = requestedSchema;
        this.blocks = blocks;
        this.dataSource = dataSource;
        this.typeManager = typeManager;
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.currentRowGroupMemoryContext = systemMemoryContext.newAggregatedMemoryContext();
        initializeColumnReaders();
    }

    @Override
    public void close()
            throws IOException
    {
        currentRowGroupMemoryContext.close();
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

        batchSize = toIntExact(min(MAX_VECTOR_LENGTH, currentGroupRowCount - nextRowInGroup));

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
        currentRowGroupMemoryContext.close();
        currentRowGroupMemoryContext = systemMemoryContext.newAggregatedMemoryContext();

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

    public Block readArray(Type type, List<String> path)
            throws IOException
    {
        return readArray(type, path, new IntArrayList());
    }

    private Block readArray(Type type, List<String> path, IntList elementOffsets)
            throws IOException
    {
        List<Type> parameters = type.getTypeParameters();
        checkArgument(parameters.size() == 1, "Arrays must have a single type parameter, found %d", parameters.size());
        path.add(ARRAY_TYPE_NAME);
        Type elementType = parameters.get(0);
        Block block = readBlock(ARRAY_ELEMENT_NAME, elementType, path, elementOffsets);
        path.remove(ARRAY_TYPE_NAME);

        if (elementOffsets.isEmpty()) {
            for (int i = 0; i < batchSize; i++) {
                elementOffsets.add(0);
            }
            return RunLengthEncodedBlock.create(elementType, null, batchSize);
        }

        int[] offsets = new int[batchSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            offsets[i] = offsets[i - 1] + elementOffsets.getInt(i - 1);
        }
        return new ArrayBlock(batchSize, new boolean[batchSize], offsets, block);
    }

    public Block readMap(Type type, List<String> path)
            throws IOException
    {
        return readMap(type, path, new IntArrayList());
    }

    private Block readMap(Type type, List<String> path, IntList elementOffsets)
            throws IOException
    {
        List<Type> parameters = type.getTypeParameters();
        checkArgument(parameters.size() == 2, "Maps must have two type parameters, found %d", parameters.size());
        Block[] blocks = new Block[parameters.size()];

        IntList keyOffsets = new IntArrayList();
        IntList valueOffsets = new IntArrayList();
        path.add(MAP_TYPE_NAME);
        blocks[0] = readBlock(MAP_KEY_NAME, parameters.get(0), path, keyOffsets);
        blocks[1] = readBlock(MAP_VALUE_NAME, parameters.get(1), path, valueOffsets);
        path.remove(MAP_TYPE_NAME);

        if (blocks[0].getPositionCount() == 0) {
            for (int i = 0; i < batchSize; i++) {
                elementOffsets.add(0);
            }
            return RunLengthEncodedBlock.create(parameters.get(0), null, batchSize);
        }
        InterleavedBlock interleavedBlock = new InterleavedBlock(new Block[] {blocks[0], blocks[1]});
        int[] offsets = new int[batchSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            int elementPositionCount = keyOffsets.getInt(i - 1) * 2;
            elementOffsets.add(elementPositionCount);
            offsets[i] = offsets[i - 1] + elementPositionCount;
        }
        return new ArrayBlock(batchSize, new boolean[batchSize], offsets, interleavedBlock);
    }

    public Block readStruct(Type type, List<String> path)
            throws IOException
    {
        return readStruct(type, path, new IntArrayList());
    }

    private Block readStruct(Type type, List<String> path, IntList elementOffsets)
            throws IOException
    {
        List<TypeSignatureParameter> parameters = type.getTypeSignature().getParameters();
        Block[] blocks = new Block[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            NamedTypeSignature namedTypeSignature = parameters.get(i).getNamedTypeSignature();
            Type fieldType = typeManager.getType(namedTypeSignature.getTypeSignature());
            String name = namedTypeSignature.getName();
            blocks[i] = readBlock(name, fieldType, path, new IntArrayList());
        }

        InterleavedBlock interleavedBlock = new InterleavedBlock(blocks);
        int blockSize = blocks[0].getPositionCount();
        int[] offsets = new int[blockSize + 1];
        for (int i = 1; i < offsets.length; i++) {
            elementOffsets.add(parameters.size());
            offsets[i] = i * parameters.size();
        }
        return new ArrayBlock(blockSize, new boolean[blockSize], offsets, interleavedBlock);
    }

    public Block readPrimitive(ColumnDescriptor columnDescriptor, Type type)
            throws IOException
    {
        return readPrimitive(columnDescriptor, type, new IntArrayList());
    }

    private Block readPrimitive(ColumnDescriptor columnDescriptor, Type type, IntList offsets)
            throws IOException
    {
        ParquetColumnReader columnReader = columnReadersMap.get(columnDescriptor);
        if (columnReader.getPageReader() == null) {
            validateParquet(currentBlockMetadata.getRowCount() > 0, "Row group has 0 rows");
            ColumnChunkMetaData metadata = getColumnChunkMetaData(columnDescriptor);
            long startingPosition = metadata.getStartingPos();
            int totalSize = toIntExact(metadata.getTotalSize());
            byte[] buffer = allocateBlock(totalSize);
            dataSource.readFully(startingPosition, buffer);
            ParquetColumnChunkDescriptor descriptor = new ParquetColumnChunkDescriptor(columnDescriptor, metadata, totalSize);
            ParquetColumnChunk columnChunk = new ParquetColumnChunk(descriptor, buffer, 0);
            columnReader.setPageReader(columnChunk.readAllPages());
        }
        return columnReader.readPrimitive(type, offsets);
    }

    private byte[] allocateBlock(int length)
    {
        byte[] buffer = new byte[length];
        LocalMemoryContext blockMemoryContext = currentRowGroupMemoryContext.newLocalMemoryContext();
        blockMemoryContext.setBytes(buffer.length);
        return buffer;
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

    private Block readBlock(String name, Type type, List<String> path, IntList offsets)
            throws IOException
    {
        path.add(name);
        Optional<RichColumnDescriptor> descriptor = getDescriptor(fileSchema, requestedSchema, path);
        if (!descriptor.isPresent()) {
            path.remove(name);
            return RunLengthEncodedBlock.create(type, null, batchSize);
        }

        Block block;
        if (ROW.equals(type.getTypeSignature().getBase())) {
            block = readStruct(type, path, offsets);
        }
        else if (MAP.equals(type.getTypeSignature().getBase())) {
            block = readMap(type, path, offsets);
        }
        else if (ARRAY.equals(type.getTypeSignature().getBase())) {
            block = readArray(type, path, offsets);
        }
        else {
            block = readPrimitive(descriptor.get(), type, offsets);
        }
        path.remove(name);
        return block;
    }
}
