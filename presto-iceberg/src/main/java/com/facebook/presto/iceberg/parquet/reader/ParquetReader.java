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
package com.facebook.presto.iceberg.parquet.reader;

import com.facebook.presto.iceberg.parquet.ParquetCorruptionException;
import com.facebook.presto.iceberg.parquet.ParquetDataSource;
import com.facebook.presto.iceberg.parquet.RichColumnDescriptor;
import com.facebook.presto.iceberg.parquet.memory.AggregatedMemoryContext;
import com.facebook.presto.iceberg.parquet.memory.LocalMemoryContext;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.parquet.ParquetTypeUtils.getColumns;
import static com.facebook.presto.iceberg.parquet.ParquetTypeUtils.getDescriptor;
import static com.facebook.presto.iceberg.parquet.ParquetTypeUtils.isValueNull;
import static com.facebook.presto.iceberg.parquet.ParquetValidationUtils.validateParquet;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

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
        return readArray(type, path, new IntArrayList(), new IntArrayList());
    }

    private Block readArray(Type type, List<String> path, IntList definitionLevels, IntList repetitionLevels)
            throws IOException
    {
        List<Type> parameters = type.getTypeParameters();
        checkArgument(parameters.size() == 1, "Arrays must have a single type parameter, found %d", parameters.size());
        final List<String[]> matchingPath = findMatchingPath(path);
        checkArgument(matchingPath.size() >= 1, "Arrays must have at least one matching path, none found for path = %s, requestedSchema=%s", Arrays.toString(path.toArray()), requestedSchema);
        final String arrayTypeName = matchingPath.get(0)[path.size()];
        path.add(arrayTypeName);
        Type elementType = parameters.get(0);
        Block block = readBlock(matchingPath.get(0)[path.size()], elementType, path, definitionLevels, repetitionLevels);
        path.remove(path.lastIndexOf(arrayTypeName));
        IntList offsets = new IntArrayList();
        BooleanList valueIsNull = new BooleanArrayList();
        calculateCollectionOffsets(toArray(path), offsets, valueIsNull, definitionLevels, repetitionLevels);
        return ArrayBlock.fromElementBlock(valueIsNull.size(), Optional.of(valueIsNull.toBooleanArray()), offsets.toIntArray(), block);
    }

    public List<String[]> findMatchingPath(List<String> processingPath)
    {
        final List<String[]> schemaPaths = requestedSchema.getPaths();
        return schemaPaths.stream().filter(paths -> paths.length > processingPath.size() && Arrays.equals(Arrays.copyOf(paths, processingPath.size()), processingPath.toArray())).collect(Collectors.toList());
    }

    public Block readMap(Type type, List<String> path)
            throws IOException
    {
        return readMap(type, path, new IntArrayList(), new IntArrayList());
    }

    private Block readMap(Type type, List<String> path, IntList definitionLevels, IntList repetitionLevels)
            throws IOException
    {
        List<Type> parameters = type.getTypeParameters();
        checkArgument(parameters.size() == 2, "Maps must have two type parameters, found %d", parameters.size());
        Block[] blocks = new Block[parameters.size()];

        IntList keyDefinitionLevels = new IntArrayList();
        IntList keyRepetitionLevels = new IntArrayList();
        final List<String[]> matchingPath = findMatchingPath(path);
        checkArgument(matchingPath.size() >= 2, "Maps must have at least two matching paths, none found for path = %s, requestedSchema=%s", Arrays.toString(path.toArray()), requestedSchema);
        final String mapTypeName = matchingPath.get(0)[path.size()];
        path.add(mapTypeName);
        final int size = path.size();
        blocks[0] = readBlock(matchingPath.get(0)[size], parameters.get(0), path, keyDefinitionLevels, keyRepetitionLevels);
        blocks[1] = readBlock(matchingPath.get(1)[size], parameters.get(1), path, new IntArrayList(), new IntArrayList());
        path.remove(path.lastIndexOf(mapTypeName));
        definitionLevels.addAll(keyDefinitionLevels);
        repetitionLevels.addAll(keyRepetitionLevels);
        IntList offsets = new IntArrayList();
        BooleanList valueIsNull = new BooleanArrayList();
        calculateCollectionOffsets(toArray(path), offsets, valueIsNull, keyDefinitionLevels, keyRepetitionLevels);
        return ((MapType) type).createBlockFromKeyValue(Optional.of(valueIsNull.toBooleanArray()), offsets.toIntArray(), blocks[0], blocks[1]);
    }

    public Block readStruct(Type type, List<String> path)
            throws IOException
    {
        return readStruct(type, path, new IntArrayList(), new IntArrayList());
    }

    private Block readStruct(Type type, List<String> path, IntList structDefinitionLevels, IntList structRepetitionLevels)
            throws IOException
    {
        List<TypeSignatureParameter> fields = type.getTypeSignature().getParameters();
        Block[] blocks = new Block[fields.size()];
        IntList fieldDefinitionLevels = null;
        IntList fieldRepetitionLevels = null;
        for (int i = 0; i < fields.size(); i++) {
            fieldDefinitionLevels = new IntArrayList();
            fieldRepetitionLevels = new IntArrayList();
            NamedTypeSignature namedTypeSignature = fields.get(i).getNamedTypeSignature();
            Type fieldType = typeManager.getType(namedTypeSignature.getTypeSignature());
            String name = namedTypeSignature.getName().get();
            blocks[i] = readBlock(name, fieldType, path, fieldDefinitionLevels, fieldRepetitionLevels);
        }
        BooleanList structIsNull = new BooleanArrayList();
        IntList structOffsets = new IntArrayList();
        calculateStructOffsets(toArray(path), structOffsets, structIsNull, structDefinitionLevels, structRepetitionLevels, fieldDefinitionLevels, fieldRepetitionLevels);
        return RowBlock.fromFieldBlocks(structIsNull.size(), Optional.of(structIsNull.toBooleanArray()), blocks);
    }

    /**
     * Each struct has three variants of presence:
     * 1) Struct is not defined, because one of it's optional parent fields is null
     * 2) Struct is null
     * 3) Struct is defined and not empty. In this case offset value is increased by one.
     * <p>
     * One of the struct's field repetition/definition levels are used to calculate offsets of the struct.
     */
    private void calculateStructOffsets(
            String[] path,
            IntList structOffsets,
            BooleanList structIsNull,
            IntList structDefinitionLevels,
            IntList structRepetitionLevels,
            IntList fieldDefinitionLevels,
            IntList fieldRepetitionLevels)
    {
        int offset = 0;
        structOffsets.add(offset);
        if (fieldDefinitionLevels != null) {
            int maxDefinitionLevel = getMaxDefinitionLevel(path);
            int maxRepetitionLevel = getMaxRepetitionLevel(path);
            for (int i = 0; i < fieldDefinitionLevels.size(); i++) {
                if (fieldRepetitionLevels.get(i) <= maxRepetitionLevel) {
                    if (isValueNull(isRequired(path), fieldDefinitionLevels.get(i), maxDefinitionLevel)) {
                        // Struct is null
                        structIsNull.add(true);
                        structOffsets.add(offset);
                    }
                    else if (fieldDefinitionLevels.get(i) >= maxDefinitionLevel) {
                        // Struct is defined and not empty
                        structIsNull.add(false);
                        offset++;
                        structOffsets.add(offset);
                    }
                }
            }
            structDefinitionLevels.addAll(fieldDefinitionLevels);
            structRepetitionLevels.addAll(fieldRepetitionLevels);
        }
    }

    /**
     * Each collection (Array or Map) has four variants of presence:
     * 1) Collection is not defined, because one of it's optional parent fields is null
     * 2) Collection is null
     * 3) Collection is defined but empty
     * 4) Collection is defined and not empty. In this case offset value is increased by the number of elements in that collection
     */
    private void calculateCollectionOffsets(String[] path, IntList offsets, BooleanList collectionIsNull, IntList definitionLevels, IntList repetitionLevels)
    {
        int maxDefinitionLevel = getMaxDefinitionLevel(path);
        int maxElementRepetitionLevel = getMaxRepetitionLevel(path) + 1;
        int offset = 0;
        offsets.add(offset);
        for (int i = 0; i < definitionLevels.size(); i = getNextCollectionStartIndex(repetitionLevels, maxElementRepetitionLevel, i)) {
            if (isValueNull(isRequired(path), definitionLevels.get(i), maxDefinitionLevel)) {
                //Collection is null
                collectionIsNull.add(true);
                offsets.add(offset);
            }
            else if (definitionLevels.get(i) == maxDefinitionLevel) {
                // Collection is defined but empty
                collectionIsNull.add(false);
                offsets.add(offset);
            }
            else if (definitionLevels.get(i) > maxDefinitionLevel) {
                //Collection is defined and not empty
                collectionIsNull.add(false);
                offset += getCollectionSize(repetitionLevels, maxElementRepetitionLevel, i + 1);
                offsets.add(offset);
            }
        }
    }

    private int getNextCollectionStartIndex(IntList repetitionLevels, int maxRepetitionLevel, int elementIndex)
    {
        do {
            elementIndex++;
        }
        while (hasMoreElements(repetitionLevels, elementIndex) && !isCollectionBeginningMarker(repetitionLevels, maxRepetitionLevel, elementIndex));
        return elementIndex;
    }

    private int getCollectionSize(IntList repetitionLevels, int maxRepetitionLevel, int nextIndex)
    {
        int size = 1; // taking into account, that the method is called for not empty collections only
        while (hasMoreElements(repetitionLevels, nextIndex) && !isCollectionBeginningMarker(repetitionLevels, maxRepetitionLevel, nextIndex)) {
            //Collection elements can be not primitive.
            //Counting only elements which belong to current collection, skipping inner elements of nested collections/structs
            if (repetitionLevels.get(nextIndex) <= maxRepetitionLevel) {
                size++;
            }
            nextIndex++;
        }
        return size;
    }

    private boolean isCollectionBeginningMarker(IntList repetitionLevels, int maxRepetitionLevel, int nextIndex)
    {
        return repetitionLevels.get(nextIndex) < maxRepetitionLevel;
    }

    private boolean hasMoreElements(IntList repetitionLevels, int nextIndex)
    {
        return nextIndex < repetitionLevels.size();
    }

    public Block readPrimitive(ColumnDescriptor columnDescriptor, Type type)
            throws IOException
    {
        return readPrimitive(columnDescriptor, type, new IntArrayList(), new IntArrayList());
    }

    private Block readPrimitive(ColumnDescriptor columnDescriptor, Type type, IntList definitionLevels, IntList repetitionLevels)
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
        return columnReader.readPrimitive(type, definitionLevels, repetitionLevels);
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

    private Block readBlock(String name, Type type, List<String> path, IntList definitionLevels, IntList repetitionLevels)
            throws IOException
    {
        path.add(name);
        Optional<RichColumnDescriptor> descriptor = getDescriptor(fileSchema, requestedSchema, path);
        if (!descriptor.isPresent()) {
            path.remove(path.lastIndexOf(name));
            return RunLengthEncodedBlock.create(type, null, batchSize);
        }

        Block block;
        if (ROW.equals(type.getTypeSignature().getBase())) {
            block = readStruct(type, path, definitionLevels, repetitionLevels);
        }
        else if (MAP.equals(type.getTypeSignature().getBase())) {
            block = readMap(type, path, definitionLevels, repetitionLevels);
        }
        else if (ARRAY.equals(type.getTypeSignature().getBase())) {
            block = readArray(type, path, definitionLevels, repetitionLevels);
        }
        else {
            block = readPrimitive(descriptor.get(), type, definitionLevels, repetitionLevels);
        }
        path.remove(path.lastIndexOf(name));
        return block;
    }

    private String[] toArray(List<String> list)
    {
        return list.toArray(new String[list.size()]);
    }

    private int getMaxDefinitionLevel(String... path)
    {
        return requestedSchema.getMaxDefinitionLevel(path);
    }

    private int getMaxRepetitionLevel(String... path)
    {
        return requestedSchema.getMaxRepetitionLevel(path);
    }

    private boolean isRequired(String... path)
    {
        return requestedSchema.getType(path).getRepetition() == REQUIRED;
    }
}
