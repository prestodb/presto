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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.parquet.ColumnReader;
import com.facebook.presto.parquet.ColumnReaderFactory;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.GroupField;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.ParquetResultVerifierUtils;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.predicate.TupleDomainParquetPredicate;
import com.facebook.presto.parquet.reader.ColumnIndexFilterUtils.OffsetRange;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.crypto.HiddenColumnChunkMetaData;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.openjdk.jol.info.ClassLayout;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.parquet.ParquetValidationUtils.validateParquet;
import static com.facebook.presto.parquet.reader.ListColumnReader.calculateCollectionOffsets;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ParquetReader
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ParquetReader.class).instanceSize();
    private static final int MAX_VECTOR_LENGTH = 1024;
    private static final int INITIAL_BATCH_SIZE = 1;
    private static final int BATCH_SIZE_GROWTH_FACTOR = 2;
    protected final ColumnReader[] verificationColumnReaders;
    protected final ParquetDataSource dataSource;
    private final Optional<InternalFileDecryptor> fileDecryptor;
    private final List<BlockMetaData> blocks;
    private final Optional<List<Long>> firstRowsOfBlocks;
    private final List<PrimitiveColumnIO> columns;
    private final AggregatedMemoryContext systemMemoryContext;
    private final LocalMemoryContext parquetReaderMemoryContext;
    private final LocalMemoryContext pageReaderMemoryContext;
    private final LocalMemoryContext verificationPageReaderMemoryContext;
    private final boolean batchReadEnabled;
    private final boolean enableVerification;
    private final FilterPredicate filter;
    private final ColumnReader[] columnReaders;
    private final long maxReadBlockBytes;
    private final List<ColumnIndexStore> blockIndexStores;
    private final List<RowRanges> blockRowRanges;
    private final Map<ColumnPath, ColumnDescriptor> paths = new HashMap<>();
    private final boolean columnIndexFilterEnabled;
    protected BlockMetaData currentBlockMetadata;
    /**
     * Index in the Parquet file of the first row of the current group
     */
    private Optional<Long> firstRowIndexInGroup = Optional.empty();
    private RowRanges currentGroupRowRanges;
    private long nextRowInGroup;
    private int batchSize;
    private int nextBatchSize = INITIAL_BATCH_SIZE;
    private final long[] maxBytesPerCell;
    private long maxCombinedBytesPerRow;
    private int maxBatchSize = MAX_VECTOR_LENGTH;
    private AggregatedMemoryContext currentRowGroupMemoryContext;
    private int currentBlock;
    private long currentPosition;
    private long currentGroupRowCount;

    public ParquetReader(
            MessageColumnIO messageColumnIO,
            List<BlockMetaData> blocks,
            Optional<List<Long>> firstRowsOfBlocks,
            ParquetDataSource dataSource,
            AggregatedMemoryContext systemMemoryContext,
            DataSize maxReadBlockSize,
            boolean batchReadEnabled,
            boolean enableVerification,
            Predicate parquetPredicate,
            List<ColumnIndexStore> blockIndexStores,
            boolean columnIndexFilterEnabled,
            Optional<InternalFileDecryptor> fileDecryptor)
    {
        this.blocks = blocks;
        this.firstRowsOfBlocks = requireNonNull(firstRowsOfBlocks, "firstRowsOfBlocks is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.parquetReaderMemoryContext = systemMemoryContext.newLocalMemoryContext("ParquetReader");
        this.pageReaderMemoryContext = systemMemoryContext.newLocalMemoryContext("PageReader");
        this.verificationPageReaderMemoryContext = systemMemoryContext.newLocalMemoryContext("PageReader");
        this.maxReadBlockBytes = requireNonNull(maxReadBlockSize, "maxReadBlockSize is null").toBytes();
        this.batchReadEnabled = batchReadEnabled;
        columns = messageColumnIO.getLeaves();
        columnReaders = new ColumnReader[columns.size()];
        this.enableVerification = enableVerification;
        verificationColumnReaders = enableVerification ? new ColumnReader[columns.size()] : null;
        maxBytesPerCell = new long[columns.size()];
        this.blockIndexStores = blockIndexStores;
        this.blockRowRanges = listWithNulls(this.blocks.size());

        firstRowsOfBlocks.ifPresent(firstRows -> {
            checkArgument(blocks.size() == firstRows.size(), "elements of firstRowsOfBlocks must correspond to blocks");
        });

        for (PrimitiveColumnIO column : columns) {
            ColumnDescriptor columnDescriptor = column.getColumnDescriptor();
            this.paths.put(ColumnPath.get(columnDescriptor.getPath()), columnDescriptor);
        }
        if (parquetPredicate != null && columnIndexFilterEnabled && parquetPredicate instanceof TupleDomainParquetPredicate) {
            this.filter = ((TupleDomainParquetPredicate) parquetPredicate).getParquetUserDefinedPredicate();
        }
        else {
            this.filter = null;
        }
        this.currentBlock = -1;
        this.columnIndexFilterEnabled = columnIndexFilterEnabled;
        requireNonNull(fileDecryptor, "fileDecryptor is null");
        this.fileDecryptor = fileDecryptor;
    }

    @Override
    public void close()
            throws IOException
    {
        dataSource.close();
        parquetReaderMemoryContext.close();
        systemMemoryContext.close();
    }

    public long getPosition()
    {
        return currentPosition;
    }

    /**
     * Get the global row index of the first row in the last batch.
     */
    public long lastBatchStartRow()
    {
        long baseIndex = firstRowIndexInGroup.orElseThrow(() -> new IllegalStateException("row index unavailable"));
        return baseIndex + nextRowInGroup - batchSize;
    }

    public int nextBatch()
    {
        if (nextRowInGroup >= currentGroupRowCount && !advanceToNextRowGroup()) {
            return -1;
        }

        batchSize = toIntExact(min(nextBatchSize, maxBatchSize));
        nextBatchSize = min(batchSize * BATCH_SIZE_GROWTH_FACTOR, MAX_VECTOR_LENGTH);
        batchSize = toIntExact(min(batchSize, currentGroupRowCount - nextRowInGroup));

        nextRowInGroup += batchSize;
        currentPosition += batchSize;
        Arrays.stream(columnReaders)
                .forEach(reader -> reader.prepareNextRead(batchSize));

        if (enableVerification) {
            Arrays.stream(verificationColumnReaders)
                    .forEach(reader -> reader.prepareNextRead(batchSize));
        }

        return batchSize;
    }

    public ParquetDataSource getDataSource()
    {
        return dataSource;
    }

    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    private boolean advanceToNextRowGroup()
    {
        currentBlock++;
        if (currentBlock == blocks.size()) {
            return false;
        }
        currentBlockMetadata = blocks.get(currentBlock);
        firstRowIndexInGroup = firstRowsOfBlocks.map(firstRows -> firstRows.get(currentBlock));

        if (filter != null && columnIndexFilterEnabled) {
            ColumnIndexStore columnIndexStore = blockIndexStores.get(currentBlock);
            if (columnIndexStore != null) {
                currentGroupRowRanges = getRowRanges(currentBlock);
                long rowCount = currentGroupRowRanges.rowCount();
                if (rowCount == 0) {
                    return false;
                }
            }
        }

        nextRowInGroup = 0L;
        currentGroupRowCount = currentBlockMetadata.getRowCount();
        initializeColumnReaders();
        return true;
    }

    private ColumnChunk readArray(GroupField field)
            throws IOException
    {
        List<Type> parameters = field.getType().getTypeParameters();
        checkArgument(parameters.size() == 1, "Arrays must have a single type parameter, found %d", parameters.size());
        Field elementField = field.getChildren().get(0).get();
        ColumnChunk columnChunk = readColumnChunk(elementField);
        IntList offsets = new IntArrayList();
        BooleanList valueIsNull = new BooleanArrayList();

        calculateCollectionOffsets(field, offsets, valueIsNull, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        Block arrayBlock = ArrayBlock.fromElementBlock(valueIsNull.size(), Optional.of(valueIsNull.toBooleanArray()), offsets.toIntArray(), columnChunk.getBlock());
        return new ColumnChunk(arrayBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private ColumnChunk readMap(GroupField field)
            throws IOException
    {
        List<Type> parameters = field.getType().getTypeParameters();
        checkArgument(parameters.size() == 2, "Maps must have two type parameters, found %d", parameters.size());
        Block[] blocks = new Block[parameters.size()];

        ColumnChunk columnChunk = readColumnChunk(field.getChildren().get(0).get());
        blocks[0] = columnChunk.getBlock();
        blocks[1] = readColumnChunk(field.getChildren().get(1).get()).getBlock();
        IntList offsets = new IntArrayList();
        BooleanList valueIsNull = new BooleanArrayList();
        calculateCollectionOffsets(field, offsets, valueIsNull, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        Block mapBlock = ((MapType) field.getType()).createBlockFromKeyValue(offsets.size() - 1, Optional.of(valueIsNull.toBooleanArray()), offsets.toIntArray(), blocks[0], blocks[1]);
        return new ColumnChunk(mapBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private ColumnChunk readStruct(GroupField field)
            throws IOException
    {
        List<TypeSignatureParameter> fields = field.getType().getTypeSignature().getParameters();
        Block[] blocks = new Block[fields.size()];
        ColumnChunk columnChunk = null;
        List<Optional<Field>> parameters = field.getChildren();
        for (int i = 0; i < fields.size(); i++) {
            Optional<Field> parameter = parameters.get(i);
            if (parameter.isPresent()) {
                columnChunk = readColumnChunk(parameter.get());
                blocks[i] = columnChunk.getBlock();
            }
        }
        for (int i = 0; i < fields.size(); i++) {
            if (blocks[i] == null) {
                blocks[i] = RunLengthEncodedBlock.create(field.getType(), null, columnChunk.getBlock().getPositionCount());
            }
        }
        BooleanList structIsNull = StructColumnReader.calculateStructOffsets(field, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        boolean[] structIsNullVector = structIsNull.toBooleanArray();
        Block rowBlock = RowBlock.fromFieldBlocks(structIsNullVector.length, Optional.of(structIsNullVector), blocks);
        return new ColumnChunk(rowBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
    }

    private ColumnChunk readPrimitive(PrimitiveField field)
            throws IOException
    {
        ColumnDescriptor columnDescriptor = field.getDescriptor();

        int fieldId = field.getId();
        ColumnReader columnReader = columnReaders[fieldId];
        if (!columnReader.isInitialized()) {
            validateParquet(currentBlockMetadata.getRowCount() > 0, "Row group has 0 rows");
            ColumnChunkMetaData columnChunkMetaData = getColumnChunkMetaData(columnDescriptor);
            long startingPosition = columnChunkMetaData.getStartingPos();
            int columnChunkSize = toIntExact(columnChunkMetaData.getTotalSize());

            if (shouldUseColumnIndex(columnChunkMetaData.getPath())) {
                OffsetIndex offsetIndex = blockIndexStores.get(currentBlock).getOffsetIndex(columnChunkMetaData.getPath());
                OffsetIndex filteredOffsetIndex = ColumnIndexFilterUtils.filterOffsetIndex(offsetIndex, currentGroupRowRanges, blocks.get(currentBlock).getRowCount());
                List<OffsetRange> offsetRanges = ColumnIndexFilterUtils.calculateOffsetRanges(filteredOffsetIndex, columnChunkMetaData, offsetIndex.getOffset(0), startingPosition);
                List<OffsetRange> consecutiveRanges = concatRanges(offsetRanges);
                int consecutiveRangesSize = consecutiveRanges.stream().mapToInt(range -> (int) range.getLength()).sum();
                PageReader pageReader = createPageReader(
                        dataSourceAsInputStream(startingPosition, consecutiveRanges),
                        consecutiveRangesSize,
                        columnChunkMetaData,
                        columnDescriptor,
                        Optional.of(filteredOffsetIndex),
                        pageReaderMemoryContext);

                columnReader.init(pageReader, field, currentGroupRowRanges);

                if (enableVerification) {
                    ColumnReader verificationColumnReader = verificationColumnReaders[field.getId()];
                    PageReader pageReaderVerification = createPageReader(
                            dataSourceAsInputStream(startingPosition, consecutiveRanges),
                            consecutiveRangesSize,
                            columnChunkMetaData,
                            columnDescriptor,
                            Optional.of(filteredOffsetIndex),
                            verificationPageReaderMemoryContext);
                    verificationColumnReader.init(pageReaderVerification, field, currentGroupRowRanges);
                }
            }
            else {
                PageReader pageReader = createPageReader(
                        dataSourceAsInputStream(startingPosition, columnChunkSize),
                        columnChunkSize,
                        columnChunkMetaData,
                        columnDescriptor,
                        Optional.empty(),
                        pageReaderMemoryContext);
                columnReader.init(pageReader, field, null);

                if (enableVerification) {
                    ColumnReader verificationColumnReader = verificationColumnReaders[field.getId()];
                    PageReader pageReaderVerification = createPageReader(
                            dataSourceAsInputStream(startingPosition, columnChunkSize),
                            columnChunkSize,
                            columnChunkMetaData,
                            columnDescriptor,
                            Optional.empty(),
                            verificationPageReaderMemoryContext);
                    verificationColumnReader.init(pageReaderVerification, field, null);
                }
            }
        }

        ColumnChunk columnChunk = columnReader.readNext();
        columnChunk = typeCoercion(columnChunk, field.getDescriptor().getPrimitiveType().getPrimitiveTypeName(), field.getType());

        if (enableVerification) {
            ColumnReader verificationColumnReader = verificationColumnReaders[field.getId()];
            ColumnChunk expected = verificationColumnReader.readNext();
            ParquetResultVerifierUtils.verifyColumnChunks(columnChunk, expected, columnDescriptor.getPath().length > 1, field, dataSource.getId());
        }

        // update max size per primitive column chunk
        long bytesPerCell = columnChunk.getBlock().getSizeInBytes() / batchSize;
        if (maxBytesPerCell[fieldId] < bytesPerCell) {
            // update batch size
            maxCombinedBytesPerRow = maxCombinedBytesPerRow - maxBytesPerCell[fieldId] + bytesPerCell;
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxReadBlockBytes / maxCombinedBytesPerRow)));
            maxBytesPerCell[fieldId] = bytesPerCell;
        }
        return columnChunk;
    }

    private InputStream dataSourceAsInputStream(long startingPosition, List<OffsetRange> offsetRanges)
    {
        List<InputStream> inputStreams = new ArrayList<>();
        for (OffsetRange r : offsetRanges) {
            InputStream inputStream = dataSourceAsInputStream(startingPosition + r.getOffset(), r.getLength());
            inputStreams.add(inputStream);
        }

        return new SequenceInputStream(Collections.enumeration(inputStreams));
    }

    private InputStream dataSourceAsInputStream(long startingPosition, long totalSize)
    {
        InputStream dataSourceAsStream = new InputStream()
        {
            private long readBytes;
            private long currentPosition = startingPosition;

            @Override
            public int read()
            {
                byte[] buffer = new byte[1];
                read(buffer, 0, 1);
                return buffer[0];
            }

            @Override
            public int read(byte[] buffer, int offset, int len)
            {
                if (readBytes >= totalSize) {
                    return 0;
                }
                //Read upto totalSize bytes
                len = (int) Math.min(len, totalSize - readBytes);
                dataSource.readFully(currentPosition, buffer, offset, len);

                //Update references
                currentPosition += len;
                readBytes += len;
                return len;
            }

            @Override
            public int available()
            {
                return (int) (totalSize - readBytes);
            }

            @Override
            public boolean markSupported()
            {
                return false;
            }
        };
        return dataSourceAsStream;
    }

    private boolean shouldUseColumnIndex(ColumnPath path)
    {
        return filter != null &&
                columnIndexFilterEnabled &&
                currentGroupRowRanges != null &&
                currentGroupRowRanges.rowCount() < currentGroupRowCount &&
                blockIndexStores.get(currentBlock) != null &&
                blockIndexStores.get(currentBlock).getColumnIndex(path) != null;
    }

    private PageReader createPageReader(
            InputStream inputStream,
            int columnChunkSize,
            ColumnChunkMetaData columnChunkMetaData,
            ColumnDescriptor columnDescriptor,
            Optional<OffsetIndex> offsetIndex,
            LocalMemoryContext memoryContext)
            throws IOException
    {
        ColumnChunkDescriptor descriptor = new ColumnChunkDescriptor(columnDescriptor, columnChunkMetaData, columnChunkSize);

        //We use the value of the maxReadBlockBytes to set the max size of the data source buffer
        ParquetColumnChunk columnChunk = new ParquetColumnChunk(
                descriptor,
                new ParquetColumnChunk.ColumnChunkBufferedInputStream(requireNonNull(inputStream), min(columnChunkSize, (int) maxReadBlockBytes)),
                offsetIndex,
                memoryContext);
        return createPageReaderInternal(columnDescriptor, columnChunk, memoryContext);
    }

    private PageReader createPageReaderInternal(ColumnDescriptor columnDescriptor, ParquetColumnChunk columnChunk, LocalMemoryContext memoryContext)
            throws IOException
    {
        if (!isEncryptedColumn(fileDecryptor, columnDescriptor)) {
            return columnChunk.buildPageReader(Optional.empty(), -1, -1);
        }

        int columnOrdinal = fileDecryptor.get().getColumnSetup(ColumnPath.get(columnChunk.getDescriptor().getColumnDescriptor().getPath())).getOrdinal();
        return columnChunk.buildPageReader(fileDecryptor, currentBlock, columnOrdinal);
    }

    private boolean isEncryptedColumn(Optional<InternalFileDecryptor> fileDecryptor, ColumnDescriptor columnDescriptor)
    {
        ColumnPath columnPath = ColumnPath.get(columnDescriptor.getPath());
        return fileDecryptor.isPresent() && !fileDecryptor.get().plaintextFile() && fileDecryptor.get().getColumnSetup(columnPath).isEncrypted();
    }

    private ColumnChunkMetaData getColumnChunkMetaData(ColumnDescriptor columnDescriptor)
            throws IOException
    {
        for (ColumnChunkMetaData metadata : currentBlockMetadata.getColumns()) {
            if (!HiddenColumnChunkMetaData.isHiddenColumn(metadata) && metadata.getPath().equals(ColumnPath.get(columnDescriptor.getPath()))) {
                return metadata;
            }
        }
        throw new ParquetCorruptionException("Metadata is missing for column: %s", columnDescriptor);
    }

    private void initializeColumnReaders()
    {
        for (PrimitiveColumnIO columnIO : columns) {
            RichColumnDescriptor column = new RichColumnDescriptor(columnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
            columnReaders[columnIO.getId()] = ColumnReaderFactory.createReader(column, batchReadEnabled);

            if (enableVerification) {
                verificationColumnReaders[columnIO.getId()] = ColumnReaderFactory.createReader(column, false);
            }
        }
    }

    public Block readBlock(Field field)
            throws IOException
    {
        return readColumnChunk(field).getBlock();
    }

    int getLevel(Type rootType, Type leafType)
    {
        int level = 0;
        Type currentType = rootType;
        while (!currentType.equals(leafType)) {
            currentType = currentType.getTypeParameters().get(0);
            ++level;
        }
        return level;
    }
    /**
     * Directly return a nested column as block. This is when subfield within a struct is pushed down
     * to the reader. As we are reading the nested column, the column Reader only returns null or non-null
     * values. If the parent of the subfield is null, the Block doesn't contain any values. We need to look
     * at the definition levels to insert nulls in places where the parent of subfield is null.
     * Ex:
     *    Schema: struct(int a) msg
     *    Values in File:
     *            Rows: [msg(a=1)], [msg(a=2)], [msg(a=null)], [null]
     *    Column reader returned ColumnChunk for `msg.a` contains
     *        Block: count = 3, [1, 2, null] (even though there are 4 rows)
     *        Definition Levels: count = 4, [2, 2, 1, 0]
     *        Repetition Levels: count = 4, [0, 0, 0, 0]
     *
     *    This function converts the Block to: count = 4, [1, 2, null, null]
     * @param field
     * @return
     * @throws IOException
     */
    public Block readPushedDownSubfieldBlock(Field field, Type type)
            throws IOException
    {
        checkArgument(field.isPushedDownSubfield(), "input is expected to be a pushed down subfield");
        ColumnChunk columnChunk = readColumnChunk(field);

        String typeBase = field.getType().getTypeSignature().getBase();
        if (ROW.equals(typeBase) || MAP.equals(typeBase) || ARRAY.equals(typeBase)) {
            Block block = columnChunk.getBlock();

            int size = block.getPositionCount();
            int level = getLevel(field.getType(), type);
            boolean[] isNulls = new boolean[size];

            for (int currentLevel = 0; currentLevel < level; ++currentLevel) {
                ColumnarRow rowBlock = ColumnarRow.toColumnarRow(block);
                int index = 0;
                for (int j = 0; j < size; ++j) {
                    if (!isNulls[j]) {
                        isNulls[j] = rowBlock.isNull(index);
                        ++index;
                    }
                }
                block = rowBlock.getField(0);
            }
            BlockBuilder blockBuilder = type.createBlockBuilder(null, size);
            int currentPosition = 0;
            for (int i = 0; i < size; ++i) {
                if (isNulls[i]) {
                    blockBuilder.appendNull();
                }
                else {
                    Preconditions.checkArgument(currentPosition < block.getPositionCount(), "current position cannot exceed total position count");
                    type.appendTo(block, currentPosition, blockBuilder);
                    currentPosition++;
                }
            }
            return blockBuilder.build();
        }

        BlockBuilder blockBuilder = field.getType().createBlockBuilder(null, nextBatchSize);
        int valueIndex = 0;
        for (int i = 0; i < columnChunk.getDefinitionLevels().length; i++) {
            int definitionLevel = columnChunk.getDefinitionLevels()[i];
            if (definitionLevel < field.getDefinitionLevel()) {
                blockBuilder.appendNull();
                if (definitionLevel == field.getDefinitionLevel() - 1) {
                    valueIndex++;
                }
            }
            else {
                field.getType().appendTo(columnChunk.getBlock(), valueIndex, blockBuilder);
                valueIndex++;
            }
        }
        Block block = blockBuilder.build();
        return block;
    }

    private ColumnChunk readColumnChunk(Field field)
            throws IOException
    {
        ColumnChunk columnChunk;
        if (ROW.equals(field.getType().getTypeSignature().getBase())) {
            columnChunk = readStruct((GroupField) field);
        }
        else if (MAP.equals(field.getType().getTypeSignature().getBase())) {
            columnChunk = readMap((GroupField) field);
        }
        else if (ARRAY.equals(field.getType().getTypeSignature().getBase())) {
            columnChunk = readArray((GroupField) field);
        }
        else {
            columnChunk = readPrimitive((PrimitiveField) field);
        }

        parquetReaderMemoryContext.setBytes(getRetainedSizeInBytes());
        return columnChunk;
    }

    private long getRetainedSizeInBytes()
    {
        long sizeInBytes = INSTANCE_SIZE;
        for (int i = 0; i < columnReaders.length; i++) {
            sizeInBytes += columnReaders[i] == null ? 0 : columnReaders[i].getRetainedSizeInBytes();
            if (verificationColumnReaders != null) {
                sizeInBytes += verificationColumnReaders[i] == null ? 0 : verificationColumnReaders[i].getRetainedSizeInBytes();
            }
        }
        sizeInBytes += sizeOf(maxBytesPerCell);
        return sizeInBytes;
    }

    /**
     * The reader returns the Block type (LongArrayBlock or IntArrayBlock) based on the physical type of the column in Parquet file, but
     * operators after scan expect output block type based on the metadata defined in Hive table. This causes issues as the operators call
     * methods that are based on the expected output block type which the returned block type may not have implemented.
     * To overcome this issue rewrite the block.
     */
    private static ColumnChunk typeCoercion(ColumnChunk columnChunk, PrimitiveTypeName physicalDataType, Type outputType)
    {
        Block newBlock = null;
        if (SMALLINT.equals(outputType) || TINYINT.equals(outputType)) {
            if (columnChunk.getBlock() instanceof IntArrayBlock) {
                newBlock = rewriteIntegerArrayBlock((IntArrayBlock) columnChunk.getBlock(), outputType);
            }
            else if (columnChunk.getBlock() instanceof LongArrayBlock) {
                newBlock = rewriteLongArrayBlock((LongArrayBlock) columnChunk.getBlock(), outputType);
            }
        }
        else if (INTEGER.equals(outputType) && physicalDataType == PrimitiveTypeName.INT64) {
            if (columnChunk.getBlock() instanceof LongArrayBlock) {
                newBlock = rewriteLongArrayBlock((LongArrayBlock) columnChunk.getBlock(), outputType);
            }
        }
        else if (BIGINT.equals(outputType) && physicalDataType == PrimitiveTypeName.INT32) {
            if (columnChunk.getBlock() instanceof IntArrayBlock) {
                newBlock = rewriteIntegerArrayBlock((IntArrayBlock) columnChunk.getBlock(), outputType);
            }
        }

        if (newBlock != null) {
            return new ColumnChunk(newBlock, columnChunk.getDefinitionLevels(), columnChunk.getRepetitionLevels());
        }

        return columnChunk;
    }

    private static Block rewriteIntegerArrayBlock(IntArrayBlock intArrayBlock, Type targetType)
    {
        int positionCount = intArrayBlock.getPositionCount();
        BlockBuilder newBlockBuilder = targetType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (intArrayBlock.isNull(position)) {
                newBlockBuilder.appendNull();
            }
            else {
                targetType.writeLong(newBlockBuilder, intArrayBlock.getInt(position));
            }
        }

        return newBlockBuilder.build();
    }

    private static Block rewriteLongArrayBlock(LongArrayBlock longArrayBlock, Type targetType)
    {
        int positionCount = longArrayBlock.getPositionCount();
        BlockBuilder newBlockBuilder = targetType.createBlockBuilder(null, positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (longArrayBlock.isNull(position)) {
                newBlockBuilder.appendNull();
            }
            else {
                targetType.writeLong(newBlockBuilder, longArrayBlock.getLong(position, 0));
            }
        }

        return newBlockBuilder.build();
    }

    private static <T> List<T> listWithNulls(int size)
    {
        return Stream.generate(() -> (T) null).limit(size).collect(Collectors.toCollection(ArrayList<T>::new));
    }

    private RowRanges getRowRanges(int blockIndex)
    {
        assert filter != null;

        RowRanges rowRanges = blockRowRanges.get(blockIndex);
        if (rowRanges == null) {
            rowRanges = ColumnIndexFilter.calculateRowRanges(FilterCompat.get(filter), blockIndexStores.get(blockIndex),
                    paths.keySet(), blocks.get(blockIndex).getRowCount());
            blockRowRanges.set(blockIndex, rowRanges);
        }
        return rowRanges;
    }

    private List<OffsetRange> concatRanges(List<OffsetRange> offsetRanges)
    {
        List<OffsetRange> pageRanges = new ArrayList<>();
        OffsetRange currentParts = null;
        for (OffsetRange range : offsetRanges) {
            long startPosition = range.getOffset();
            // first part or not consecutive => new list
            if (currentParts == null || currentParts.endPos() != startPosition) {
                currentParts = new OffsetRange(startPosition, 0);
            }
            pageRanges.add(currentParts);
            currentParts.extendLength(range.getLength());
        }
        return pageRanges;
    }
}
