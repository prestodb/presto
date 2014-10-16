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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.HiveUtil;
import com.facebook.presto.orc.BooleanVector;
import com.facebook.presto.orc.DoubleVector;
import com.facebook.presto.orc.LongVector;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.SliceVector;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.LazyFixedWidthBlock;
import com.facebook.presto.spi.block.LazySliceArrayBlock;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveUtil.parseHiveTimestamp;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static com.facebook.presto.orc.Vector.MAX_VECTOR_LENGTH;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.slice.Slices.wrappedBooleanArray;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static io.airlift.slice.Slices.wrappedLongArray;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class OrcPageSource
        implements ConnectorPageSource
{
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private final OrcRecordReader recordReader;
    private final HdfsOrcDataSource orcDataSource;

    private final List<String> columnNames;
    private final List<Type> types;
    private final boolean[] isStructuralType;

    private final Block[] constantBlocks;
    private final int[] hiveColumnIndexes;

    private long completedBytes;

    private int batchId;
    private boolean closed;

    public OrcPageSource(
            OrcRecordReader recordReader,
            HdfsOrcDataSource orcDataSource,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        this.recordReader = checkNotNull(recordReader, "recordReader is null");
        this.orcDataSource = checkNotNull(orcDataSource, "orcDataSource is null");

        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(checkNotNull(partitionKeys, "partitionKeys is null"), HivePartitionKey.nameGetter());

        int size = checkNotNull(columns, "columns is null").size();

        this.isStructuralType = new boolean[size];

        this.constantBlocks = new Block[size];
        this.hiveColumnIndexes = new int[size];

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());

            namesBuilder.add(name);
            typesBuilder.add(type);

            String typeBase = column.getTypeSignature().getBase();
            isStructuralType[columnIndex] = ARRAY.equals(typeBase) || MAP.equals(typeBase);

            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();

            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(name);
                checkArgument(partitionKey != null, "No value provided for partition key %s", name);

                byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);

                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());

                if (HiveUtil.isHiveNull(bytes)) {
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        blockBuilder.appendNull();
                    }
                }
                else if (type.equals(BOOLEAN)) {
                    boolean value;
                    if (isTrue(bytes, 0, bytes.length)) {
                        value = true;
                    }
                    else if (isFalse(bytes, 0, bytes.length)) {
                        value = false;
                    }
                    else {
                        String valueString = new String(bytes, Charsets.UTF_8);
                        throw new IllegalArgumentException(String.format("Invalid partition value '%s' for BOOLEAN partition key %s", valueString, name));
                    }
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        BOOLEAN.writeBoolean(blockBuilder, value);
                    }
                }
                else if (type.equals(BIGINT)) {
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for BIGINT partition key %s", name));
                    }
                    long value = parseLong(bytes, 0, bytes.length);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        BIGINT.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(DOUBLE)) {
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for DOUBLE partition key %s", name));
                    }
                    double value = parseDouble(bytes, 0, bytes.length);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        DOUBLE.writeDouble(blockBuilder, value);
                    }
                }
                else if (type.equals(VARCHAR)) {
                    Slice value = Slices.wrappedBuffer(bytes);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        VARCHAR.writeSlice(blockBuilder, value);
                    }
                }
                else if (type.equals(DATE)) {
                    long value = ISODateTimeFormat.date().withZone(DateTimeZone.UTC).parseMillis(partitionKey.getValue());
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        DATE.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(TIMESTAMP)) {
                    long value = parseHiveTimestamp(partitionKey.getValue(), hiveStorageTimeZone);
                    for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                        DATE.writeLong(blockBuilder, value);
                    }
                }
                else {
                    throw new UnsupportedOperationException("Partition key " + name + " had an unsupported column type " + type);
                }

                constantBlocks[columnIndex] = blockBuilder.build();
            }
            else if (!recordReader.isColumnPresent(column.getHiveColumnIndex())) {
                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
                for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
                    blockBuilder.appendNull();
                }
                constantBlocks[columnIndex] = blockBuilder.build();
            }
        }
        types = typesBuilder.build();
        columnNames = namesBuilder.build();
    }

    @Override
    public long getTotalBytes()
    {
        return recordReader.getSplitLength();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        try {
            batchId++;
            int batchSize = recordReader.nextBatch();
            if (batchSize <= 0) {
                close();
                return null;
            }

            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                Type type = types.get(fieldId);
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                }
                else if (BOOLEAN.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(BOOLEAN.getFixedSize(), batchSize, new LazyBooleanBlockLoader(hiveColumnIndexes[fieldId], batchSize));
                }
                else if (DATE.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(DATE.getFixedSize(), batchSize, new LazyDateBlockLoader(hiveColumnIndexes[fieldId], batchSize));
                }
                else if (BIGINT.equals(type) || TIMESTAMP.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(((FixedWidthType) type).getFixedSize(), batchSize, new LazyLongBlockLoader(hiveColumnIndexes[fieldId], batchSize));
                }
                else if (DOUBLE.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(DOUBLE.getFixedSize(), batchSize, new LazyDoubleBlockLoader(hiveColumnIndexes[fieldId], batchSize));
                }
                else if (VARCHAR.equals(type) || VARBINARY.equals(type) || isStructuralType[fieldId]) {
                    blocks[fieldId] = new LazySliceArrayBlock(batchSize, new LazySliceBlockLoader(hiveColumnIndexes[fieldId]));
                }
                else {
                    throw new UnsupportedOperationException("Unsupported column type: " + type);
                }
            }
            Page page = new Page(batchSize, blocks);

            long newCompletedBytes = (long) (recordReader.getSplitLength() * recordReader.getProgress());
            completedBytes = min(recordReader.getSplitLength(), max(completedBytes, newCompletedBytes));

            return page;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("columnNames", columnNames)
                .add("types", types)
                .toString();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        checkNotNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            throwable.addSuppressed(e);
        }
    }

    private final class LazyBooleanBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int hiveColumnIndex;

        public LazyBooleanBlockLoader(int hiveColumnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.hiveColumnIndex = hiveColumnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                BooleanVector vector = new BooleanVector();
                recordReader.readVector(hiveColumnIndex, vector);
                block.setNullVector(vector.isNull);
                block.setRawSlice(wrappedBooleanArray(vector.vector, 0, batchSize));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private final class LazyDateBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int hiveColumnIndex;

        public LazyDateBlockLoader(int hiveColumnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.hiveColumnIndex = hiveColumnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                LongVector vector = new LongVector();
                recordReader.readVector(hiveColumnIndex, vector);
                for (int i = 0; i < batchSize; i++) {
                    vector.vector[i] *= MILLIS_IN_DAY;
                }
                block.setNullVector(vector.isNull);
                block.setRawSlice(wrappedLongArray(vector.vector, 0, batchSize));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private final class LazyLongBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int hiveColumnIndex;

        public LazyLongBlockLoader(int hiveColumnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.hiveColumnIndex = hiveColumnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                LongVector vector = new LongVector();
                recordReader.readVector(hiveColumnIndex, vector);
                block.setNullVector(vector.isNull);
                block.setRawSlice(wrappedLongArray(vector.vector, 0, batchSize));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private final class LazyDoubleBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;

        private final int batchSize;
        private final int hiveColumnIndex;

        public LazyDoubleBlockLoader(int hiveColumnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.hiveColumnIndex = hiveColumnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                DoubleVector vector = new DoubleVector();
                recordReader.readVector(hiveColumnIndex, vector);
                block.setNullVector(vector.isNull);
                block.setRawSlice(wrappedDoubleArray(vector.vector, 0, batchSize));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private final class LazySliceBlockLoader
            implements LazyBlockLoader<LazySliceArrayBlock>
    {
        private final int expectedBatchId = batchId;

        private final int hiveColumnIndex;

        public LazySliceBlockLoader(int hiveColumnIndex)
        {
            this.hiveColumnIndex = hiveColumnIndex;
        }

        @Override
        public void load(LazySliceArrayBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                SliceVector vector = new SliceVector();
                recordReader.readVector(hiveColumnIndex, vector);
                block.setValues(vector.vector);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
