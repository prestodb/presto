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
package com.facebook.presto.hive;

import com.facebook.presto.hive.orc.BooleanVector;
import com.facebook.presto.hive.orc.DoubleVector;
import com.facebook.presto.hive.orc.LongVector;
import com.facebook.presto.hive.orc.OrcRecordReader;
import com.facebook.presto.hive.orc.SliceVector;
import com.facebook.presto.hive.orc.Vector;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyFixedWidthBlock;
import com.facebook.presto.spi.block.LazyFixedWidthBlock.LazyFixedWidthBlockLoader;
import com.facebook.presto.spi.block.LazySliceArrayBlock;
import com.facebook.presto.spi.block.LazySliceArrayBlock.LazySliceArrayBlockLoader;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
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

class OrcDataStream
        implements Operator, Closeable
{
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private final OperatorContext operatorContext;
    private final OrcRecordReader recordReader;

    private final List<String> columnNames;
    private final List<Type> types;

    private final Block[] partitionKeyBlocks;
    private final int[] hiveColumnIndexes;

    private int batchId;
    private boolean closed;

    public OrcDataStream(
            OperatorContext operatorContext,
            OrcRecordReader recordReader,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.recordReader = checkNotNull(recordReader, "recordReader is null");

        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(checkNotNull(partitionKeys, "partitionKeys is null"), HivePartitionKey.nameGetter());

        int size = checkNotNull(columns, "columns is null").size();

        this.partitionKeyBlocks = new Block[size];
        this.hiveColumnIndexes = new int[size];

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);

            String name = column.getName();
            Type type = column.getType();

            namesBuilder.add(name);
            typesBuilder.add(type);

            hiveColumnIndexes[columnIndex] = column.getHiveColumnIndex();

            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(name);
                checkArgument(partitionKey != null, "No value provided for partition key %s", name);

                byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);

                BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());

                if (type.equals(BOOLEAN)) {
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
                    for (int i = 0; i < Vector.MAX_VECTOR_LENGTH; i++) {
                        BOOLEAN.writeBoolean(blockBuilder, value);
                    }
                }
                else if (type.equals(BIGINT)) {
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for BIGINT partition key %s", name));
                    }
                    long value = parseLong(bytes, 0, bytes.length);
                    for (int i = 0; i < Vector.MAX_VECTOR_LENGTH; i++) {
                        BIGINT.writeLong(blockBuilder, value);
                    }
                }
                else if (type.equals(DOUBLE)) {
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for DOUBLE partition key %s", name));
                    }
                    double value = parseDouble(bytes, 0, bytes.length);
                    for (int i = 0; i < Vector.MAX_VECTOR_LENGTH; i++) {
                        DOUBLE.writeDouble(blockBuilder, value);
                    }
                }
                else if (type.equals(VARCHAR)) {
                    Slice value = Slices.wrappedBuffer(bytes);
                    for (int i = 0; i < Vector.MAX_VECTOR_LENGTH; i++) {
                        VARCHAR.writeSlice(blockBuilder, value);
                    }
                }
                else if (type.equals(DATE)) {
                    long value = ISODateTimeFormat.date().withZone(DateTimeZone.UTC).parseMillis(partitionKey.getValue());
                    for (int i = 0; i < Vector.MAX_VECTOR_LENGTH; i++) {
                        DATE.writeLong(blockBuilder, value);
                    }
                }
                else {
                    throw new UnsupportedOperationException("Partition key " + name + " had an unsupported column type " + type);
                }

                partitionKeyBlocks[columnIndex] = blockBuilder.build();
            }
        }
        types = typesBuilder.build();
        columnNames = namesBuilder.build();
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        try {
            long startReadTimeNanos = System.nanoTime();

            batchId++;
            int batchSize = recordReader.nextBatch();
            if (batchSize <= 0) {
                close();
                return null;
            }

            Block[] blocks = new Block[hiveColumnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                Type type = types.get(fieldId);
                if (partitionKeyBlocks[fieldId] != null) {
                    blocks[fieldId] = partitionKeyBlocks[fieldId].getRegion(0, batchSize);
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
                else if (VARCHAR.equals(type) || VARBINARY.equals(type)) {
                    blocks[fieldId] = new LazySliceArrayBlock(batchSize, new LazySliceBlockLoader(hiveColumnIndexes[fieldId]));
                }
                else {
                    throw new UnsupportedOperationException("Unsupported column type: " + type);
                }
            }
            Page page = new Page(batchSize, blocks);
            operatorContext.recordGeneratedInput(page.getDataSize(), page.getPositionCount(), System.nanoTime() - startReadTimeNanos);
            return page;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR.toErrorCode(), e);
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
            implements LazyFixedWidthBlockLoader
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
            implements LazyFixedWidthBlockLoader
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
            implements LazyFixedWidthBlockLoader
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
            implements LazyFixedWidthBlockLoader
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
            implements LazySliceArrayBlockLoader
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
                block.setValues(vector.slice);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
