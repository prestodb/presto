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
import com.facebook.presto.spi.block.FixedWidthBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
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
import static com.google.common.collect.Maps.uniqueIndex;

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
                    BooleanVector vector = new BooleanVector();
                    recordReader.readVector(hiveColumnIndexes[fieldId], vector);
                    blocks[fieldId] = new FixedWidthBlock(BOOLEAN.getFixedSize(), batchSize, Slices.wrappedBooleanArray(vector.vector, 0, batchSize), vector.isNull);
                }
                else if (DATE.equals(type)) {
                    LongVector vector = new LongVector();
                    recordReader.readVector(hiveColumnIndexes[fieldId], vector);
                    for (int i = 0; i < batchSize; i++) {
                        vector.vector[i] *= MILLIS_IN_DAY;
                    }
                    blocks[fieldId] = new FixedWidthBlock(DATE.getFixedSize(), batchSize, Slices.wrappedLongArray(vector.vector, 0, batchSize), vector.isNull);
                }
                else if (BIGINT.equals(type) || TIMESTAMP.equals(type)) {
                    LongVector vector = new LongVector();
                    recordReader.readVector(hiveColumnIndexes[fieldId], vector);
                    blocks[fieldId] = new FixedWidthBlock(((FixedWidthType) type).getFixedSize(), batchSize, Slices.wrappedLongArray(vector.vector, 0, batchSize), vector.isNull);
                }
                else if (DOUBLE.equals(type)) {
                    DoubleVector vector = new DoubleVector();
                    recordReader.readVector(hiveColumnIndexes[fieldId], vector);
                    blocks[fieldId] = new FixedWidthBlock(DOUBLE.getFixedSize(), batchSize, Slices.wrappedDoubleArray(vector.vector, 0, batchSize), vector.isNull);
                }
                else if (VARCHAR.equals(type) || VARBINARY.equals(type)) {
                    SliceVector vector = new SliceVector();
                    recordReader.readVector(hiveColumnIndexes[fieldId], vector);
                    blocks[fieldId] = new SliceArrayBlock(batchSize, vector.slice);
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
}
