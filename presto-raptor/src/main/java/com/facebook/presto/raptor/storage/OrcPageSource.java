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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.BooleanVector;
import com.facebook.presto.orc.DoubleVector;
import com.facebook.presto.orc.LongVector;
import com.facebook.presto.orc.OrcDataSource;
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
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.Vector.MAX_VECTOR_LENGTH;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.wrappedBooleanArray;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static io.airlift.slice.Slices.wrappedIntArray;
import static io.airlift.slice.Slices.wrappedLongArray;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class OrcPageSource
        implements ConnectorPageSource
{
    private final OrcRecordReader recordReader;
    private final OrcDataSource orcDataSource;

    private final List<Long> columnIds;
    private final List<Type> types;

    private final Block[] constantBlocks;
    private final int[] columnIndexes;

    private long completedBytes;

    private int batchId;
    private boolean closed;

    public OrcPageSource(
            OrcRecordReader recordReader,
            OrcDataSource orcDataSource,
            List<Long> columnIds,
            List<Type> columnTypes,
            List<Integer> columnIndexes)
    {
        this.recordReader = checkNotNull(recordReader, "recordReader is null");
        this.orcDataSource = checkNotNull(orcDataSource, "orcDataSource is null");

        checkArgument(columnIds.size() == columnTypes.size(), "ids and types mismatch");
        checkArgument(columnIds.size() == columnIndexes.size(), "ids and indexes mismatch");
        int size = columnIds.size();

        this.columnIds = ImmutableList.copyOf(columnIds);
        this.types = ImmutableList.copyOf(columnTypes);

        this.constantBlocks = new Block[size];
        this.columnIndexes = new int[size];

        for (int i = 0; i < size; i++) {
            this.columnIndexes[i] = columnIndexes.get(i);
            if (this.columnIndexes[i] == -1) {
                constantBlocks[i] = buildNullBlock(columnTypes.get(i));
            }
        }
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

            Block[] blocks = new Block[columnIndexes.length];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                Type type = types.get(fieldId);
                if (constantBlocks[fieldId] != null) {
                    blocks[fieldId] = constantBlocks[fieldId].getRegion(0, batchSize);
                }
                else if (BOOLEAN.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(BOOLEAN.getFixedSize(), batchSize, new LazyBooleanBlockLoader(columnIndexes[fieldId], batchSize));
                }
                else if (DATE.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(DATE.getFixedSize(), batchSize, new LazyIntBlockLoader(columnIndexes[fieldId], batchSize));
                }
                else if (BIGINT.equals(type) || TIMESTAMP.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(((FixedWidthType) type).getFixedSize(), batchSize, new LazyLongBlockLoader(columnIndexes[fieldId], batchSize));
                }
                else if (DOUBLE.equals(type)) {
                    blocks[fieldId] = new LazyFixedWidthBlock(DOUBLE.getFixedSize(), batchSize, new LazyDoubleBlockLoader(columnIndexes[fieldId], batchSize));
                }
                else if (VARCHAR.equals(type) || VARBINARY.equals(type)) {
                    blocks[fieldId] = new LazySliceArrayBlock(batchSize, new LazySliceBlockLoader(columnIndexes[fieldId], batchSize));
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type);
                }
            }

            updateCompletedBytes();

            return new Page(batchSize, blocks);
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        closed = true;

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnNames", columnIds)
                .add("types", types)
                .toString();
    }

    private void closeWithSuppression(Throwable throwable)
    {
        checkNotNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            throwable.addSuppressed(e);
        }
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private void updateCompletedBytes()
    {
        long newCompletedBytes = (long) (recordReader.getSplitLength() * recordReader.getProgress());
        completedBytes = min(recordReader.getSplitLength(), max(completedBytes, newCompletedBytes));
    }

    private static Block buildNullBlock(Type type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        for (int i = 0; i < MAX_VECTOR_LENGTH; i++) {
            blockBuilder.appendNull();
        }
        return blockBuilder.build();
    }

    private final class LazyBooleanBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int columnIndex;

        public LazyBooleanBlockLoader(int columnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.columnIndex = columnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                BooleanVector vector = new BooleanVector(batchSize);
                recordReader.readVector(columnIndex, vector);
                block.setNullVector(vector.isNull);
                block.setRawSlice(wrappedBooleanArray(vector.vector, 0, batchSize));
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, e);
            }
        }
    }

    private final class LazyIntBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int columnIndex;

        public LazyIntBlockLoader(int columnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.columnIndex = columnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                // TODO to add an ORC int vector
                LongVector vector = new LongVector(batchSize);
                recordReader.readVector(columnIndex, vector);
                block.setNullVector(vector.isNull);

                int[] ints = new int[batchSize];
                for (int i = 0; i < batchSize; i++) {
                    ints[i] = (int) vector.vector[i];
                }

                block.setRawSlice(wrappedIntArray(ints, 0, batchSize));
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, e);
            }
        }
    }

    private final class LazyLongBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int columnIndex;

        public LazyLongBlockLoader(int columnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.columnIndex = columnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                LongVector vector = new LongVector(batchSize);
                recordReader.readVector(columnIndex, vector);
                block.setNullVector(vector.isNull);
                block.setRawSlice(wrappedLongArray(vector.vector, 0, batchSize));
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, e);
            }
        }
    }

    private final class LazyDoubleBlockLoader
            implements LazyBlockLoader<LazyFixedWidthBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int columnIndex;

        public LazyDoubleBlockLoader(int columnIndex, int batchSize)
        {
            this.batchSize = batchSize;
            this.columnIndex = columnIndex;
        }

        @Override
        public void load(LazyFixedWidthBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                DoubleVector vector = new DoubleVector(batchSize);
                recordReader.readVector(columnIndex, vector);
                block.setNullVector(vector.isNull);
                block.setRawSlice(wrappedDoubleArray(vector.vector, 0, batchSize));
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, e);
            }
        }
    }

    private final class LazySliceBlockLoader
            implements LazyBlockLoader<LazySliceArrayBlock>
    {
        private final int expectedBatchId = batchId;
        private final int batchSize;
        private final int columnIndex;

        public LazySliceBlockLoader(int columnIndex, int batchSize)
        {
            this.columnIndex = columnIndex;
            this.batchSize = batchSize;
        }

        @Override
        public void load(LazySliceArrayBlock block)
        {
            checkState(batchId == expectedBatchId);
            try {
                SliceVector vector = new SliceVector(batchSize);
                recordReader.readVector(columnIndex, vector);
                block.setValues(vector.vector);
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, e);
            }
        }
    }
}
