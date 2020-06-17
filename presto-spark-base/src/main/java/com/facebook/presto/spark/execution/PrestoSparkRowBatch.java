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
package com.facebook.presto.spark.execution;

import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;
import scala.Tuple2;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRowBatch
        implements PrestoSparkBufferedResult
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PrestoSparkRowBatch.class).instanceSize();

    private static final int DEFAULT_TARGET_SIZE = 1024 * 1024;
    private static final int DEFAULT_EXPECTED_ROWS_COUNT = 10000;
    private static final int REPLICATED_ROW_PARTITION_ID = -1;

    private final int partitionCount;
    private final int rowCount;
    private final byte[] rowData;
    private final int[] rowPartitions;
    private final int[] rowSizes;
    private final long retainedSizeInBytes;

    private PrestoSparkRowBatch(int partitionCount, int rowCount, byte[] rowData, int[] rowPartitions, int[] rowSizes)
    {
        this.partitionCount = partitionCount;
        this.rowCount = rowCount;
        this.rowData = requireNonNull(rowData, "rowData is null");
        this.rowPartitions = requireNonNull(rowPartitions, "rowPartitions is null");
        this.rowSizes = requireNonNull(rowSizes, "rowSizes is null");
        this.retainedSizeInBytes = INSTANCE_SIZE
                + sizeOf(rowData)
                + sizeOf(rowPartitions)
                + sizeOf(rowSizes);
    }

    public RowTupleSupplier createRowTupleSupplier()
    {
        return new RowTupleSupplier(partitionCount, rowCount, rowData, rowPartitions, rowSizes);
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    public static PrestoSparkRowBatchBuilder builder(int partitionCount)
    {
        return new PrestoSparkRowBatchBuilder(partitionCount, DEFAULT_TARGET_SIZE, DEFAULT_EXPECTED_ROWS_COUNT);
    }

    public static PrestoSparkRowBatchBuilder builder(int partitionCount, int targetSizeInBytes, int expectedRowsCount)
    {
        return new PrestoSparkRowBatchBuilder(partitionCount, targetSizeInBytes, expectedRowsCount);
    }

    public static class PrestoSparkRowBatchBuilder
    {
        private static final int BUILDER_INSTANCE_SIZE = ClassLayout.parseClass(PrestoSparkRowBatchBuilder.class).instanceSize();

        private final int partitionCount;
        private final int targetSizeInBytes;
        private final DynamicSliceOutput sliceOutput;
        private int[] rowSizes;
        private int[] rowPartitions;
        private int rowCount;

        private int currentRowOffset;
        private boolean openEntry;

        private PrestoSparkRowBatchBuilder(int partitionCount, int targetSizeInBytes, int expectedRowsCount)
        {
            checkArgument(partitionCount > 0, "partitionCount must be greater then zero: %s", partitionCount);
            this.partitionCount = partitionCount;
            this.targetSizeInBytes = targetSizeInBytes;
            sliceOutput = new DynamicSliceOutput((int) (targetSizeInBytes * 1.2f));
            rowSizes = new int[expectedRowsCount];
            rowPartitions = new int[expectedRowsCount];
        }

        public long getRetainedSizeInBytes()
        {
            return BUILDER_INSTANCE_SIZE + sliceOutput.getRetainedSize() + sizeOf(rowSizes) + sizeOf(rowPartitions);
        }

        public boolean isFull()
        {
            return sliceOutput.size() >= targetSizeInBytes;
        }

        public boolean isEmpty()
        {
            return rowCount == 0;
        }

        public SliceOutput beginRowEntry()
        {
            checkState(!openEntry, "previous entry must be closed before creating a new entry");
            openEntry = true;
            currentRowOffset = sliceOutput.size();
            return sliceOutput;
        }

        public void closeEntryForNonReplicatedRow(int partition)
        {
            closeEntry(partition);
        }

        public void closeEntryForReplicatedRow()
        {
            closeEntry(REPLICATED_ROW_PARTITION_ID);
        }

        private void closeEntry(int partitionId)
        {
            checkState(openEntry, "entry must be opened first");
            openEntry = false;

            rowSizes = ensureCapacity(rowSizes, rowCount + 1);
            rowSizes[rowCount] = sliceOutput.size() - currentRowOffset;

            rowPartitions = ensureCapacity(rowPartitions, rowCount + 1);
            rowPartitions[rowCount] = partitionId;

            rowCount++;
        }

        private static int[] ensureCapacity(int[] array, int capacity)
        {
            if (array.length >= capacity) {
                return array;
            }
            return Arrays.copyOf(array, capacity * 2);
        }

        public PrestoSparkRowBatch build()
        {
            checkState(!openEntry, "entry must be closed before creating a row batch");
            return new PrestoSparkRowBatch(partitionCount, rowCount, sliceOutput.getUnderlyingSlice().byteArray(), rowPartitions, rowSizes);
        }
    }

    public static class RowTupleSupplier
    {
        private final int partitionCount;
        private final int rowCount;
        private final int[] rowPartitions;
        private final int[] rowSizes;

        private int remainingReplicasCount;
        private int currentRow;
        private int currentOffset;
        private final ByteBuffer rowData;
        private final MutablePartitionId mutablePartitionId;
        private final Tuple2<MutablePartitionId, PrestoSparkMutableRow> tuple;

        private RowTupleSupplier(int partitionCount, int rowCount, byte[] rowData, int[] rowPartitions, int[] rowSizes)
        {
            this.partitionCount = partitionCount;
            this.rowCount = rowCount;
            this.rowPartitions = requireNonNull(rowPartitions, "rowPartitions is null");
            this.rowSizes = requireNonNull(rowSizes, "rowSizes is null");

            this.rowData = ByteBuffer.wrap(requireNonNull(rowData, "rowData is null"));
            mutablePartitionId = new MutablePartitionId();
            PrestoSparkMutableRow row = new PrestoSparkMutableRow();
            row.setBuffer(this.rowData);
            tuple = new Tuple2<>(mutablePartitionId, row);
        }

        @Nullable
        public Tuple2<MutablePartitionId, PrestoSparkMutableRow> getNext()
        {
            if (currentRow >= rowCount) {
                return null;
            }

            int rowSize = rowSizes[currentRow];
            rowData.limit(currentOffset + rowSize);
            rowData.position(currentOffset);

            int partition = rowPartitions[currentRow];
            if (partition == REPLICATED_ROW_PARTITION_ID) {
                if (remainingReplicasCount == 0) {
                    remainingReplicasCount = partitionCount;
                }
                mutablePartitionId.setPartition(remainingReplicasCount - 1);
                remainingReplicasCount--;
                if (remainingReplicasCount == 0) {
                    currentRow++;
                    currentOffset += rowSize;
                }
            }
            else {
                mutablePartitionId.setPartition(partition);
                currentRow++;
                currentOffset += rowSize;
            }
            return tuple;
        }
    }
}
