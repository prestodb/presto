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
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;
import scala.Tuple2;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRowBatch
        implements PrestoSparkBufferedResult
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PrestoSparkRowBatch.class).instanceSize();

    private static final int MIN_TARGET_SIZE_IN_BYTES = 1024 * 1024;
    private static final int MAX_TARGET_SIZE_IN_BYTES = 10 * 1024 * 1024;
    private static final int DEFAULT_EXPECTED_ROWS_COUNT = 10000;
    private static final int REPLICATED_ROW_PARTITION_ID = -1;
    private static final short MULTI_ROW_ENTRY_MAX_SIZE_IN_BYTES = 10 * 1024;
    private static final short MULTI_ROW_ENTRY_MAX_ROW_COUNT = 10 * 1024;

    private final int partitionCount;
    private final int rowCount;
    private final byte[] rowData;
    private final int[] rowPartitions;
    private final int[] rowOffsets;
    private final int totalSizeInBytes;
    private final long retainedSizeInBytes;

    private PrestoSparkRowBatch(int partitionCount, int rowCount, byte[] rowData, int[] rowPartitions, int[] rowOffsets, int totalSizeInBytes)
    {
        this.partitionCount = partitionCount;
        this.rowCount = rowCount;
        this.rowData = requireNonNull(rowData, "rowData is null");
        this.rowPartitions = requireNonNull(rowPartitions, "rowPartitions is null");
        this.rowOffsets = requireNonNull(rowOffsets, "rowOffsets is null");
        this.retainedSizeInBytes = INSTANCE_SIZE
                + sizeOf(rowData)
                + sizeOf(rowPartitions)
                + sizeOf(rowOffsets);
        this.totalSizeInBytes = totalSizeInBytes;
    }

    public RowTupleSupplier createRowTupleSupplier()
    {
        return new RowTupleSupplier(partitionCount, rowCount, rowData, rowPartitions, rowOffsets, totalSizeInBytes);
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public int getPositionCount()
    {
        return rowCount;
    }

    public static PrestoSparkRowBatchBuilder builder(int partitionCount, int targetAverageRowSizeInBytes)
    {
        checkArgument(partitionCount > 0, "partitionCount must be greater then zero: %s", partitionCount);
        int targetSizeInBytes = partitionCount * targetAverageRowSizeInBytes;
        targetSizeInBytes = max(targetSizeInBytes, MIN_TARGET_SIZE_IN_BYTES);
        targetSizeInBytes = min(targetSizeInBytes, MAX_TARGET_SIZE_IN_BYTES);
        targetAverageRowSizeInBytes = min(targetSizeInBytes / partitionCount, targetAverageRowSizeInBytes);
        return builder(
                partitionCount,
                targetSizeInBytes,
                DEFAULT_EXPECTED_ROWS_COUNT,
                targetAverageRowSizeInBytes,
                MULTI_ROW_ENTRY_MAX_SIZE_IN_BYTES,
                MULTI_ROW_ENTRY_MAX_ROW_COUNT);
    }

    @VisibleForTesting
    static PrestoSparkRowBatchBuilder builder(
            int partitionCount,
            int targetSizeInBytes,
            int expectedRowsCount,
            int targetAverageRowSizeInBytes,
            int maxEntrySizeInBytes,
            int maxRowsPerEntry)
    {
        return new PrestoSparkRowBatchBuilder(
                partitionCount,
                targetSizeInBytes,
                expectedRowsCount,
                targetAverageRowSizeInBytes,
                maxEntrySizeInBytes,
                maxRowsPerEntry);
    }

    public static class PrestoSparkRowBatchBuilder
    {
        private static final int BUILDER_INSTANCE_SIZE = ClassLayout.parseClass(PrestoSparkRowBatchBuilder.class).instanceSize();

        private final int partitionCount;
        private final int targetSizeInBytes;
        private final int targetAverageRowSizeInBytes;
        private final int maxEntrySizeInBytes;
        private final int maxRowsPerEntry;
        private final DynamicSliceOutput sliceOutput;
        private int[] rowOffsets;
        private int totalSizeInBytes;
        private int[] rowPartitions;
        private int rowCount;

        private int currentRowOffset;
        private boolean openEntry;

        private PrestoSparkRowBatchBuilder(
                int partitionCount,
                int targetSizeInBytes,
                int expectedRowsCount,
                int targetAverageRowSizeInBytes,
                int maxEntrySizeInBytes,
                int maxRowsPerEntry)
        {
            checkArgument(partitionCount > 0, "partitionCount must be greater then zero: %s", partitionCount);
            this.partitionCount = partitionCount;
            this.targetSizeInBytes = targetSizeInBytes;
            this.targetAverageRowSizeInBytes = targetAverageRowSizeInBytes;
            this.maxEntrySizeInBytes = maxEntrySizeInBytes;
            this.maxRowsPerEntry = maxRowsPerEntry;
            sliceOutput = new DynamicSliceOutput((int) (targetSizeInBytes * 1.2f));
            rowOffsets = new int[expectedRowsCount];
            rowPartitions = new int[expectedRowsCount];
        }

        public long getRetainedSizeInBytes()
        {
            return BUILDER_INSTANCE_SIZE + sliceOutput.getRetainedSize() + sizeOf(rowOffsets) + sizeOf(rowPartitions);
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
            sliceOutput.writeShort(1);
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

            rowOffsets = ensureCapacity(rowOffsets, rowCount + 1);
            rowOffsets[rowCount] = currentRowOffset;

            rowPartitions = ensureCapacity(rowPartitions, rowCount + 1);
            rowPartitions[rowCount] = partitionId;

            rowCount++;
            totalSizeInBytes += sliceOutput.size() - currentRowOffset;
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

            if (rowCount == 0) {
                return createDirectRowBatch();
            }

            int averageRowSize = totalSizeInBytes / rowCount;
            if (averageRowSize < targetAverageRowSizeInBytes) {
                return createGroupedRowBatch();
            }

            return createDirectRowBatch();
        }

        private PrestoSparkRowBatch createDirectRowBatch()
        {
            return new PrestoSparkRowBatch(
                    partitionCount,
                    rowCount,
                    sliceOutput.getUnderlyingSlice().byteArray(),
                    rowPartitions,
                    rowOffsets,
                    totalSizeInBytes);
        }

        private PrestoSparkRowBatch createGroupedRowBatch()
        {
            RowIndex rowIndex = RowIndex.create(rowCount, partitionCount, rowPartitions);
            byte[] data = sliceOutput.getUnderlyingSlice().byteArray();

            DynamicSliceOutput output = new DynamicSliceOutput((int) (totalSizeInBytes * 1.2f));
            int expectedEntriesCount = (int) ((totalSizeInBytes / targetAverageRowSizeInBytes) * 1.2f);
            int[] entryOffsets = new int[expectedEntriesCount];
            int[] entryPartitions = new int[expectedEntriesCount];
            int entriesCount = 0;
            for (int partition = REPLICATED_ROW_PARTITION_ID; partition < partitionCount; partition++) {
                while (rowIndex.hasNextRow(partition)) {
                    // start entry
                    short currentEntrySize = 0;
                    short currentEntryRowCount = 0;
                    int currentEntryOffset = output.size();
                    // Reserve space for the row count, the actual row count will be set later
                    output.writeShort(0);

                    entryOffsets = ensureCapacity(entryOffsets, entriesCount + 1);
                    entryOffsets[entriesCount] = currentEntryOffset;
                    entryPartitions = ensureCapacity(entryPartitions, entriesCount + 1);
                    entryPartitions[entriesCount] = partition;

                    while (rowIndex.hasNextRow(partition)) {
                        int row = rowIndex.peekRow(partition);
                        int followingRow = row + 1;
                        int rowOffset = rowOffsets[row];
                        int followingRowOffset = followingRow < rowCount ? rowOffsets[followingRow] : totalSizeInBytes;
                        int rowSize = followingRowOffset - rowOffset;

                        verify(rowSize >= 2, "rowSize is expected to be greater than or equal to 2: %s", rowSize);

                        // skip the rows count
                        rowOffset += 2;
                        rowSize -= 2;

                        if (currentEntryRowCount > 0 && (currentEntrySize + rowSize > maxEntrySizeInBytes || currentEntryRowCount + 1 > maxRowsPerEntry)) {
                            break;
                        }

                        output.writeBytes(data, rowOffset, rowSize);
                        currentEntrySize += rowSize;
                        currentEntryRowCount++;

                        rowIndex.nextRow(partition);
                    }

                    // entry is done
                    output.getUnderlyingSlice().setShort(currentEntryOffset, currentEntryRowCount);
                    entriesCount++;
                }
            }

            return new PrestoSparkRowBatch(
                    partitionCount,
                    entriesCount,
                    output.getUnderlyingSlice().byteArray(),
                    entryPartitions,
                    entryOffsets,
                    output.size());
        }
    }

    public static class RowTupleSupplier
    {
        private final int partitionCount;
        private final int rowCount;
        private final int[] rowPartitions;
        private final int[] rowOffsets;
        private final int totalSizeInBytes;

        private int remainingReplicasCount;
        private int currentRow;
        private final ByteBuffer rowData;
        private final MutablePartitionId mutablePartitionId;
        private final PrestoSparkMutableRow row;
        private final Tuple2<MutablePartitionId, PrestoSparkMutableRow> tuple;

        private RowTupleSupplier(int partitionCount, int rowCount, byte[] rowData, int[] rowPartitions, int[] rowOffsets, int totalSizeInBytes)
        {
            this.partitionCount = partitionCount;
            this.rowCount = rowCount;
            this.rowPartitions = requireNonNull(rowPartitions, "rowPartitions is null");
            this.rowOffsets = requireNonNull(rowOffsets, "rowSizes is null");
            this.totalSizeInBytes = totalSizeInBytes;

            this.rowData = ByteBuffer.wrap(requireNonNull(rowData, "rowData is null"));
            this.rowData.order(LITTLE_ENDIAN);
            mutablePartitionId = new MutablePartitionId();
            row = new PrestoSparkMutableRow();
            row.setBuffer(this.rowData);
            tuple = new Tuple2<>(mutablePartitionId, row);
        }

        @Nullable
        public Tuple2<MutablePartitionId, PrestoSparkMutableRow> getNext()
        {
            if (currentRow >= rowCount) {
                return null;
            }

            int currentRowOffset = rowOffsets[currentRow];
            int nextRow = currentRow + 1;
            int nextRowOffset = nextRow < rowCount ? rowOffsets[nextRow] : totalSizeInBytes;
            int rowSize = nextRowOffset - currentRowOffset;
            rowData.limit(currentRowOffset + rowSize);
            rowData.position(currentRowOffset);

            short rowsCount = rowData.getShort(currentRowOffset);
            row.setPositionCount(rowsCount);

            int partition = rowPartitions[currentRow];
            if (partition == REPLICATED_ROW_PARTITION_ID) {
                if (remainingReplicasCount == 0) {
                    remainingReplicasCount = partitionCount;
                }
                mutablePartitionId.setPartition(remainingReplicasCount - 1);
                remainingReplicasCount--;
                if (remainingReplicasCount == 0) {
                    currentRow++;
                }
            }
            else {
                mutablePartitionId.setPartition(partition);
                currentRow++;
            }
            return tuple;
        }
    }

    /*
     * Partitions rows into disjoint sets based on the partitions assigned
     *
     * int[] rowIndex - links rows that belong for the same partition
     *
     * For example for 3 rows with partitions assigned [2, 1, 2, 1] the
     * row index will look like:
     *
     * [2, 3, -1, -1]
     *
     * int[] nextRow - contains the pointers to the next row for each partition:
     *
     * [-1, 1, 0]
     *
     * note: there's no rows with partition 0
     *
     * To get all rows for a single partition first we check what is the tip of the
     * list of rows for that partition at the moment:
     *
     * int row = nextRow[partition]
     *
     * And then we iterate over the linked list to get all the rows that belong to
     * the same partition:
     *
     * while (rowIndex[row] != -1)
     *      row = rowIndex[row]
     */
    public static class RowIndex
    {
        private static final int NIL = -1;

        private final int[] nextRow;
        private final int[] rowIndex;

        public static RowIndex create(int rowCount, int partitionCount, int[] partitions)
        {
            // one more slot for replicated partition
            int[] nextRow = new int[partitionCount + 1];
            fill(nextRow, NIL);
            int[] rowIndex = new int[rowCount];
            fill(rowIndex, NIL);

            for (int row = rowCount - 1; row >= 0; row--) {
                int partition = partitions[row];
                int partitionIndex = getPartitionIndex(partition, nextRow);
                int currentPointer = nextRow[partitionIndex];
                nextRow[partitionIndex] = row;
                rowIndex[row] = currentPointer;
            }

            return new RowIndex(nextRow, rowIndex);
        }

        private RowIndex(int[] nextRow, int[] rowIndex)
        {
            this.nextRow = requireNonNull(nextRow, "nextRow is null");
            this.rowIndex = requireNonNull(rowIndex, "rowIndex is null");
        }

        public boolean hasNextRow(int partition)
        {
            return peekRow(partition) != NIL;
        }

        public int peekRow(int partition)
        {
            return nextRow[getPartitionIndex(partition, nextRow)];
        }

        public int nextRow(int partition)
        {
            int partitionIndex = getPartitionIndex(partition, nextRow);
            int result = nextRow[partitionIndex];
            nextRow[partitionIndex] = rowIndex[result];
            return result;
        }

        private static int getPartitionIndex(int partition, int[] nextRow)
        {
            if (partition == REPLICATED_ROW_PARTITION_ID) {
                return nextRow.length - 1;
            }
            return partition;
        }
    }
}
