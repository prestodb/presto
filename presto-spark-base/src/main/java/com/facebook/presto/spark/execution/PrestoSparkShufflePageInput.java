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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;
import scala.collection.Iterator;

import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.List;

import static com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats.Operation.READ;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class PrestoSparkShufflePageInput
        implements PrestoSparkPageInput
{
    private static final int TARGET_SIZE = 1024 * 1024;
    private static final int BUFFER_SIZE = (int) (TARGET_SIZE * 1.2f);
    private static final int MAX_ROWS_PER_PAGE = 20_000;

    private final List<Type> types;
    private final List<PrestoSparkShuffleInput> shuffleInputs;
    private final int taskId;
    private final CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector;

    @GuardedBy("this")
    private int currentIteratorIndex;
    @GuardedBy("this")
    private final ShuffleStats shuffleStats = new ShuffleStats();

    public PrestoSparkShufflePageInput(
            List<Type> types,
            List<PrestoSparkShuffleInput> shuffleInputs,
            int taskId,
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.shuffleInputs = ImmutableList.copyOf(requireNonNull(shuffleInputs, "shuffleInputs is null"));
        this.taskId = taskId;
        this.shuffleStatsCollector = requireNonNull(shuffleStatsCollector, "shuffleStatsCollector is null");
    }

    @Override
    public Page getNextPage()
    {
        SliceOutput output = new DynamicSliceOutput(types.isEmpty() ? 0 : BUFFER_SIZE);
        int rowCount = 0;
        synchronized (this) {
            while (currentIteratorIndex < shuffleInputs.size()) {
                PrestoSparkShuffleInput input = shuffleInputs.get(currentIteratorIndex);
                Iterator<Tuple2<MutablePartitionId, PrestoSparkMutableRow>> iterator = input.getIterator();
                long currentIteratorProcessedBytes = 0;
                long currentIteratorProcessedRows = 0;
                long currentIteratorProcessedRowBatches = 0;
                long start = System.currentTimeMillis();
                while (iterator.hasNext() && output.size() <= TARGET_SIZE && rowCount <= MAX_ROWS_PER_PAGE) {
                    currentIteratorProcessedRowBatches++;
                    PrestoSparkMutableRow row = iterator.next()._2;
                    if (row.getBuffer() != null) {
                        ByteBuffer buffer = row.getBuffer();
                        verify(buffer.remaining() >= 2, "row data is expected to be at least 2 bytes long");
                        currentIteratorProcessedBytes += buffer.remaining();
                        short entryRowCount = getShortLittleEndian(buffer);
                        rowCount += entryRowCount;
                        currentIteratorProcessedRows += entryRowCount;
                        buffer.position(buffer.position() + 2);
                        output.writeBytes(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
                    }
                    else if (row.getArray() != null) {
                        verify(row.getLength() >= 2, "row data is expected to be at least 2 bytes long");
                        currentIteratorProcessedBytes += row.getLength();
                        short entryRowCount = getShortLittleEndian(row.getArray(), row.getOffset());
                        rowCount += entryRowCount;
                        currentIteratorProcessedRows += entryRowCount;
                        output.writeBytes(row.getArray(), row.getOffset() + 2, row.getLength() - 2);
                    }
                    else {
                        throw new IllegalArgumentException("Unexpected PrestoSparkMutableRow: 'buffer' and 'array' fields are both null");
                    }
                }
                long end = System.currentTimeMillis();
                shuffleStats.accumulate(
                        currentIteratorProcessedRows,
                        currentIteratorProcessedRowBatches,
                        currentIteratorProcessedBytes,
                        end - start);
                if (!iterator.hasNext()) {
                    shuffleStatsCollector.add(new PrestoSparkShuffleStats(
                            input.getFragmentId(),
                            taskId,
                            READ,
                            shuffleStats.getProcessedRows(),
                            shuffleStats.getProcessedRowBatches(),
                            shuffleStats.getProcessedBytes(),
                            shuffleStats.getElapsedWallTimeMills()));
                    shuffleStats.reset();
                    currentIteratorIndex++;
                }
                else {
                    break;
                }
            }
        }
        if (rowCount == 0) {
            return null;
        }
        return createPage(rowCount, output.slice().getInput(), types);
    }

    private static Page createPage(int rowCount, BasicSliceInput input, List<Type> types)
    {
        checkArgument(rowCount > 0, "rowCount must be greater than zero: %s", rowCount);
        if (input.length() == 0) {
            // zero column page
            verify(types.isEmpty(), "types is expected to be empty");
            return new Page(rowCount);
        }
        PageBuilder pageBuilder = new PageBuilder(types);
        while (input.isReadable()) {
            pageBuilder.declarePosition();
            for (int channel = 0; channel < types.size(); channel++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(channel);
                blockBuilder.readPositionFrom(input);
            }
        }
        Page page = pageBuilder.build();
        verify(page.getPositionCount() == rowCount, "unexpected row count: %s != %s", page.getPositionCount(), rowCount);
        return page;
    }

    private static short getShortLittleEndian(ByteBuffer byteBuffer)
    {
        byte leastSignificant = byteBuffer.get(byteBuffer.position());
        byte mostSignificant = byteBuffer.get(byteBuffer.position() + 1);
        return getShort(leastSignificant, mostSignificant);
    }

    private static short getShortLittleEndian(byte[] bytes, int offset)
    {
        byte leastSignificant = bytes[offset];
        byte mostSignificant = bytes[offset + 1];
        return getShort(leastSignificant, mostSignificant);
    }

    private static short getShort(byte leastSignificant, byte mostSignificant)
    {
        return (short) ((leastSignificant & 0xFF) | ((mostSignificant & 0xFF) << 8));
    }

    private static class ShuffleStats
    {
        private long processedRows;
        private long processedRowBatches;
        private long processedBytes;
        private long elapsedWallTimeMills;

        public void accumulate(long processedRows, long processedRowBatches, long processedBytes, long elapsedWallTimeMills)
        {
            this.processedRows += processedRows;
            this.processedRowBatches += processedRowBatches;
            this.processedBytes += processedBytes;
            this.elapsedWallTimeMills += elapsedWallTimeMills;
        }

        public void reset()
        {
            processedRows = 0;
            processedRowBatches = 0;
            processedBytes = 0;
            elapsedWallTimeMills = 0;
        }

        public long getProcessedRows()
        {
            return processedRows;
        }

        public long getProcessedRowBatches()
        {
            return processedRowBatches;
        }

        public long getProcessedBytes()
        {
            return processedBytes;
        }

        public long getElapsedWallTimeMills()
        {
            return elapsedWallTimeMills;
        }
    }
}
