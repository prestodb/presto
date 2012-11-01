package com.facebook.presto.serde;

import com.facebook.presto.SizeOf;
import com.facebook.presto.Tuple;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.nblock.Blocks;
import com.facebook.presto.serde.StatsCollectingBlocksSerde.StatsCollector.Stats;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class StatsCollectingBlocksSerde
{
    private StatsCollectingBlocksSerde()
    {
    }

    public static BlocksWriter createBlocksWriter(SliceOutput sliceOutput)
    {
        return createBlocksWriter(sliceOutput, null);
    }

    public static BlocksWriter createBlocksWriter(final SliceOutput sliceOutput, @Nullable final BlockSerde blockSerde)
    {
        checkNotNull(sliceOutput, "sliceOutput is null");
        return new StatsCollectingBlocksWriter(BlocksSerde.createBlocksWriter(sliceOutput, blockSerde), sliceOutput);
    }

    public static void writeBlocks(SliceOutput sliceOutput, Block... blocks)
    {
        writeBlocks(sliceOutput, ImmutableList.copyOf(blocks).iterator());
    }

    public static void writeBlocks(SliceOutput sliceOutput, Iterable<? extends Block> blocks)
    {
        writeBlocks(sliceOutput, blocks.iterator());
    }

    public static void writeBlocks(SliceOutput sliceOutput, Iterator<? extends Block> blocks)
    {
        BlocksWriter blocksWriter = createBlocksWriter(sliceOutput);
        while (blocks.hasNext()) {
            blocksWriter.append(blocks.next());
        }
        blocksWriter.finish();
    }

    public static Blocks readBlocks(Slice slice)
    {
        return readBlocks(slice, 0);
    }

    public static Blocks readBlocks(Slice slice, long startPosition)
    {
        int footerLength = slice.getInt(slice.length() - SizeOf.SIZE_OF_INT);
        int footerOffset = slice.length() - footerLength - SizeOf.SIZE_OF_INT;
        return BlocksSerde.readBlocks(slice.slice(0, footerOffset), startPosition);
    }

    public static Stats readStats(Slice slice)
    {
        checkNotNull(slice, "slice is null");
        int footerLength = slice.getInt(slice.length() - SizeOf.SIZE_OF_INT);
        int footerOffset = slice.length() - footerLength - SizeOf.SIZE_OF_INT;
        return Stats.deserialize(slice.slice(footerOffset, footerLength));
    }

    private static class StatsCollectingBlocksWriter implements BlocksWriter
    {
        private final BlocksWriter blocksWriter;
        private final SliceOutput sliceOutput;
        private final StatsCollector statsCollector = new StatsCollector();

        public StatsCollectingBlocksWriter(BlocksWriter blocksWriter, SliceOutput sliceOutput)
        {
            this.blocksWriter = blocksWriter;
            this.sliceOutput = sliceOutput;
        }

        @Override
        public BlocksWriter append(Block block)
        {
            Preconditions.checkNotNull(block, "block is null");
            statsCollector.process(block);
            blocksWriter.append(block);
            return this;
        }

        @Override
        public void finish()
        {
            blocksWriter.finish();

            int startingIndex = sliceOutput.size();
            Stats.serialize(statsCollector.getStats(), sliceOutput);
            int endingIndex = sliceOutput.size();
            checkState(endingIndex > startingIndex);
            sliceOutput.writeInt(endingIndex - startingIndex);
        }
    }


    public static class StatsCollector
    {
        private static final int MAX_UNIQUE_COUNT = 1000;

        private long rowCount;
        private long runsCount;
        private Tuple lastTuple;
        private long minPosition = Long.MAX_VALUE;
        private long maxPosition = -1;
        private final Set<Tuple> set = new HashSet<>(MAX_UNIQUE_COUNT);
        private boolean finished = false;

        public void process(Block block)
        {
            Preconditions.checkNotNull(block, "block is null");
            Preconditions.checkState(!finished, "already finished");

            BlockCursor cursor = block.cursor();
            while (cursor.advanceNextPosition()) {
                if (lastTuple == null) {
                    lastTuple = cursor.getTuple();
                    if (set.size() < MAX_UNIQUE_COUNT) {
                        set.add(lastTuple);
                    }
                }
                else if (!cursor.currentTupleEquals(lastTuple)) {
                    runsCount++;
                    lastTuple = cursor.getTuple();
                    if (set.size() < MAX_UNIQUE_COUNT) {
                        set.add(lastTuple);
                    }
                }
            }

            minPosition = Math.min(minPosition, block.getRange().getStart());
            maxPosition = Math.max(maxPosition, block.getRange().getEnd());
            rowCount += block.getPositionCount();
        }

        public void finished()
        {
            finished = true;
        }

        public Stats getStats()
        {
            // TODO: expose a way to indicate whether the unique count is EXACT or APPROXIMATE
            return new Stats(rowCount, runsCount + 1, minPosition, maxPosition, rowCount / (runsCount + 1), (set.size() == MAX_UNIQUE_COUNT) ? Integer.MAX_VALUE : set.size());
        }

        public static class Stats
        {
            private final long rowCount;
            private final long runsCount;
            private final long minPosition;
            private final long maxPosition;
            private final long avgRunLength;
            private final int uniqueCount;

            public Stats(long rowCount, long runsCount, long minPosition, long maxPosition, long avgRunLength, int uniqueCount)
            {
                this.rowCount = rowCount;
                this.runsCount = runsCount;
                this.minPosition = minPosition;
                this.maxPosition = maxPosition;
                this.avgRunLength = avgRunLength;
                this.uniqueCount = uniqueCount;
            }

            public static void serialize(Stats stats, SliceOutput sliceOutput)
            {
                // TODO: add a better way of serializing the stats that is less fragile
                sliceOutput.appendLong(stats.getRowCount())
                        .appendLong(stats.getRunsCount())
                        .appendLong(stats.getMinPosition())
                        .appendLong(stats.getMaxPosition())
                        .appendLong(stats.getAvgRunLength())
                        .appendInt(stats.getUniqueCount());
            }

            public static Stats deserialize(Slice slice)
            {
                SliceInput input = slice.getInput();
                long rowCount = input.readLong();
                long runsCount = input.readLong();
                long minPosition = input.readLong();
                long maxPosition = input.readLong();
                long avgRunLength = input.readLong();
                int uniqueCount = input.readInt();
                return new Stats(rowCount, runsCount, minPosition, maxPosition, avgRunLength, uniqueCount);
            }

            public long getRowCount()
            {
                return rowCount;
            }

            public long getRunsCount()
            {
                return runsCount;
            }

            public long getMinPosition()
            {
                return minPosition;
            }

            public long getMaxPosition()
            {
                return maxPosition;
            }

            public long getAvgRunLength()
            {
                return avgRunLength;
            }

            public int getUniqueCount()
            {
                return uniqueCount;
            }
        }
    }

}
