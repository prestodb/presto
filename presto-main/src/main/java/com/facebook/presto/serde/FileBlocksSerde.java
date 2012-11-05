package com.facebook.presto.serde;

import com.facebook.presto.SizeOf;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.FileBlocksSerde.StatsCollector.Stats;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.serde.RunLengthEncodedBlockSerde.RLE_BLOCK_SERDE;
import static com.facebook.presto.serde.UncompressedBlockSerde.UNCOMPRESSED_BLOCK_SERDE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class FileBlocksSerde
{
    private FileBlocksSerde()
    {
    }

    public static FileBlockIterable readBlocks(Slice slice)
    {
        return readBlocks(slice, 0);
    }

    public static FileBlockIterable readBlocks(Slice slice, long startPosition)
    {
        return new FileBlockIterable(slice, startPosition);
    }

    public static class FileBlockIterable
            implements BlockIterable
    {
        private final Slice statsSlice;
        private final FileEncoding encoding;
        private final Slice blocksSlice;
        private final BlockIterable blockIterable;

        private FileBlockIterable(Slice slice, long positionOffset)
        {
            Preconditions.checkNotNull(slice, "slice is null");
            Preconditions.checkArgument(positionOffset >= 0, "positionOffset is negative");

            // read file footer
            int footerLength = slice.getInt(slice.length() - SizeOf.SIZE_OF_INT);
            int footerOffset = slice.length() - footerLength - SizeOf.SIZE_OF_INT;
            statsSlice = slice.slice(footerOffset, footerLength);

            // read file header
            encoding = FileEncoding.encodingForId(slice.getByte(0));
            blocksSlice = slice.slice(1, footerOffset - 1);
            blockIterable = encoding.createBlocksReader(blocksSlice, positionOffset);
        }

        public FileEncoding getEncoding()
        {
            return encoding;
        }

        public Stats getStats()
        {
            return Stats.deserialize(statsSlice);
        }

        @Override
        public Iterator<Block> iterator()
        {
            return blockIterable.iterator();
        }
    }

    public static void writeBlocks(FileEncoding encoding, SliceOutput sliceOutput, Block... blocks)
    {
        writeBlocks(encoding, sliceOutput, ImmutableList.copyOf(blocks));
    }

    public static void writeBlocks(FileEncoding encoding, SliceOutput sliceOutput, Iterable<? extends Block> blocks)
    {
        writeBlocks(encoding, sliceOutput, blocks.iterator());
    }

    public static void writeBlocks(FileEncoding encoding, SliceOutput sliceOutput, Iterator<? extends Block> blocks)
    {
        checkNotNull(sliceOutput, "sliceOutput is null");
        BlocksWriter blocksWriter = createBlocksWriter(encoding, sliceOutput);
        while (blocks.hasNext()) {
            blocksWriter.append(blocks.next());
        }
        blocksWriter.finish();
    }

    public static FileBlocksWriter createBlocksWriter(FileEncoding encoding, SliceOutput sliceOutput)
    {
        return new FileBlocksWriter(encoding, sliceOutput);
    }

    private static class FileBlocksWriter implements BlocksWriter
    {
        private final StatsCollector statsCollector = new StatsCollector();
        private final SliceOutput sliceOutput;
        private BlocksWriter blocksWriter;
        private final FileEncoding encoding;

        public FileBlocksWriter(FileEncoding encoding, SliceOutput sliceOutput)
        {
            checkNotNull(encoding, "encoding is null");
            checkNotNull(sliceOutput, "sliceOutput is null");

            this.encoding = encoding;
            this.sliceOutput = sliceOutput;
        }

        @Override
        public BlocksWriter append(Tuple tuple)
        {
            Preconditions.checkNotNull(tuple, "tuple is null");
            if (blocksWriter == null) {
                // write header
                sliceOutput.writeByte(encoding.getId());
                blocksWriter = encoding.createBlocksWriter(sliceOutput);
            }
            statsCollector.process(tuple);
            blocksWriter.append(tuple);
            return this;
        }

        @Override
        public BlocksWriter append(Block block)
        {
            Preconditions.checkNotNull(block, "block is null");
            if (blocksWriter == null) {
                // write header
                sliceOutput.writeByte(encoding.getId());
                blocksWriter = encoding.createBlocksWriter(sliceOutput);
            }
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

    public static enum FileEncoding
    {
        RAW((byte) 0, "raw")
                {
                    private final BlocksSerde blocksSerde = new SimpleBlocksSerde(UNCOMPRESSED_BLOCK_SERDE);

                    @Override
                    public BlockIterable createBlocksReader(final Slice slice, final long positionOffset)
                    {
                        return blocksSerde.createBlocksReader(slice, positionOffset);
                    }

                    @Override
                    public BlocksWriter createBlocksWriter(SliceOutput sliceOutput)
                    {
                        return blocksSerde.createBlocksWriter(sliceOutput);
                    }
                },
        RLE((byte) 1, "rle")
                {
                    private final BlocksSerde blocksSerde = new SimpleBlocksSerde(RLE_BLOCK_SERDE);

                    @Override
                    public BlockIterable createBlocksReader(final Slice slice, final long positionOffset)
                    {
                        return blocksSerde.createBlocksReader(slice, positionOffset);
                    }

                    @Override
                    public BlocksWriter createBlocksWriter(SliceOutput sliceOutput)
                    {
                        return blocksSerde.createBlocksWriter(sliceOutput);
                    }
                },
        DIC_RAW((byte) 2, "dic-raw")
                {
                    private final DictionaryEncodedBlocksSerde blocksSerde = new DictionaryEncodedBlocksSerde(new SimpleBlocksSerde(UNCOMPRESSED_BLOCK_SERDE));

                    @Override
                    public BlockIterable createBlocksReader(final Slice slice, final long positionOffset)
                    {
                        return blocksSerde.createBlocksReader(slice, positionOffset);
                    }

                    @Override
                    public BlocksWriter createBlocksWriter(SliceOutput sliceOutput)
                    {
                        return blocksSerde.createBlocksWriter(sliceOutput);
                    }
                },
        DIC_RLE((byte) 3, "dic-rle")
                {
                    private final DictionaryEncodedBlocksSerde blocksSerde = new DictionaryEncodedBlocksSerde(new SimpleBlocksSerde(RLE_BLOCK_SERDE));

                    @Override
                    public BlockIterable createBlocksReader(final Slice slice, final long positionOffset)
                    {
                        return blocksSerde.createBlocksReader(slice, positionOffset);
                    }

                    @Override
                    public BlocksWriter createBlocksWriter(SliceOutput sliceOutput)
                    {
                        return blocksSerde.createBlocksWriter(sliceOutput);
                    }
                };

        private static final Map<Byte, FileEncoding> ID_MAP;

        static {
            ImmutableMap.Builder<Byte, FileEncoding> builder = ImmutableMap.builder();
            for (FileEncoding encoding : FileEncoding.values()) {
                builder.put(encoding.getId(), encoding);
            }
            ID_MAP = builder.build();
        }

        private final byte id;
        private final String name;

        private FileEncoding(byte id, String name)
        {
            this.id = checkNotNull(id, "id is null");
            this.name = checkNotNull(name, "name is null");
        }

        public byte getId()
        {
            return id;
        }

        public String getName()
        {
            return name;
        }

        public abstract BlockIterable createBlocksReader(Slice slice, long positionOffset);

        public abstract BlocksWriter createBlocksWriter(SliceOutput sliceOutput);

        public static FileEncoding encodingForId(byte id)
        {
            FileEncoding encoding = ID_MAP.get(id);
            checkArgument(encoding != null, "Invalid id name: %s", id);
            return encoding;
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

        public void process(Tuple tuple)
        {
            Preconditions.checkNotNull(tuple, "tuple is null");
            Preconditions.checkState(!finished, "already finished");

            if (lastTuple == null) {
                lastTuple = tuple;
                if (set.size() < MAX_UNIQUE_COUNT) {
                    set.add(lastTuple);
                }
            }
            else if (!tuple.equals(lastTuple)) {
                runsCount++;
                lastTuple = tuple;
                if (set.size() < MAX_UNIQUE_COUNT) {
                    set.add(lastTuple);
                }
            }
            rowCount++;
        }

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
