package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.longs.LongHash.Strategy;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;

public class HashJoinOperator
        implements Operator
{
    private final Operator buildSource;
    private final int buildJoinChannel;
    private final Operator probeSource;
    private final int probeJoinChannel;
    private final List<TupleInfo> tupleInfos;

    public HashJoinOperator(Operator buildSource, int buildJoinChannel, Operator probeSource, int probeJoinChannel)
    {
        Preconditions.checkNotNull(buildSource, "buildSource is null");
        Preconditions.checkArgument(buildJoinChannel >= 0, "buildJoinChannel is negative");
        Preconditions.checkNotNull(probeSource, "probeSource is null");
        Preconditions.checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");

        this.buildSource = buildSource;
        this.buildJoinChannel = buildJoinChannel;
        this.probeSource = probeSource;
        this.probeJoinChannel = probeJoinChannel;

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        tupleInfos.addAll(probeSource.getTupleInfos());
        int buildChannel = 0;
        for (TupleInfo tupleInfo : buildSource.getTupleInfos()) {
            if (buildChannel != buildJoinChannel) {
                tupleInfos.add(tupleInfo);
            }
            buildChannel++;
        }
        this.tupleInfos = tupleInfos.build();
    }

    @Override
    public int getChannelCount()
    {
        return buildSource.getChannelCount() + probeSource.getChannelCount() - 1;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new HashJoinIterator(tupleInfos, buildSource, buildJoinChannel, probeSource, probeJoinChannel, 1_000_000);
    }

    private static class HashJoinIterator
            extends AbstractIterator<Page>
    {
        private final List<TupleInfo> tupleInfos;

        private final Iterator<Page> probeIterator;
        private final int probeJoinChannel;

        private final PagesIndex buildIndex;
        private final SliceHashStrategy hashStrategy;
        private final Long2IntOpenCustomHashMap joinChannelHash;

        private final int buildJoinChannel;
        private final IntArrayList positionLinks;

        private HashJoinIterator(List<TupleInfo> tupleInfos, Operator buildSource, int buildJoinChannel, Operator probeSource, int probeJoinChannel, int expectedPositions)
        {
            this.tupleInfos = tupleInfos;
            this.buildJoinChannel = buildJoinChannel;
            probeIterator = probeSource.iterator();
            this.probeJoinChannel = probeJoinChannel;

            // index build channel
            buildIndex = new PagesIndex(buildSource.getChannelCount(), expectedPositions, tupleInfos);
            for (Page page : buildSource) {
                buildIndex.indexPage(page);
            }

            // build hash over build join channel
            BlocksIndex joinChannelIndex = buildIndex.getIndex(buildJoinChannel);
            hashStrategy = new SliceHashStrategy(joinChannelIndex.getTupleInfo(), joinChannelIndex.getSlices().elements());
            joinChannelHash = new Long2IntOpenCustomHashMap(expectedPositions, hashStrategy);
            joinChannelHash.defaultReturnValue(-1);
            positionLinks = new IntArrayList(new int[joinChannelIndex.getOffsets().size()]);
            Arrays.fill(positionLinks.elements(), -1);
            for (int position = 0; position < joinChannelIndex.getOffsets().size(); position++) {
                long sliceAddress = joinChannelIndex.getOffsets().elements()[position];
                int oldPosition = joinChannelHash.put(sliceAddress, position);
                if (oldPosition >= 0) {
                    // link the new position to the old position
                    positionLinks.set(position, oldPosition);
                }
            }
        }

        protected Page computeNext()
        {
            if (!probeIterator.hasNext()) {
                return endOfData();
            }

            // create outputs
            BlockBuilder[] outputs = new BlockBuilder[tupleInfos.size()];
            for (int i = 0; i < outputs.length; i++) {
                outputs[i] = new BlockBuilder(tupleInfos.get(i));
            }

            // join probe pages with the hash
            while (!isFull(outputs) && probeIterator.hasNext()) {
                Page page = probeIterator.next();
                join(page, outputs);
            }

            // output data
            if (outputs[0].isEmpty()) {
                return endOfData();
            }

            Block[] blocks = new Block[outputs.length];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = outputs[i].build();
            }

            Page page = new Page(blocks);
            return page;
        }

        private void join(Page page, BlockBuilder[] outputs)
        {
            Block[] blocks = page.getBlocks();

            int probeChannelCount = blocks.length;
            BlockCursor[] cursors = new BlockCursor[probeChannelCount];
            for (int i = 0; i < probeChannelCount; i++) {
                cursors[i] = blocks[i].cursor();
            }
            UncompressedBlock probeJoinBlock = (UncompressedBlock) page.getBlock(probeJoinChannel);
            hashStrategy.setProbeSlice(probeJoinBlock.getSlice());

            BlockCursor probeJoinCursor = cursors[probeJoinChannel];

            int rows = page.getPositionCount();
            for (int position = 0; position < rows; position++) {
                for (BlockCursor cursor : cursors) {
                    checkState(cursor.advanceNextPosition());
                }

                int joinPosition = joinChannelHash.get(0xFF_FF_FF_FF_00_00_00_00L | probeJoinCursor.getRawOffset());
                while (joinPosition >= 0) {
                    for (int probeChannel = 0; probeChannel < cursors.length; probeChannel++) {
                        outputs[probeChannel].append(cursors[probeChannel].getTuple());
                    }

                    int outputIndex = probeChannelCount;
                    for (int buildChannel = 0; buildChannel < buildIndex.getChannelCount(); buildChannel++) {
                        if (buildChannel != buildJoinChannel) {
                            buildIndex.appendTupleTo(buildChannel, joinPosition, outputs[outputIndex++]);
                        }
                    }
                    joinPosition = positionLinks.getInt(joinPosition);
                }
            }

            for (BlockCursor cursor : cursors) {
                checkState(!cursor.advanceNextPosition());
            }
        }

        private boolean isFull(BlockBuilder... outputs)
        {
            if (outputs == null) {
                return false;
            }
            for (BlockBuilder output : outputs) {
                if (output.isFull()) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class SliceHashStrategy
            implements Strategy
    {
        private final TupleInfo tupleInfo;
        private final Slice[] slices;
        private Slice probeSlice;

        public SliceHashStrategy(TupleInfo tupleInfo, Slice[] slices)
        {
            this.tupleInfo = tupleInfo;
            this.slices = slices;
        }

        public void setProbeSlice(Slice probeSlice)
        {
            this.probeSlice = probeSlice;
        }

        @Override
        public int hashCode(long sliceAddress)
        {
            Slice slice = getSlice(sliceAddress);
            int offset = (int) sliceAddress;
            int length = tupleInfo.size(slice, offset);
            int hashCode = slice.hashCode(offset, length);
            return hashCode;
        }

        @Override
        public boolean equals(long leftSliceAddress, long rightSliceAddress)
        {
            Slice leftSlice = getSlice(leftSliceAddress);
            int leftOffset = (int) leftSliceAddress;
            int leftLength = tupleInfo.size(leftSlice, leftOffset);

            Slice rightSlice = getSlice(rightSliceAddress);
            int rightOffset = (int) rightSliceAddress;
            int rightLength = tupleInfo.size(rightSlice, rightOffset);

            return leftSlice.equals(leftOffset, leftLength, rightSlice, rightOffset, rightLength);

        }

        private Slice getSlice(long sliceAddress)
        {
            int sliceIndex = (int) (sliceAddress >> 32);
            Slice slice;
            if (sliceIndex == 0xFF_FF_FF_FF) {
                slice = probeSlice;
            }
            else {
                slice = slices[sliceIndex];
            }
            return slice;
        }
    }
}
