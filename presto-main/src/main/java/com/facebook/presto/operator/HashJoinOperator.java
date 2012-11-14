package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

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
        private final BlocksHash hash;

        private final int buildJoinChannel;

        private HashJoinIterator(List<TupleInfo> tupleInfos, Operator buildSource, int buildJoinChannel, Operator probeSource, int probeJoinChannel, int expectedPositions)
        {
            this.tupleInfos = tupleInfos;
            this.buildJoinChannel = buildJoinChannel;
            probeIterator = probeSource.iterator();
            this.probeJoinChannel = probeJoinChannel;

            // index build channel
            buildIndex = new PagesIndex(buildSource, expectedPositions);
            hash = new BlocksHash(buildIndex.getIndex(buildJoinChannel));
        }

        protected Page computeNext()
        {
            if (!probeIterator.hasNext()) {
                return endOfData();
            }

            // create output
            PageBuilder pageBuilder = new PageBuilder(tupleInfos);

            // join probe pages with the hash
            while (!pageBuilder.isFull() && probeIterator.hasNext()) {
                Page page = probeIterator.next();
                join(page, pageBuilder);
            }

            // output data
            if (pageBuilder.isEmpty()) {
                return endOfData();
            }
            Page page = pageBuilder.build();
            return page;
        }

        private void join(Page page, PageBuilder pageBuilder)
        {
            // open cursors
            BlockCursor[] cursors = new BlockCursor[page.getChannelCount()];
            Block[] blocks = page.getBlocks();
            for (int i = 0; i < page.getChannelCount(); i++) {
                cursors[i] = blocks[i].cursor();
            }

            // set hashing strategy to use probe block
            UncompressedBlock probeJoinBlock = (UncompressedBlock) page.getBlock(probeJoinChannel);
            hash.setProbeSlice(probeJoinBlock.getSlice());

            int rows = page.getPositionCount();
            for (int position = 0; position < rows; position++) {
                // advance cursors
                for (BlockCursor cursor : cursors) {
                    checkState(cursor.advanceNextPosition());
                }

                // lookup the position of the "first" joined row
                int joinPosition = hash.get(cursors[probeJoinChannel]);
                // while we have a position to join against...
                while (joinPosition >= 0) {
                    for (int probeChannel = 0; probeChannel < cursors.length; probeChannel++) {
                        cursors[probeChannel].appendTupleTo(pageBuilder.getBlockBuilder(probeChannel));
                    }

                    int outputIndex = page.getChannelCount();
                    for (int buildChannel = 0; buildChannel < buildIndex.getChannelCount(); buildChannel++) {
                        if (buildChannel != buildJoinChannel) {
                            buildIndex.appendTupleTo(buildChannel, joinPosition, pageBuilder.getBlockBuilder(outputIndex));
                            outputIndex++;
                        }
                    }
                    joinPosition = hash.getNextPosition(joinPosition);
                }
            }

            for (BlockCursor cursor : cursors) {
                checkState(!cursor.advanceNextPosition());
            }
        }
    }
}
