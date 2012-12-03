package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class HashJoinOperator
        implements Operator
{
    private final Operator probeSource;
    private final int probeJoinChannel;
    private final List<TupleInfo> tupleInfos;
    private final SourceHashProvider sourceHashProvider;
    private final int channelCount;

    public HashJoinOperator(SourceHashProvider sourceHashProvider, Operator probeSource, int probeJoinChannel)
    {
        Preconditions.checkNotNull(sourceHashProvider, "sourceHashProvider is null");
        Preconditions.checkNotNull(probeSource, "probeSource is null");
        Preconditions.checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");

        this.sourceHashProvider = sourceHashProvider;
        this.probeSource = probeSource;
        this.probeJoinChannel = probeJoinChannel;

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        tupleInfos.addAll(probeSource.getTupleInfos());
        int buildChannel = 0;
        // todo planner should choose which channels are preserved/dropped
        for (TupleInfo tupleInfo : this.sourceHashProvider.getTupleInfos()) {
            if (buildChannel != this.sourceHashProvider.getHashChannel()) {
                tupleInfos.add(tupleInfo);
            }
            buildChannel++;
        }
        this.tupleInfos = tupleInfos.build();
        channelCount = this.sourceHashProvider.getChannelCount() + this.probeSource.getChannelCount() - 1;
    }

    @Override
    public int getChannelCount()
    {
        return channelCount;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new HashJoinIterator(tupleInfos, probeSource, probeJoinChannel, sourceHashProvider);
    }

    private static class HashJoinIterator
            extends AbstractIterator<Page>
    {
        private final List<TupleInfo> tupleInfos;

        private final Iterator<Page> probeIterator;
        private final int probeJoinChannel;

        private final SourceHash hash;

        private final BlockCursor[] cursors;
        private int joinPosition = -1;

        private HashJoinIterator(List<TupleInfo> tupleInfos, Operator probeSource, int probeJoinChannel, SourceHashProvider sourceHashProvider)
        {
            this.tupleInfos = tupleInfos;
            probeIterator = probeSource.iterator();
            this.probeJoinChannel = probeJoinChannel;

            hash = sourceHashProvider.get();
            cursors = new BlockCursor[probeSource.getChannelCount()];
        }

        protected Page computeNext()
        {
            // create output
            PageBuilder pageBuilder = new PageBuilder(tupleInfos);

            // join probe pages with the hash
            while (joinCurrentPosition(pageBuilder)) {
                // advance cursors (only if we have initialized the cursors)
                if (cursors[0] == null || !advanceNextPosition()) {
                    // advance failed, do we have more cursors
                    if (!probeIterator.hasNext()) {
                        break;
                    }

                    // open next cursor
                    Page page = probeIterator.next();
                    for (int i = 0; i < page.getChannelCount(); i++) {
                        cursors[i] = page.getBlock(i).cursor();
                        cursors[i].advanceNextPosition();
                    }

                    // set hashing strategy to use probe block
                    UncompressedBlock probeJoinBlock = (UncompressedBlock) page.getBlock(probeJoinChannel);
                    hash.setProbeSlice(probeJoinBlock.getSlice());
                }

                // update join position
                joinPosition = hash.getJoinPosition(cursors[probeJoinChannel]);
            }

            // output data
            if (pageBuilder.isEmpty()) {
                return endOfData();
            }
            Page page = pageBuilder.build();
            return page;
        }

        private boolean joinCurrentPosition(PageBuilder pageBuilder)
        {
            // while we have a position to join against...
            while (joinPosition >= 0) {
                // write probe columns
                for (int probeChannel = 0; probeChannel < cursors.length; probeChannel++) {
                    cursors[probeChannel].appendTupleTo(pageBuilder.getBlockBuilder(probeChannel));
                }

                // write build columns
                int outputIndex = cursors.length;
                for (int buildChannel = 0; buildChannel < hash.getChannelCount(); buildChannel++) {
                    if (buildChannel != hash.getHashChannel()) {
                        hash.appendTupleTo(buildChannel, joinPosition, pageBuilder.getBlockBuilder(outputIndex));
                        outputIndex++;
                    }
                }

                // get next join position for this row
                joinPosition = hash.getNextJoinPosition(joinPosition);
                if (pageBuilder.isFull()) {
                    return false;
                }
            }
            return true;
        }

        public boolean advanceNextPosition()
        {
            if (cursors[0].advanceNextPosition()) {
                for (int i = 1; i < cursors.length; i++) {
                    checkState(cursors[i].advanceNextPosition());
                }
                return true;
            }
            else {
                for (int i = 1; i < cursors.length; i++) {
                    checkState(!cursors[i].advanceNextPosition());
                }
                return false;
            }
        }
    }
}
