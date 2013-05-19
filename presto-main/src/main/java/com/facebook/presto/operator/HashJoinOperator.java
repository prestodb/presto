package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class HashJoinOperator
        implements Operator
{
    private final Operator probeSource;
    private final int probeJoinChannel;
    private final List<TupleInfo> tupleInfos;
    private final SourceHashProvider sourceHashProvider;

    public HashJoinOperator(SourceHashProvider sourceHashProvider, Operator probeSource, int probeJoinChannel)
    {
        // todo pass in desired projection
        Preconditions.checkNotNull(sourceHashProvider, "sourceHashProvider is null");
        Preconditions.checkNotNull(probeSource, "probeSource is null");
        Preconditions.checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");

        this.sourceHashProvider = sourceHashProvider;
        this.probeSource = probeSource;
        this.probeJoinChannel = probeJoinChannel;

        this.tupleInfos = ImmutableList.<TupleInfo>builder()
                .addAll(probeSource.getTupleInfos())
                .addAll(sourceHashProvider.getTupleInfos())
                .build();
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        return new HashJoinIterator(tupleInfos, probeSource, probeJoinChannel, sourceHashProvider, operatorStats);
    }

    private static class HashJoinIterator
            extends AbstractPageIterator
    {
        private final PageIterator probeIterator;
        private final int probeJoinChannel;
        private final SourceHashProvider sourceHashProvider;

        private SourceHash hash;

        private final BlockCursor[] cursors;
        private final PageBuilder pageBuilder;
        private int joinPosition = -1;

        private HashJoinIterator(List<TupleInfo> tupleInfos, Operator probeSource, int probeJoinChannel, SourceHashProvider sourceHashProvider, OperatorStats operatorStats)
        {
            super(tupleInfos);

            this.sourceHashProvider = sourceHashProvider;

            this.probeIterator = probeSource.iterator(operatorStats);
            this.probeJoinChannel = probeJoinChannel;

            this.cursors = new BlockCursor[probeSource.getChannelCount()];
            this.pageBuilder = new PageBuilder(getTupleInfos());
        }

        protected Page computeNext()
        {
            if (hash == null) {
                hash = sourceHashProvider.get();
            }

            // join probe pages with the hash
            pageBuilder.reset();
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

        @Override
        protected void doClose()
        {
            sourceHashProvider.close();
            probeIterator.close();
        }

        private boolean joinCurrentPosition(PageBuilder pageBuilder)
        {
            // while we have a position to join against...
            while (joinPosition >= 0) {
                // write probe columns
                int outputIndex = 0;
                for (BlockCursor cursor : cursors) {
                    cursor.appendTupleTo(pageBuilder.getBlockBuilder(outputIndex));
                    outputIndex++;
                }

                // write build columns
                for (int buildChannel = 0; buildChannel < hash.getChannelCount(); buildChannel++) {
                    hash.appendTupleTo(buildChannel, joinPosition, pageBuilder.getBlockBuilder(outputIndex));
                    outputIndex++;
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
