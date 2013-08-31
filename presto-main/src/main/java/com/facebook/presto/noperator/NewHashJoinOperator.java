package com.facebook.presto.noperator;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashSupplier;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.util.MoreFutures.tryGetUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NewHashJoinOperator
        implements NewOperator
{
    public static NewHashJoinOperatorFactory innerJoin(NewHashSupplier hashSupplier, List<TupleInfo> probeTupleInfos, int probeJoinChannel)
    {
        return new NewHashJoinOperatorFactory(hashSupplier, probeTupleInfos, probeJoinChannel, false);
    }

    public static NewHashJoinOperatorFactory outerJoin(NewHashSupplier hashSupplier, List<TupleInfo> probeTupleInfos, int probeJoinChannel)
    {
        return new NewHashJoinOperatorFactory(hashSupplier, probeTupleInfos, probeJoinChannel, true);
    }

    public static class NewHashJoinOperatorFactory
            implements NewOperatorFactory
    {
        private final NewHashSupplier hashSupplier;
        private final List<TupleInfo> probeTupleInfos;
        private final int probeJoinChannel;
        private final boolean enableOuterJoin;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public NewHashJoinOperatorFactory(NewHashSupplier hashSupplier, List<TupleInfo> probeTupleInfos, int probeJoinChannel, boolean enableOuterJoin)
        {
            this.hashSupplier = hashSupplier;
            this.probeTupleInfos = probeTupleInfos;
            this.probeJoinChannel = probeJoinChannel;
            this.enableOuterJoin = enableOuterJoin;

            this.tupleInfos = ImmutableList.<TupleInfo>builder()
                    .addAll(probeTupleInfos)
                    .addAll(hashSupplier.getTupleInfos())
                    .build();
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            checkState(!closed, "Factory is already closed");
            return new NewHashJoinOperator(hashSupplier, probeTupleInfos, probeJoinChannel, enableOuterJoin);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }


    private final ListenableFuture<NewSourceHash> sourceHashFuture;

    private final int probeJoinChannel;
    private final int probeJoinChannelFieldCount;
    private final boolean enableOuterJoin;
    private final List<TupleInfo> tupleInfos;

    private final BlockCursor[] cursors;

    private final PageBuilder pageBuilder;

    private NewSourceHash hash;
    private boolean finishing;
    private int joinPosition = -1;

    public NewHashJoinOperator(NewHashSupplier hashSupplier, List<TupleInfo> probeTupleInfos, int probeJoinChannel, boolean enableOuterJoin)
    {
        // todo pass in desired projection
        checkNotNull(hashSupplier, "hashSupplier is null");
        checkNotNull(probeTupleInfos, "probeTupleInfos is null");
        Preconditions.checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");

        this.sourceHashFuture = hashSupplier.getSourceHash();
        this.probeJoinChannel = probeJoinChannel;
        this.probeJoinChannelFieldCount = probeTupleInfos.get(probeJoinChannel).getFieldCount();
        this.enableOuterJoin = enableOuterJoin;

        this.tupleInfos = ImmutableList.<TupleInfo>builder()
                .addAll(probeTupleInfos)
                .addAll(hashSupplier.getTupleInfos())
                .build();
        this.pageBuilder = new PageBuilder(tupleInfos);

        this.cursors = new BlockCursor[probeTupleInfos.size()];
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = finishing && cursors[0] == null && pageBuilder.isEmpty();

        // if finished drop references so memory is freed early
        if (finished) {
            hash = null;

            for (int i = 0; i < cursors.length; i++) {
                cursors[i] = null;
            }
            pageBuilder.reset();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return sourceHashFuture;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }

        if (hash == null) {
            hash = tryGetUnchecked(sourceHashFuture);
        }
        return hash != null && cursors[0] == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(hash != null, "Hash has not been built yet");
        checkState(cursors[0] == null, "Current page has not been completely processed yet");

        // open cursors
        for (int i = 0; i < page.getChannelCount(); i++) {
            cursors[i] = page.getBlock(i).cursor();
        }

        // set hashing strategy to use probe block
        UncompressedBlock probeJoinBlock = (UncompressedBlock) page.getBlock(probeJoinChannel);
        hash.setProbeSlice(probeJoinBlock.getSlice());

        // initialize to invalid join position to force, output code to advance the cursors
        joinPosition = -1;
    }

    @Override
    public Page getOutput()
    {
        // join probe page with the hash
        if (cursors[0] != null) {
            while (joinCurrentPosition()) {
                if (!advanceProbePosition()) {
                    break;
                }
                if (!outerJoinCurrentPosition()) {
                    break;
                }
            }
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty() && cursors[0] == null)) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }

    private boolean joinCurrentPosition()
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

    private boolean advanceProbePosition()
    {
        // advance cursors (only if we have initialized the cursors)
        if (!advanceNextCursorPosition()) {
            return false;
        }

        // update join position
        if (tupleContainsNull(cursors[probeJoinChannel])) {
            // Null values will never match in an equijoin, so just omit them from the probe side
            joinPosition = -1;
        }
        else {
            joinPosition = hash.getJoinPosition(cursors[probeJoinChannel]);
        }

        return true;
    }

    private boolean outerJoinCurrentPosition()
    {
        if (enableOuterJoin && joinPosition < 0) {
            // write probe columns
            int outputIndex = 0;
            for (BlockCursor cursor : cursors) {
                cursor.appendTupleTo(pageBuilder.getBlockBuilder(outputIndex));
                outputIndex++;
            }

            // write nulls into build columns
            for (int buildChannel = 0; buildChannel < hash.getChannelCount(); buildChannel++) {
                for (int i = 0; i < getTupleInfos().get(outputIndex).getTypes().size(); i++) {
                    pageBuilder.getBlockBuilder(outputIndex).appendNull();
                }
                outputIndex++;
            }
            if (pageBuilder.isFull()) {
                return false;
            }
        }
        return true;
    }

    public boolean advanceNextCursorPosition()
    {
        // advance all cursors
        boolean advanced = cursors[0].advanceNextPosition();
        for (int i = 1; i < cursors.length; i++) {
            checkState(advanced == cursors[i].advanceNextPosition());
        }

        // null out the cursors to signal the need for more input
        if (!advanced) {
            for (int i = 0; i < cursors.length; i++) {
                cursors[i] = null;
            }
        }

        return advanced;
    }

    private boolean tupleContainsNull(BlockCursor cursor)
    {
        boolean containsNull = false;
        for (int i = 0; i < probeJoinChannelFieldCount; i++) {
            containsNull |= cursor.isNull(i);
        }
        return containsNull;
    }
}
