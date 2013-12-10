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
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.util.MoreFutures.tryGetUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class LookupJoinOperator
        implements Operator
{
    public static LookupJoinOperatorFactory innerJoin(int operatorId, LookupSourceSupplier lookupSourceSupplier, List<TupleInfo> probeTupleInfos, List<Integer> probeJoinChannels)
    {
        return new LookupJoinOperatorFactory(operatorId, lookupSourceSupplier, probeTupleInfos, probeJoinChannels, false);
    }

    public static LookupJoinOperatorFactory outerJoin(int operatorId, LookupSourceSupplier lookupSourceSupplier, List<TupleInfo> probeTupleInfos, List<Integer> probeJoinChannels)
    {
        return new LookupJoinOperatorFactory(operatorId, lookupSourceSupplier, probeTupleInfos, probeJoinChannels, true);
    }

    public static class LookupJoinOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final LookupSourceSupplier lookupSourceSupplier;
        private final List<TupleInfo> probeTupleInfos;
        private final List<Integer> probeJoinChannels;
        private final boolean enableOuterJoin;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public LookupJoinOperatorFactory(int operatorId, LookupSourceSupplier lookupSourceSupplier, List<TupleInfo> probeTupleInfos, List<Integer> probeJoinChannels, boolean enableOuterJoin)
        {
            this.operatorId = operatorId;
            this.lookupSourceSupplier = lookupSourceSupplier;
            this.probeTupleInfos = probeTupleInfos;
            this.probeJoinChannels = probeJoinChannels;
            this.enableOuterJoin = enableOuterJoin;

            this.tupleInfos = ImmutableList.<TupleInfo>builder()
                    .addAll(probeTupleInfos)
                    .addAll(lookupSourceSupplier.getTupleInfos())
                    .build();
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, LookupJoinOperator.class.getSimpleName());
            return new LookupJoinOperator(operatorContext, lookupSourceSupplier, probeTupleInfos, probeJoinChannels, enableOuterJoin);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final ListenableFuture<LookupSource> lookupSourceFuture;

    private final OperatorContext operatorContext;
    private final int[] probeJoinChannels;
    private final boolean enableOuterJoin;
    private final List<TupleInfo> tupleInfos;

    private final BlockCursor[] cursors;
    private final BlockCursor[] probeJoinCursors;

    private final PageBuilder pageBuilder;

    private LookupSource lookupSource;
    private boolean finishing;
    private long joinPosition = -1;

    public LookupJoinOperator(OperatorContext operatorContext, LookupSourceSupplier lookupSourceSupplier, List<TupleInfo> probeTupleInfos, List<Integer> probeJoinChannels, boolean enableOuterJoin)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        // todo pass in desired projection
        checkNotNull(lookupSourceSupplier, "lookupSourceSupplier is null");
        checkNotNull(probeTupleInfos, "probeTupleInfos is null");

        this.lookupSourceFuture = lookupSourceSupplier.getLookupSource(operatorContext);
        this.probeJoinChannels = Ints.toArray(probeJoinChannels);
        this.enableOuterJoin = enableOuterJoin;

        this.tupleInfos = ImmutableList.<TupleInfo>builder()
                .addAll(probeTupleInfos)
                .addAll(lookupSourceSupplier.getTupleInfos())
                .build();
        this.pageBuilder = new PageBuilder(tupleInfos);

        this.cursors = new BlockCursor[probeTupleInfos.size()];
        this.probeJoinCursors = new BlockCursor[probeJoinChannels.size()];
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
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
            lookupSource = null;

            Arrays.fill(cursors, null);
            Arrays.fill(probeJoinCursors, null);
            pageBuilder.reset();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return lookupSourceFuture;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }

        if (lookupSource == null) {
            lookupSource = tryGetUnchecked(lookupSourceFuture);
        }
        return lookupSource != null && cursors[0] == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(lookupSource != null, "lookupSource has not been built yet");
        checkState(cursors[0] == null, "Current page has not been completely processed yet");

        // open cursors
        for (int i = 0; i < page.getChannelCount(); i++) {
            cursors[i] = page.getBlock(i).cursor();
        }
        for (int i = 0; i < probeJoinChannels.length; i++) {
            probeJoinCursors[i] = cursors[probeJoinChannels[i]];
        }

        // initialize to invalid join position to force output code to advance the cursors
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
            for (int i = 0; i < cursors.length; i++) {
                cursors[i].appendTupleTo(pageBuilder.getBlockBuilder(i));
            }

            // write build columns
            lookupSource.appendTupleTo(joinPosition, pageBuilder, cursors.length);

            // get next join position for this row
            joinPosition = lookupSource.getNextJoinPosition(joinPosition);
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
        if (currentRowJoinPositionContainsNull()) {
            // Null values will never match in an equijoin, so just omit them from the probe side
            joinPosition = -1;
        }
        else {
            joinPosition = lookupSource.getJoinPosition(probeJoinCursors);
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
            for (int buildChannel = 0; buildChannel < lookupSource.getChannelCount(); buildChannel++) {
                pageBuilder.getBlockBuilder(outputIndex).appendNull();
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
            Arrays.fill(cursors, null);
            Arrays.fill(probeJoinCursors, null);
        }

        return advanced;
    }

    private boolean currentRowJoinPositionContainsNull()
    {
        for (BlockCursor probeJoinCursor : probeJoinCursors) {
            if (probeJoinCursor.isNull()) {
                return true;
            }
        }
        return false;
    }
}
