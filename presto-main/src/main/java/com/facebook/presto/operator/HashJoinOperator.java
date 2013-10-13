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
import com.facebook.presto.operator.HashBuilderOperator.HashSupplier;
import com.facebook.presto.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.util.MoreFutures.tryGetUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class HashJoinOperator
        implements Operator
{
    public static HashJoinOperatorFactory innerJoin(int operatorId, HashSupplier hashSupplier, List<Type> probeTypes, List<Integer> probeJoinChannel)
    {
        return new HashJoinOperatorFactory(operatorId, hashSupplier, probeTypes, probeJoinChannel, false);
    }

    public static HashJoinOperatorFactory outerJoin(int operatorId, HashSupplier hashSupplier, List<Type> probeTypes, List<Integer> probeJoinChannel)
    {
        return new HashJoinOperatorFactory(operatorId, hashSupplier, probeTypes, probeJoinChannel, true);
    }

    public static class HashJoinOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final HashSupplier hashSupplier;
        private final List<Type> probeTypes;
        private final List<Integer> probeJoinChannels;
        private final boolean enableOuterJoin;
        private final List<Type> types;
        private boolean closed;

        public HashJoinOperatorFactory(int operatorId, HashSupplier hashSupplier, List<Type> probeTypes, List<Integer> probeJoinChannels, boolean enableOuterJoin)
        {
            this.operatorId = operatorId;
            this.hashSupplier = hashSupplier;
            this.probeTypes = probeTypes;
            this.probeJoinChannels = probeJoinChannels;
            this.enableOuterJoin = enableOuterJoin;

            this.types = ImmutableList.<Type>builder()
                    .addAll(probeTypes)
                    .addAll(hashSupplier.getTypes())
                    .build();
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, HashJoinOperator.class.getSimpleName());
            return new HashJoinOperator(operatorContext, hashSupplier, probeTypes, probeJoinChannels, enableOuterJoin);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final ListenableFuture<JoinHash> hashFuture;

    private final OperatorContext operatorContext;
    private final int[] probeJoinChannels;
    private final boolean enableOuterJoin;
    private final List<Type> types;

    private final BlockCursor[] cursors;

    private final PageBuilder pageBuilder;

    private JoinHash hash;
    private boolean finishing;
    private int joinPosition = -1;

    public HashJoinOperator(OperatorContext operatorContext, HashSupplier hashSupplier, List<Type> probeTypes, List<Integer> probeJoinChannels, boolean enableOuterJoin)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");

        // todo pass in desired projection
        checkNotNull(hashSupplier, "hashSupplier is null");
        checkNotNull(probeTypes, "probeTypes is null");

        this.hashFuture = hashSupplier.getSourceHash();
        this.probeJoinChannels = Ints.toArray(probeJoinChannels);
        this.enableOuterJoin = enableOuterJoin;

        this.types = ImmutableList.<Type>builder()
                .addAll(probeTypes)
                .addAll(hashSupplier.getTypes())
                .build();
        this.pageBuilder = new PageBuilder(types);

        this.cursors = new BlockCursor[probeTypes.size()];
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
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
        return hashFuture;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing) {
            return false;
        }

        if (hash == null) {
            hash = tryGetUnchecked(hashFuture);
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
        hash.setProbeCursors(cursors, probeJoinChannels);

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
                cursors[i].appendTo(pageBuilder.getBlockBuilder(i));
            }

            // write build columns
            hash.appendTo(joinPosition, pageBuilder, cursors.length);

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
        if (currentRowJoinPositionContainsNull()) {
            // Null values will never match in an equijoin, so just omit them from the probe side
            joinPosition = -1;
        }
        else {
            joinPosition = hash.getJoinPosition();
        }

        return true;
    }

    private boolean outerJoinCurrentPosition()
    {
        if (enableOuterJoin && joinPosition < 0) {
            // write probe columns
            int outputIndex = 0;
            for (BlockCursor cursor : cursors) {
                cursor.appendTo(pageBuilder.getBlockBuilder(outputIndex));
                outputIndex++;
            }

            // write nulls into build columns
            for (int buildChannel = 0; buildChannel < hash.getChannelCount(); buildChannel++) {
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
            for (int i = 0; i < cursors.length; i++) {
                cursors[i] = null;
            }
        }

        return advanced;
    }

    private boolean currentRowJoinPositionContainsNull()
    {
        for (int probeJoinChannel : probeJoinChannels) {
            if (cursors[probeJoinChannel].isNull()) {
                return true;
            }
        }
        return false;
    }
}
