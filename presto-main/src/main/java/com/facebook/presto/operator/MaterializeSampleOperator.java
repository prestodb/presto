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
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MaterializeSampleOperator
        implements Operator
{
    public static class MaterializeSampleOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final int sampleWeightChannel;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public MaterializeSampleOperatorFactory(int operatorId, List<TupleInfo> outputTupleInfos, int sampleWeightChannel)
        {
            this.operatorId = operatorId;
            this.sampleWeightChannel = sampleWeightChannel;
            this.tupleInfos = ImmutableList.copyOf(checkNotNull(outputTupleInfos, "outputTupleInfos is null"));
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, MaterializeSampleOperator.class.getSimpleName());
            return new MaterializeSampleOperator(operatorContext, tupleInfos, sampleWeightChannel);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<TupleInfo> tupleInfos;
    private final int sampleWeightChannel;
    private boolean finishing;
    private BlockCursor[] cursors;
    private BlockCursor sampleWeightCursor;
    private long remainingWeight;
    private PageBuilder pageBuilder;

    public MaterializeSampleOperator(OperatorContext operatorContext, List<TupleInfo> tupleInfos, int sampleWeightChannel)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.sampleWeightChannel = sampleWeightChannel;
        this.tupleInfos = ImmutableList.copyOf(checkNotNull(tupleInfos, "tupleInfos is null"));
        this.pageBuilder = new PageBuilder(tupleInfos);
        this.cursors = new BlockCursor[tupleInfos.size()];
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
        return finishing && sampleWeightCursor == null && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || sampleWeightCursor != null) {
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(sampleWeightCursor == null, "Current page has not been completely processed yet");

        BlockCursor[] cursors = new BlockCursor[page.getChannelCount()];
        for (int i = 0; i < cursors.length; i++) {
            cursors[i] = page.getBlock(i).cursor();
        }

        this.sampleWeightCursor = cursors[sampleWeightChannel];

        for (int i = 0, j = 0; i < cursors.length; i++) {
            if (i == sampleWeightChannel) {
                continue;
            }
            this.cursors[j] = cursors[i];
            j++;
        }
    }

    private boolean advance()
    {
        if (remainingWeight > 0) {
            remainingWeight--;
            return true;
        }

        if (sampleWeightCursor == null) {
            return false;
        }

        boolean advanced;
        // Read rows until we find one that has a non-zero weight
        do {
            advanced = sampleWeightCursor.advanceNextPosition();
            for (BlockCursor cursor : cursors) {
                checkState(advanced == cursor.advanceNextPosition());
            }
            checkState(!(advanced && sampleWeightCursor.isNull()), "Encountered NULL sample weight");
        } while(advanced && sampleWeightCursor.getLong() == 0);

        if (!advanced) {
            sampleWeightCursor = null;
            Arrays.fill(cursors, null);
        }
        else {
            remainingWeight = sampleWeightCursor.getLong() - 1;
        }

        return advanced;
    }

    @Override
    public Page getOutput()
    {
        while (!pageBuilder.isFull() && advance()) {
            // We might be outputting empty rows, if $sampleWeight is the only column (such as in a COUNT(*) query)
            pageBuilder.declarePosition();

            for (int i = 0; i < cursors.length; i++) {
                cursors[i].appendTupleTo(pageBuilder.getBlockBuilder(i));
            }
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty() && sampleWeightCursor == null)) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }
}
