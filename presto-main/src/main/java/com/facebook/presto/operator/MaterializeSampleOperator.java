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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
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
        private final List<Type> types;
        private boolean closed;

        public MaterializeSampleOperatorFactory(int operatorId, List<? extends Type> outputTypes, int sampleWeightChannel)
        {
            this.operatorId = operatorId;
            this.sampleWeightChannel = sampleWeightChannel;
            this.types = ImmutableList.copyOf(checkNotNull(outputTypes, "outputTypes is null"));
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, MaterializeSampleOperator.class.getSimpleName());
            return new MaterializeSampleOperator(operatorContext, types, sampleWeightChannel);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final int sampleWeightChannel;
    private boolean finishing;
    private int position = -1;
    private Block[] blocks;
    private Block sampleWeightBlock;
    private long remainingWeight;
    private PageBuilder pageBuilder;

    public MaterializeSampleOperator(OperatorContext operatorContext, List<Type> types, int sampleWeightChannel)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.sampleWeightChannel = sampleWeightChannel;
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.pageBuilder = new PageBuilder(types);
        this.blocks = new Block[types.size()];
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
        return finishing && sampleWeightBlock == null && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || sampleWeightBlock != null) {
            return false;
        }
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(sampleWeightBlock == null, "Current page has not been completely processed yet");

        this.position = -1;
        this.sampleWeightBlock = page.getBlock(sampleWeightChannel);

        for (int i = 0, j = 0; i < page.getChannelCount(); i++) {
            if (i == sampleWeightChannel) {
                continue;
            }
            this.blocks[j] = page.getBlock(i);
            j++;
        }
    }

    private boolean advance()
    {
        if (remainingWeight > 0) {
            remainingWeight--;
            return true;
        }

        if (sampleWeightBlock == null) {
            return false;
        }

        // Read rows until we find one that has a non-zero weight
        position++;
        while (position < sampleWeightBlock.getPositionCount()) {
            checkState(!(sampleWeightBlock.isNull(position)), "Encountered NULL sample weight");
            if (sampleWeightBlock.getLong(position) != 0) {
                remainingWeight = sampleWeightBlock.getLong(position) - 1;
                return true;
            }
            position++;
        }
        sampleWeightBlock = null;
        Arrays.fill(blocks, null);
        return false;
    }

    @Override
    public Page getOutput()
    {
        while (!pageBuilder.isFull() && advance()) {
            // We might be outputting empty rows, if $sampleWeight is the only column (such as in a COUNT(*) query)
            pageBuilder.declarePosition();

            for (int i = 0; i < blocks.length; i++) {
                blocks[i].appendTo(position, pageBuilder.getBlockBuilder(i));
            }
        }

        // only flush full pages unless we are done
        if (pageBuilder.isFull() || (finishing && !pageBuilder.isEmpty() && sampleWeightBlock == null)) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        return null;
    }
}
