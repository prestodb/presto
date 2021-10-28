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
package com.facebook.presto.operator.unnest;

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.repartition.PartitionedOutputOperator;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.common.array.Arrays.ExpansionOption.INITIALIZE;
import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Integer.max;
import static java.util.Objects.requireNonNull;

public class UnnestOperator
        implements Operator
{
    public static class UnnestOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Integer> replicateChannels;
        private final List<Type> replicateTypes;
        private final List<Integer> unnestChannels;
        private final List<Type> unnestTypes;
        private final boolean withOrdinality;
        private boolean closed;

        public UnnestOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Integer> replicateChannels,
                List<Type> replicateTypes,
                List<Integer> unnestChannels,
                List<Type> unnestTypes,
                boolean withOrdinality)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.replicateChannels = ImmutableList.copyOf(requireNonNull(replicateChannels, "replicateChannels is null"));
            this.replicateTypes = ImmutableList.copyOf(requireNonNull(replicateTypes, "replicateTypes is null"));
            checkArgument(replicateChannels.size() == replicateTypes.size(), "replicateChannels and replicateTypes do not match");
            this.unnestChannels = ImmutableList.copyOf(requireNonNull(unnestChannels, "unnestChannels is null"));
            this.unnestTypes = ImmutableList.copyOf(requireNonNull(unnestTypes, "unnestTypes is null"));
            checkArgument(unnestChannels.size() == unnestTypes.size(), "unnestChannels and unnestTypes do not match");
            this.withOrdinality = withOrdinality;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            return createOperator(driverContext, SystemSessionProperties.isLegacyUnnest(driverContext.getSession()));
        }

        @VisibleForTesting
        public Operator createOperator(DriverContext driverContext, boolean legacyUnnest)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, UnnestOperator.class.getSimpleName());
            return new UnnestOperator(operatorContext, replicateChannels, replicateTypes, unnestChannels, unnestTypes, withOrdinality, legacyUnnest);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new UnnestOperator.UnnestOperatorFactory(operatorId, planNodeId, replicateChannels, replicateTypes, unnestChannels, unnestTypes, withOrdinality);
        }
    }

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(UnnestOperator.class).instanceSize();
    private static final int MAX_ROWS_PER_BLOCK = 1000;

    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final List<Integer> replicateChannels;
    private final List<Type> replicateTypes;
    private final List<ReplicatedBlockBuilder> replicatedBlockBuilders;
    private final List<Integer> unnestChannels;
    private final List<Type> unnestTypes;
    private final List<Unnester> unnesters;
    private final boolean withOrdinality;
    private final int outputChannelCount;

    private boolean finishing;
    private Page currentPage;
    private int currentPosition;
    private int[] maxLengths = new int[0];
    private int currentBatchTotalLength;

    public UnnestOperator(OperatorContext operatorContext, List<Integer> replicateChannels, List<Type> replicateTypes, List<Integer> unnestChannels, List<Type> unnestTypes, boolean withOrdinality, boolean isLegacyUnnest)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(PartitionedOutputOperator.class.getSimpleName());

        this.replicateChannels = ImmutableList.copyOf(requireNonNull(replicateChannels, "replicateChannels is null"));
        this.replicateTypes = ImmutableList.copyOf(requireNonNull(replicateTypes, "replicateTypes is null"));
        checkArgument(replicateChannels.size() == replicateTypes.size(), "replicate channels or types has wrong size");
        this.replicatedBlockBuilders = replicateTypes.stream()
                .map(type -> new ReplicatedBlockBuilder())
                .collect(toImmutableList());

        this.unnestChannels = ImmutableList.copyOf(requireNonNull(unnestChannels, "unnestChannels is null"));
        this.unnestTypes = ImmutableList.copyOf(requireNonNull(unnestTypes, "unnestTypes is null"));
        checkArgument(unnestChannels.size() == unnestTypes.size(), "unnest channels or types has wrong size");
        this.unnesters = unnestTypes.stream()
                .map(nestedType -> createUnnester(nestedType, isLegacyUnnest))
                .collect(toImmutableList());

        this.withOrdinality = withOrdinality;
        this.outputChannelCount = unnesters.stream().mapToInt(Unnester::getChannelCount).sum() + replicateTypes.size() + (withOrdinality ? 1 : 0);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && currentPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && currentPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(currentPage == null, "currentPage is not null");

        currentPage = page;
        currentPosition = 0;

        resetBlockBuilders();

        systemMemoryContext.setBytes(getRetainedSizeInBytes());
    }

    @Override
    public Page getOutput()
    {
        if (currentPage == null) {
            return null;
        }

        int positionCount = currentPage.getPositionCount();
        int batchSize = calculateNextBatchSize();
        Block[] outputBlocks = buildOutputBlocks(batchSize);

        if (currentPosition == positionCount) {
            currentPage = null;
            currentPosition = 0;
        }

        return new Page(outputBlocks);
    }

    private static Unnester createUnnester(Type nestedType, boolean isLegacyUnnest)
    {
        if (nestedType instanceof ArrayType) {
            Type elementType = ((ArrayType) nestedType).getElementType();

            if (!isLegacyUnnest && elementType instanceof RowType) {
                return new ArrayOfRowsUnnester(elementType.getTypeParameters().size());
            }
            else {
                return new ArrayUnnester();
            }
        }
        else if (nestedType instanceof MapType) {
            return new MapUnnester();
        }
        else {
            throw new IllegalArgumentException("Cannot unnest type: " + nestedType);
        }
    }

    private void resetBlockBuilders()
    {
        for (int i = 0; i < replicateTypes.size(); i++) {
            Block newInputBlock = currentPage.getBlock(replicateChannels.get(i));
            replicatedBlockBuilders.get(i).resetInputBlock(newInputBlock);
        }

        int positionCount = currentPage.getPositionCount();
        maxLengths = ensureCapacity(maxLengths, positionCount, SMALL, INITIALIZE);

        for (int i = 0; i < unnestTypes.size(); i++) {
            int inputChannel = unnestChannels.get(i);
            Block unnestChannelInputBlock = currentPage.getBlock(inputChannel);
            Unnester unnester = unnesters.get(i);

            unnester.resetInput(unnestChannelInputBlock);

            int[] lengths = unnester.getLengths();
            for (int j = 0; j < positionCount; j++) {
                maxLengths[j] = max(maxLengths[j], lengths[j]);
            }
        }
    }

    private int calculateNextBatchSize()
    {
        int positionCount = currentPage.getPositionCount();

        int totalLengths = 0;
        int position = currentPosition;
        while (position < positionCount) {
            int length = maxLengths[position];
            if (totalLengths + length >= MAX_ROWS_PER_BLOCK) {
                break;
            }
            totalLengths += length;
            position++;
        }

        // grab at least a single position
        if (position == currentPosition) {
            currentBatchTotalLength = maxLengths[currentPosition];
            return 1;
        }

        currentBatchTotalLength = totalLengths;
        return position - currentPosition;
    }

    private Block[] buildOutputBlocks(int batchSize)
    {
        Block[] outputBlocks = new Block[outputChannelCount];
        int channel = 0;

        for (int replicateIndex = 0; replicateIndex < replicateTypes.size(); replicateIndex++) {
            outputBlocks[channel++] = replicatedBlockBuilders.get(replicateIndex).buildOutputBlock(maxLengths, currentPosition, batchSize, currentBatchTotalLength);
        }

        for (int unnestIndex = 0; unnestIndex < unnesters.size(); unnestIndex++) {
            Unnester unnester = unnesters.get(unnestIndex);
            Block[] block = unnester.buildOutputBlocks(maxLengths, currentPosition, batchSize, currentBatchTotalLength);
            for (int j = 0; j < unnester.getChannelCount(); j++) {
                outputBlocks[channel++] = block[j];
            }
        }

        if (withOrdinality) {
            outputBlocks[channel] = buildOrdinalityOutputBlock(maxLengths, currentPosition, batchSize, currentBatchTotalLength);
        }

        currentPosition += batchSize;

        return outputBlocks;
    }

    private static Block buildOrdinalityOutputBlock(int[] maxEntries, int offset, int length, int totalEntriesForBatch)
    {
        long[] values = new long[totalEntriesForBatch];
        int index = 0;
        for (int i = 0; i < length; i++) {
            int curEntries = maxEntries[offset + i];
            for (int j = 1; j <= curEntries; j++) {
                values[index++] = j;
            }
        }

        return new LongArrayBlock(totalEntriesForBatch, Optional.empty(), values);
    }

    private long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(maxLengths) + currentPage.getRetainedSizeInBytes();
        for (Unnester unnester : unnesters) {
            size += unnester.getRetainedSizeInBytes();
        }
        return size;
    }
}
