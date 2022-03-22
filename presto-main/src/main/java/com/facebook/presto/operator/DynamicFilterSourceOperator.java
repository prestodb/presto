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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TypeUtils.isFloatingPointNaN;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * This operator acts as a simple "pass-through" pipe, while saving its input pages.
 * The collected pages' value are used for creating a run-time filtering constraint (for probe-side table scan in an inner join).
 * We record all values for the run-time filter only for small build-side pages (which should be the case when using "broadcast" join).
 * For large inputs on build side, we can optionally record the min and max values per channel for orderable types (except Double and Real).
 */
public class DynamicFilterSourceOperator
        implements Operator
{
    private static final int EXPECTED_BLOCK_BUILDER_SIZE = 8;

    public static class Channel
    {
        private final String filterId;
        private final Type type;
        private final int index;

        public Channel(String filterId, Type type, int index)
        {
            this.filterId = requireNonNull(filterId, "filterId is null");
            this.type = requireNonNull(type, "type is null");
            this.index = index;
        }

        public String getFilterId()
        {
            return filterId;
        }

        public Type getType()
        {
            return type;
        }

        public int getIndex()
        {
            return index;
        }
    }

    public static class DynamicFilterSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Consumer<TupleDomain<String>> dynamicPredicateConsumer;
        private final List<Channel> channels;
        private final int maxFilterPositionsCount;
        private final DataSize maxFilterSize;
        private final int minMaxCollectionLimit;

        private boolean closed;

        public DynamicFilterSourceOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Consumer<TupleDomain<String>> dynamicPredicateConsumer,
                List<Channel> channels,
                int maxFilterPositionsCount,
                DataSize maxFilterSize,
                int minMaxCollectionLimit)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.dynamicPredicateConsumer = requireNonNull(dynamicPredicateConsumer, "dynamicPredicateConsumer is null");
            this.channels = requireNonNull(channels, "channels is null");
            verify(
                    channels.stream().map(channel -> channel.getFilterId()).collect(toSet()).size() == channels.size(),
                    "duplicate dynamic filters are not allowed");
            verify(
                    channels.stream().map(channel -> channel.getIndex()).collect(toSet()).size() == channels.size(),
                    "duplicate channel indices are not allowed");
            this.maxFilterPositionsCount = maxFilterPositionsCount;
            this.maxFilterSize = maxFilterSize;
            this.minMaxCollectionLimit = minMaxCollectionLimit;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            return new DynamicFilterSourceOperator(
                    driverContext.addOperatorContext(operatorId, planNodeId, DynamicFilterSourceOperator.class.getSimpleName()),
                    dynamicPredicateConsumer,
                    channels,
                    planNodeId,
                    maxFilterPositionsCount,
                    maxFilterSize,
                    minMaxCollectionLimit);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Factory is already closed");
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("duplicate() is not supported for DynamicFilterSourceOperatorFactory");
        }
    }

    private final OperatorContext context;
    private final Consumer<TupleDomain<String>> dynamicPredicateConsumer;
    private final int maxFilterPositionsCount;
    private final long maxFilterSizeInBytes;
    private final List<Channel> channels;
    private final List<Integer> minMaxChannels;

    private boolean finished;
    private Page current;

    // May be dropped if the predicate becomes too large.
    @Nullable
    private BlockBuilder[] blockBuilders;
    @Nullable
    private TypedSet[] valueSets;

    private int minMaxCollectionLimit;
    @Nullable
    private Block[] minValues;
    @Nullable
    private Block[] maxValues;

    private DynamicFilterSourceOperator(
            OperatorContext context,
            Consumer<TupleDomain<String>> dynamicPredicateConsumer,
            List<Channel> channels,
            PlanNodeId planNodeId,
            int maxFilterPositionsCount,
            DataSize maxFilterSize,
            int minMaxCollectionLimit)
    {
        this.context = requireNonNull(context, "context is null");
        this.maxFilterPositionsCount = maxFilterPositionsCount;
        this.maxFilterSizeInBytes = maxFilterSize.toBytes();

        this.dynamicPredicateConsumer = requireNonNull(dynamicPredicateConsumer, "dynamicPredicateConsumer is null");
        this.channels = requireNonNull(channels, "channels is null");

        this.blockBuilders = new BlockBuilder[channels.size()];
        this.valueSets = new TypedSet[channels.size()];
        ImmutableList.Builder<Integer> minMaxChannelsBuilder = ImmutableList.builder();
        for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
            Type type = channels.get(channelIndex).getType();
            // Skipping DOUBLE and REAL in collectMinMaxValues to avoid dealing with NaN values
            if (minMaxCollectionLimit > 0 && type.isOrderable() && type != DOUBLE && type != REAL) {
                minMaxChannelsBuilder.add(channelIndex);
            }
            this.blockBuilders[channelIndex] = type.createBlockBuilder(null, EXPECTED_BLOCK_BUILDER_SIZE);
            this.valueSets[channelIndex] = new TypedSet(
                    type,
                    blockBuilders[channelIndex],
                    EXPECTED_BLOCK_BUILDER_SIZE,
                    String.format("DynamicFilterSourceOperator_%s_%d", planNodeId, channelIndex),
                    Optional.empty() /* maxBlockMemory */);
        }
        this.minMaxCollectionLimit = minMaxCollectionLimit;
        minMaxChannels = minMaxChannelsBuilder.build();
        if (!minMaxChannels.isEmpty()) {
            minValues = new Block[channels.size()];
            maxValues = new Block[channels.size()];
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return context;
    }

    @Override
    public boolean needsInput()
    {
        return current == null && !finished;
    }

    @Override
    public void addInput(Page page)
    {
        verify(!finished, "DynamicFilterSourceOperator: addInput() shouldn't not be called after finish()");
        current = page;
        if (valueSets == null) {
            // the exact predicate became too large.
            if (minValues == null) {
                // there are too many rows to collect min/max range
                return;
            }
            minMaxCollectionLimit -= page.getPositionCount();
            if (minMaxCollectionLimit < 0) {
                handleMinMaxCollectionLimitExceeded();
                return;
            }
            // the predicate became too large, record only min and max values for each orderable channel
            for (Integer channelIndex : minMaxChannels) {
                Block block = page.getBlock(channels.get(channelIndex).index);
                updateMinMaxValues(block, channelIndex);
            }
            return;
        }
        minMaxCollectionLimit -= page.getPositionCount();
        // TODO: we should account for the memory used for collecting build-side values using MemoryContext
        long filterSizeInBytes = 0;
        int filterPositionsCount = 0;
        // Collect only the columns which are relevant for the JOIN.
        for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
            Block block = page.getBlock(channels.get(channelIndex).getIndex());
            TypedSet valueSet = valueSets[channelIndex];
            for (int position = 0; position < block.getPositionCount(); ++position) {
                valueSet.add(block, position);
            }
            filterSizeInBytes += valueSet.getRetainedSizeInBytes();
            filterPositionsCount += valueSet.size();
        }
        if (filterPositionsCount > maxFilterPositionsCount || filterSizeInBytes > maxFilterSizeInBytes) {
            // The whole filter (summed over all columns) contains too much values or exceeds maxFilterSizeInBytes.
            handleTooLargePredicate();
        }
    }

    private void handleTooLargePredicate()
    {
        // The resulting predicate is too large
        if (minMaxChannels.isEmpty()) {
            // allow all probe-side values to be read.
            dynamicPredicateConsumer.accept(TupleDomain.all());
        }
        else {
            if (minMaxCollectionLimit < 0) {
                handleMinMaxCollectionLimitExceeded();
            }
            else {
                // convert to min/max per column for orderable types
                for (Integer channelIndex : minMaxChannels) {
                    Block block = blockBuilders[channelIndex].build();
                    updateMinMaxValues(block, channelIndex);
                }
            }
        }

        // Drop references to collected values.
        valueSets = null;
        blockBuilders = null;
    }

    private void handleMinMaxCollectionLimitExceeded()
    {
        // allow all probe-side values to be read.
        dynamicPredicateConsumer.accept(TupleDomain.all());
        // Drop references to collected values.
        minValues = null;
        maxValues = null;
    }

    private void updateMinMaxValues(Block block, int channelIndex)
    {
        checkState(minValues != null && maxValues != null);
        Type type = channels.get(channelIndex).type;
        int minValuePosition = -1;
        int maxValuePosition = -1;
        for (int position = 0; position < block.getPositionCount(); ++position) {
            if (block.isNull(position)) {
                continue;
            }
            if (minValuePosition == -1) {
                // First non-null value
                minValuePosition = position;
                maxValuePosition = position;
                continue;
            }
            if (type.compareTo(block, position, block, minValuePosition) < 0) {
                minValuePosition = position;
            }
            else if (type.compareTo(block, position, block, maxValuePosition) > 0) {
                maxValuePosition = position;
            }
        }
        if (minValuePosition == -1) {
            // all block values are nulls
            return;
        }
        if (minValues[channelIndex] == null) {
            // First Page with non-null value for this block
            minValues[channelIndex] = block.getSingleValueBlock(minValuePosition);
            maxValues[channelIndex] = block.getSingleValueBlock(maxValuePosition);
            return;
        }
        // Compare with min/max values from previous Pages
        Block currentMin = minValues[channelIndex];
        Block currentMax = maxValues[channelIndex];
        if (type.compareTo(block, minValuePosition, currentMin, 0) < 0) {
            minValues[channelIndex] = block.getSingleValueBlock(minValuePosition);
        }
        if (type.compareTo(block, maxValuePosition, currentMax, 0) > 0) {
            maxValues[channelIndex] = block.getSingleValueBlock(maxValuePosition);
        }
    }

    @Override
    public Page getOutput()
    {
        Page result = current;
        current = null;
        return result;
    }

    @Override
    public void finish()
    {
        if (finished) {
            // NOTE: finish() may be called multiple times (see comment at Driver::processInternal).
            return;
        }
        finished = true;
        ImmutableMap.Builder<String, Domain> domainsBuilder = ImmutableMap.builder();
        if (valueSets == null) {
            if (minValues == null) {
                // there were too many rows to collect collect min/max range
                // dynamicPredicateConsumer was notified with 'all' in handleTooLargePredicate if there are no orderable types,
                // else it was notified with 'all' in handleMinMaxCollectionLimitExceeded
                return;
            }
            // valueSets became too large, create TupleDomain from min/max values
            for (Integer channelIndex : minMaxChannels) {
                Type type = channels.get(channelIndex).type;
                if (minValues[channelIndex] == null) {
                    // all values were null
                    domainsBuilder.put(channels.get(channelIndex).filterId, Domain.none(type));
                    continue;
                }
                Object min = readNativeValue(type, minValues[channelIndex], 0);
                Object max = readNativeValue(type, maxValues[channelIndex], 0);
                Domain domain = Domain.create(
                        ValueSet.ofRanges(range(type, min, true, max, true)),
                        false);
                domainsBuilder.put(channels.get(channelIndex).filterId, domain);
            }
            minValues = null;
            maxValues = null;
            dynamicPredicateConsumer.accept(TupleDomain.withColumnDomains(domainsBuilder.build()));
            return;
        }

        verify(blockBuilders != null, "blockBuilders is null when finish is called in DynamicFilterSourceOperator");
        for (int channelIndex = 0; channelIndex < channels.size(); ++channelIndex) {
            Block block = blockBuilders[channelIndex].build();
            Type type = channels.get(channelIndex).getType();
            domainsBuilder.put(channels.get(channelIndex).getFilterId(), convertToDomain(type, block));
        }
        valueSets = null;
        blockBuilders = null;
        dynamicPredicateConsumer.accept(TupleDomain.withColumnDomains(domainsBuilder.build()));
    }

    private Domain convertToDomain(Type type, Block block)
    {
        ImmutableList.Builder<Object> values = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); ++position) {
            Object value = readNativeValue(type, block, position);
            if (value != null) {
                // join doesn't match rows with NaN values.
                if (!isFloatingPointNaN(type, value)) {
                    values.add(value);
                }
            }
        }
        // Inner and right join doesn't match rows with null key column values.
        return Domain.create(ValueSet.copyOf(type, values.build()), false);
    }

    @Override
    public boolean isFinished()
    {
        return current == null && finished;
    }
}
