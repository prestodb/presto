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
package com.facebook.presto.operator.aggregation.builder;

import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class MergingHashAggregationBuilder
    implements Closeable
{
    private final List<AccumulatorFactory> accumulatorFactories;
    private final AggregationNode.Step step;
    private final int expectedGroups;
    private final ImmutableList<Integer> groupByPartialChannels;
    private final Optional<Integer> hashChannel;
    private final OperatorContext operatorContext;
    private final Iterator<Page> sortedPages;
    private InMemoryHashAggregationBuilder hashAggregationBuilder;
    private final List<Type> groupByTypes;
    private final LocalMemoryContext systemMemoryContext;
    private final long memorySizeBeforeSpill;
    private final int overwriteIntermediateChannelOffset;

    public MergingHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Iterator<Page> sortedPages,
            LocalMemoryContext systemMemoryContext,
            long memorySizeBeforeSpill,
            int overwriteIntermediateChannelOffset)
    {
        ImmutableList.Builder<Integer> groupByPartialChannels = ImmutableList.builder();
        for (int i = 0; i < groupByTypes.size(); i++) {
            groupByPartialChannels.add(i);
        }

        this.accumulatorFactories = accumulatorFactories;
        this.step = AggregationNode.Step.partialInput(step);
        this.expectedGroups = expectedGroups;
        this.groupByPartialChannels = groupByPartialChannels.build();
        this.hashChannel = hashChannel.isPresent() ? Optional.of(groupByTypes.size()) : hashChannel;
        this.operatorContext = operatorContext;
        this.sortedPages = sortedPages;
        this.groupByTypes = groupByTypes;
        this.systemMemoryContext = systemMemoryContext;
        this.memorySizeBeforeSpill = memorySizeBeforeSpill;
        this.overwriteIntermediateChannelOffset = overwriteIntermediateChannelOffset;

        rebuildHashAggregationBuilder();
    }

    public Iterator<Page> buildResult()
    {
        return new Iterator<Page>() {
            private Iterator<Page> resultPages = Collections.emptyIterator();

            @Override
            public boolean hasNext()
            {
                return sortedPages.hasNext() || resultPages.hasNext();
            }

            @Override
            public Page next()
            {
                if (!resultPages.hasNext()) {
                    rebuildHashAggregationBuilder();
                    long memorySize = 0; // ensure that at least one merged page will be processed

                    // we can produce output  after every page, because sortedPages does not have
                    // hash values that span multiple pages (guaranteed by MergeHashSort)
                    while (sortedPages.hasNext() && !shouldProduceOutput(memorySize)) {
                        hashAggregationBuilder.processPage(sortedPages.next());
                        memorySize = hashAggregationBuilder.getSizeInMemory();
                        systemMemoryContext.setBytes(memorySize);
                    }
                    resultPages = hashAggregationBuilder.buildResult();
                }

                return resultPages.next();
            }
        };
    }

    @Override
    public void close()
    {
        hashAggregationBuilder.close();
    }

    private boolean shouldProduceOutput(long memorySize)
    {
        return (memorySizeBeforeSpill > 0 && memorySize > memorySizeBeforeSpill);
    }

    private void rebuildHashAggregationBuilder()
    {
        this.hashAggregationBuilder = new InMemoryHashAggregationBuilder(
                accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByPartialChannels,
                hashChannel,
                operatorContext,
                DataSize.succinctBytes(0),
                Optional.of(overwriteIntermediateChannelOffset));
    }
}
