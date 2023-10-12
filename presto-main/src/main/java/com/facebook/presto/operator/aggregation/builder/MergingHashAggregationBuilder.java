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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.WorkProcessor;
import com.facebook.presto.operator.WorkProcessor.Transformation;
import com.facebook.presto.operator.WorkProcessor.TransformationState;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;

public class MergingHashAggregationBuilder
        implements Closeable
{
    private final List<AccumulatorFactory> accumulatorFactories;
    private final AggregationNode.Step step;
    private final int expectedGroups;
    private final ImmutableList<Integer> groupByPartialChannels;
    private final Optional<Integer> hashChannel;
    private final OperatorContext operatorContext;
    private final WorkProcessor<Page> sortedPages;
    private InMemoryHashAggregationBuilder hashAggregationBuilder;
    private final List<Type> groupByTypes;
    private final LocalMemoryContext systemMemoryContext;
    private final long memoryLimitForMerge;
    private final int overwriteIntermediateChannelOffset;
    private final JoinCompiler joinCompiler;

    public MergingHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            WorkProcessor<Page> sortedPages,
            LocalMemoryContext systemMemoryContext,
            long memoryLimitForMerge,
            int overwriteIntermediateChannelOffset,
            JoinCompiler joinCompiler)
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
        this.memoryLimitForMerge = memoryLimitForMerge;
        this.overwriteIntermediateChannelOffset = overwriteIntermediateChannelOffset;
        this.joinCompiler = joinCompiler;

        rebuildHashAggregationBuilder();
    }

    public WorkProcessor<Page> buildResult()
    {
        return sortedPages.flatTransform(new Transformation<Page, WorkProcessor<Page>>()
        {
            boolean reset = true;
            long memorySize;

            public TransformationState<WorkProcessor<Page>> process(Optional<Page> inputPageOptional)
            {
                if (reset) {
                    rebuildHashAggregationBuilder();
                    memorySize = 0;
                    reset = false;
                }

                boolean inputFinished = !inputPageOptional.isPresent();
                if (inputFinished && memorySize == 0) {
                    // no more pages and aggregation builder is empty
                    return TransformationState.finished();
                }

                if (!inputFinished) {
                    Page inputPage = inputPageOptional.get();
                    boolean done = hashAggregationBuilder.processPage(inputPage).process();
                    // TODO: this class does not yield wrt memory limit; enable it
                    verify(done);
                    memorySize = hashAggregationBuilder.getSizeInMemory();
                    systemMemoryContext.setBytes(memorySize);

                    if (!shouldProduceOutput(memorySize)) {
                        return TransformationState.needsMoreData();
                    }
                }

                reset = true;
                // we can produce output after every input page, because input pages do not have
                // hash values that span multiple pages (guaranteed by MergeHashSort)
                return TransformationState.ofResult(hashAggregationBuilder.buildResult(), !inputFinished);
            }
        });
    }

    @Override
    public void close()
    {
        hashAggregationBuilder.close();
    }

    private boolean shouldProduceOutput(long memorySize)
    {
        return (memoryLimitForMerge > 0 && memorySize > memoryLimitForMerge);
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
                Optional.of(DataSize.succinctBytes(0)),
                Optional.of(overwriteIntermediateChannelOffset),
                joinCompiler,
                false,
                false);
    }
}
