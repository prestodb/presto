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
package com.facebook.presto.operator.window;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.operator.UpdateMemory;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.WindowFunction;
import com.facebook.presto.spi.function.WindowIndex;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.aggregation.GenericAccumulatorFactory.generateAccumulatorFactory;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AggregateWindowFunction
        implements WindowFunction
{
    private final List<Integer> argumentChannels;
    private final AccumulatorFactory accumulatorFactory;

    private WindowIndex windowIndex;
    private Accumulator accumulator;
    private int currentStart;
    private int currentEnd;

    private AggregateWindowFunction(AggregationFunctionImplementation function, List<Integer> argumentChannels)
    {
        checkState(function instanceof BuiltInAggregationFunctionImplementation);
        BuiltInAggregationFunctionImplementation builtinFunction = (BuiltInAggregationFunctionImplementation) function;
        this.argumentChannels = ImmutableList.copyOf(argumentChannels);
        this.accumulatorFactory = generateAccumulatorFactory(builtinFunction, createArgs(builtinFunction), Optional.empty());
    }

    @Override
    public void reset(WindowIndex windowIndex)
    {
        this.windowIndex = windowIndex;
        resetAccumulator();
    }

    @Override
    public void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd)
    {
        if (frameStart < 0) {
            // empty frame
            resetAccumulator();
        }
        else if ((frameStart == currentStart) && (frameEnd >= currentEnd)) {
            // same or expanding frame
            accumulate(currentEnd + 1, frameEnd);
            currentEnd = frameEnd;
        }
        else {
            // different frame
            resetAccumulator();
            accumulate(frameStart, frameEnd);
            currentStart = frameStart;
            currentEnd = frameEnd;
        }

        accumulator.evaluateFinal(output);
    }

    private void accumulate(int start, int end)
    {
        accumulator.addInput(windowIndex, argumentChannels, start, end);
    }

    private void resetAccumulator()
    {
        if (currentStart >= 0) {
            // updateMemory callback is used by distinct and ordering accumulators
            // since window functions do not support distinct and ordering accumulators
            // it is ok not to provide the memory reservation callback
            accumulator = accumulatorFactory.createAccumulator(UpdateMemory.NOOP);
            currentStart = -1;
            currentEnd = -1;
        }
    }

    public static WindowFunctionSupplier supplier(Signature signature, final AggregationFunctionImplementation function)
    {
        requireNonNull(function, "function is null");
        return new AbstractWindowFunctionSupplier(signature, null)
        {
            @Override
            protected WindowFunction newWindowFunction(List<Integer> inputs, boolean ignoreNulls)
            {
                return new AggregateWindowFunction(function, inputs);
            }
        };
    }

    private static List<Integer> createArgs(BuiltInAggregationFunctionImplementation function)
    {
        ImmutableList.Builder<Integer> list = ImmutableList.builder();
        for (int i = 0; i < function.getParameterTypes().size(); i++) {
            list.add(i);
        }
        return list.build();
    }
}
