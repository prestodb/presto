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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.WindowFunction;
import com.facebook.presto.spi.function.WindowIndex;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class AggregateWindowFunction
        implements WindowFunction
{
    private final InternalAggregationFunction function;
    private final List<Integer> argumentChannels;
    private final AccumulatorFactory accumulatorFactory;

    private WindowIndex windowIndex;
    private Accumulator accumulator;
    private int currentStart;
    private int currentEnd;

    private AggregateWindowFunction(InternalAggregationFunction function, List<Integer> argumentChannels)
    {
        this.function = requireNonNull(function, "function is null");
        this.argumentChannels = ImmutableList.copyOf(argumentChannels);
        this.accumulatorFactory = function.bind(createArgs(function), Optional.empty());
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
        if (function.getParameterTypes().size() == 0) {
            accumulator.addInput(end - start + 1);
        }
        else {
            accumulator.addInput(windowIndex, argumentChannels, start, end);
        }
    }

    private void resetAccumulator()
    {
        if (currentStart >= 0) {
            accumulator = accumulatorFactory.createAccumulator();
            currentStart = -1;
            currentEnd = -1;
        }
    }

    public static WindowFunctionSupplier supplier(Signature signature, final InternalAggregationFunction function)
    {
        requireNonNull(function, "function is null");
        return new AbstractWindowFunctionSupplier(signature, null)
        {
            @Override
            protected WindowFunction newWindowFunction(List<Integer> inputs)
            {
                return new AggregateWindowFunction(function, inputs);
            }
        };
    }

    private static List<Integer> createArgs(InternalAggregationFunction function)
    {
        ImmutableList.Builder<Integer> list = ImmutableList.builder();
        for (int i = 0; i < function.getParameterTypes().size(); i++) {
            list.add(i);
        }
        return list.build();
    }
}
