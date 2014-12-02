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
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class AggregateWindowFunction
        implements WindowFunction
{
    private final InternalAggregationFunction function;
    private final List<Integer> argumentChannels;
    private final AccumulatorFactory accumulatorFactory;

    private WindowIndex windowIndex;

    private AggregateWindowFunction(InternalAggregationFunction function, List<Integer> argumentChannels)
    {
        this.function = checkNotNull(function, "function is null");
        this.argumentChannels = ImmutableList.copyOf(argumentChannels);
        this.accumulatorFactory = function.bind(createArgs(function), Optional.<Integer>absent(), Optional.<Integer>absent(), 1.0);
    }

    @Override
    public Type getType()
    {
        return function.getFinalType();
    }

    @Override
    public void reset(WindowIndex windowIndex)
    {
        this.windowIndex = windowIndex;
    }

    @Override
    public void processRow(BlockBuilder output, int peerGroupStart, int peerGroupEnd, int frameStart, int frameEnd)
    {
        PageBuilder pageBuilder = new PageBuilder(function.getParameterTypes());
        for (int position = frameStart; (position >= 0) && (position <= frameEnd); position++) {
            for (int i = 0; i < function.getParameterTypes().size(); i++) {
                windowIndex.appendTo(argumentChannels.get(i), position, pageBuilder.getBlockBuilder(i));
            }
            pageBuilder.declarePosition();
        }

        Accumulator accumulator = accumulatorFactory.createAccumulator();
        accumulator.addInput(pageBuilder.build());
        accumulator.evaluateFinal(output);
    }

    public static WindowFunctionSupplier supplier(Signature signature, final InternalAggregationFunction function)
    {
        checkNotNull(function, "function is null");
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
