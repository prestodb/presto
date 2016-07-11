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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class GenericAccumulatorFactory
        implements AccumulatorFactory
{
    private final AccumulatorStateSerializer<?> stateSerializer;
    private final AccumulatorStateFactory<?> stateFactory;
    private final Constructor<? extends Accumulator> accumulatorConstructor;
    private final Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor;
    private final Optional<Integer> maskChannel;
    private final Optional<Integer> sampleWeightChannel;
    private final double confidence;
    private final List<Integer> inputChannels;

    public GenericAccumulatorFactory(
            AccumulatorStateSerializer<?> stateSerializer,
            AccumulatorStateFactory<?> stateFactory,
            Constructor<? extends Accumulator> accumulatorConstructor,
            Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor,
            List<Integer> inputChannels,
            Optional<Integer> maskChannel,
            Optional<Integer> sampleWeightChannel,
            double confidence)
    {
        this.stateSerializer = requireNonNull(stateSerializer, "stateSerializer is null");
        this.stateFactory = requireNonNull(stateFactory, "stateFactory is null");
        this.accumulatorConstructor = requireNonNull(accumulatorConstructor, "accumulatorConstructor is null");
        this.groupedAccumulatorConstructor = requireNonNull(groupedAccumulatorConstructor, "groupedAccumulatorConstructor is null");
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        this.sampleWeightChannel = requireNonNull(sampleWeightChannel, "sampleWeightChannel is null");
        this.confidence = confidence;
        this.inputChannels = ImmutableList.copyOf(requireNonNull(inputChannels, "inputChannels is null"));
    }

    @Override
    public List<Integer> getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Accumulator createAccumulator()
    {
        try {
            return accumulatorConstructor.newInstance(stateSerializer, stateFactory, inputChannels, maskChannel, sampleWeightChannel, confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Accumulator createIntermediateAccumulator()
    {
        try {
            return accumulatorConstructor.newInstance(stateSerializer, stateFactory, ImmutableList.of(), Optional.empty(), Optional.empty(), confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public GroupedAccumulator createGroupedAccumulator()
    {
        try {
            return groupedAccumulatorConstructor.newInstance(stateSerializer, stateFactory, inputChannels, maskChannel, sampleWeightChannel, confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAccumulator()
    {
        try {
            return groupedAccumulatorConstructor.newInstance(stateSerializer, stateFactory, ImmutableList.of(), maskChannel, Optional.empty(), confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }
}
