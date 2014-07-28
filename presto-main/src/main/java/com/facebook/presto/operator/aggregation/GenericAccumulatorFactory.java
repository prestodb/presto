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

import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class GenericAccumulatorFactory
        implements AccumulatorFactory
{
    private final boolean approximationSupported;
    private final AccumulatorStateSerializer<?> stateSerializer;
    private final AccumulatorStateFactory<?> stateFactory;
    private final Constructor<? extends Accumulator> accumulatorConstructor;
    private final Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor;

    public GenericAccumulatorFactory(
            AccumulatorStateSerializer<?> stateSerializer,
            AccumulatorStateFactory<?> stateFactory,
            Class<? extends Accumulator> accumulatorClass,
            Class<? extends GroupedAccumulator> groupedAccumulatorClass,
            boolean approximationSupported)
    {
        this.stateSerializer = checkNotNull(stateSerializer, "stateSerializer is null");
        this.stateFactory = checkNotNull(stateFactory, "stateFactory is null");
        this.approximationSupported = approximationSupported;

        try {
            accumulatorConstructor = accumulatorClass.getConstructor(
                    AccumulatorStateSerializer.class,
                    AccumulatorStateFactory.class,
                    List.class,
                    Optional.class,
                    Optional.class,
                    double.class);

            groupedAccumulatorConstructor = groupedAccumulatorClass.getConstructor(
                    AccumulatorStateSerializer.class,
                    AccumulatorStateFactory.class,
                    List.class,
                    Optional.class,
                    Optional.class,
                    double.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Accumulator createAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
    {
        if (!approximationSupported) {
            checkArgument(confidence == 1.0, "Approximate queries not supported");
            checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        }
        try {
            return accumulatorConstructor.newInstance(stateSerializer, stateFactory, Ints.asList(argumentChannels), maskChannel, sampleWeightChannel, confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Accumulator createIntermediateAggregation(double confidence)
    {
        try {
            return accumulatorConstructor.newInstance(stateSerializer, stateFactory, ImmutableList.of(), Optional.<Integer>absent(), Optional.<Integer>absent(), confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public GroupedAccumulator createGroupedAggregation(Optional<Integer> maskChannel, Optional<Integer> sampleWeightChannel, double confidence, int... argumentChannels)
    {
        if (!approximationSupported) {
            checkArgument(confidence == 1.0, "Approximate queries not supported");
            checkArgument(!sampleWeightChannel.isPresent(), "Sampled data not supported");
        }
        try {
            return groupedAccumulatorConstructor.newInstance(stateSerializer, stateFactory, Ints.asList(argumentChannels), maskChannel, sampleWeightChannel, confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation(double confidence)
    {
        try {
            return groupedAccumulatorConstructor.newInstance(stateSerializer, stateFactory, ImmutableList.of(), Optional.<Integer>absent(), Optional.<Integer>absent(), confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }
}
