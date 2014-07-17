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
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class GenericAccumulatorFactory
        implements AccumulatorFactory
{
    private final boolean approximationSupported;
    private final Type finalType;
    private final Type intermediateType;
    private final AccumulatorStateSerializer<?> stateSerializer;
    private final AccumulatorStateFactory<?> stateFactory;
    private final Constructor<? extends Accumulator> accumulatorConstructor;
    private final Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor;

    public GenericAccumulatorFactory(
            Type finalType,
            Type intermediateType,
            AccumulatorStateSerializer<?> stateSerializer,
            AccumulatorStateFactory<?> stateFactory,
            Class<? extends Accumulator> accumulatorClass,
            Class<? extends GroupedAccumulator> groupedAccumulatorClass,
            boolean approximationSupported)
    {
        this.finalType = checkNotNull(finalType, "finalType is null");
        this.intermediateType = checkNotNull(intermediateType, "intermediateType is null");
        this.stateSerializer = checkNotNull(stateSerializer, "stateSerializer is null");
        this.stateFactory = checkNotNull(stateFactory, "stateFactory is null");
        this.approximationSupported = approximationSupported;

        try {
            accumulatorConstructor = accumulatorClass.getConstructor(
                    Type.class,
                    Type.class,
                    AccumulatorStateSerializer.class,
                    AccumulatorStateFactory.class,
                    int.class,
                    Optional.class,
                    Optional.class,
                    double.class);

            groupedAccumulatorConstructor = groupedAccumulatorClass.getConstructor(
                    Type.class,
                    Type.class,
                    AccumulatorStateSerializer.class,
                    AccumulatorStateFactory.class,
                    int.class,
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
            return accumulatorConstructor.newInstance(finalType, intermediateType, stateSerializer, stateFactory, argumentChannels[0], maskChannel, sampleWeightChannel, confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Accumulator createIntermediateAggregation(double confidence)
    {
        try {
            return accumulatorConstructor.newInstance(finalType, intermediateType, stateSerializer, stateFactory, -1, Optional.<Integer>absent(), Optional.<Integer>absent(), confidence);
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
            return groupedAccumulatorConstructor.newInstance(finalType, intermediateType, stateSerializer, stateFactory, argumentChannels[0], maskChannel, sampleWeightChannel, confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAggregation(double confidence)
    {
        try {
            return groupedAccumulatorConstructor.newInstance(finalType, intermediateType, stateSerializer, stateFactory, -1, Optional.<Integer>absent(), Optional.<Integer>absent(), confidence);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw Throwables.propagate(e);
        }
    }
}
