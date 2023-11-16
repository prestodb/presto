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
package com.facebook.presto.operator.aggregation.sketch.theta;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import org.apache.datasketches.theta.Union;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class ThetaSketchStateFactory
        implements AccumulatorStateFactory<ThetaSketchAggregationState>
{
    @Override
    public SingleThetaSketchState createSingleState()
    {
        return new SingleThetaSketchState();
    }

    @Override
    public Class<? extends ThetaSketchAggregationState> getSingleStateClass()
    {
        return SingleThetaSketchState.class;
    }

    @Override
    public ThetaSketchAggregationState createGroupedState()
    {
        return new GroupedThetaSketchState();
    }

    @Override
    public Class<? extends ThetaSketchAggregationState> getGroupedStateClass()
    {
        return GroupedThetaSketchState.class;
    }

    public static final class SingleThetaSketchState
            implements ThetaSketchAggregationState, AccumulatorState
    {
        private Union sketch = Union.builder().buildUnion();

        @Override
        public Union getSketch()
        {
            return sketch;
        }

        @Override
        public void setSketch(Union sketch)
        {
            this.sketch = sketch;
        }

        @Override
        public long getEstimatedSize()
        {
            return sketch.getCurrentBytes();
        }
    }

    public static final class GroupedThetaSketchState
            extends AbstractGroupedAccumulatorState
            implements ThetaSketchAggregationState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedThetaSketchState.class).instanceSize();
        private final ObjectBigArray<Union> sketches = new ObjectBigArray<>();

        @Override
        public Union getSketch()
        {
            if (sketches.get(getGroupId()) == null) {
                setSketch(Union.builder().buildUnion());
            }
            return sketches.get(getGroupId());
        }

        @Override
        public void setSketch(Union sketch)
        {
            sketches.set(getGroupId(), requireNonNull(sketch, "sketch is null"));
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + sketches.sizeOf();
        }

        @Override
        public void ensureCapacity(long size)
        {
            sketches.ensureCapacity(size);
        }
    }
}
