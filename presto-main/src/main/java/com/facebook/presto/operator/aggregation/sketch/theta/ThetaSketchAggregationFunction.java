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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.datasketches.theta.Union;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.operator.aggregation.sketch.theta.ThetaSketchStateFactory.getEstimatedMemoryUsage;

@AggregationFunction(value = "sketch_theta", isCalledOnNullInput = true)
@Description("calculates a theta sketch of the selected input column")
public class ThetaSketchAggregationFunction
{
    public static final String NAME = "sketch_theta";
    private ThetaSketchAggregationFunction()
    {
    }

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @TypeParameter("T") Type type,
            @AggregationState ThetaSketchAggregationState state,
            @SqlType("T") @BlockPosition Block block,
            @BlockIndex int position)
    {
        if (block.isNull(position)) {
            return;
        }
        Union sketch = state.getSketch();
        state.addMemoryUsage(-getEstimatedMemoryUsage(sketch));
        if (type.getJavaType().equals(Long.class) || type.getJavaType() == long.class) {
            sketch.update(type.getLong(block, position));
        }
        else if (type.getJavaType().equals(Double.class) || type.getJavaType() == double.class) {
            sketch.update(type.getDouble(block, position));
        }
        else if (type.getJavaType().equals(String.class) || type.getJavaType().equals(Slice.class)) {
            sketch.update(type.getSlice(block, position).getBytes());
        }
        else {
            throw new RuntimeException("unsupported sketch column type: " + type + " (java type: " + type.getJavaType() + ")");
        }
        state.addMemoryUsage(getEstimatedMemoryUsage(sketch));
    }

    @CombineFunction
    public static void merge(
            @AggregationState ThetaSketchAggregationState state,
            @AggregationState ThetaSketchAggregationState otherState)
    {
        Union sketch = state.getSketch();
        state.addMemoryUsage(-getEstimatedMemoryUsage(sketch));
        sketch.union(otherState.getSketch().getResult());
        state.addMemoryUsage(getEstimatedMemoryUsage(sketch));
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(
            @AggregationState ThetaSketchAggregationState state,
            BlockBuilder out)
    {
        Slice output = Slices.wrappedBuffer(state.getSketch().getResult().toByteArray());
        VARBINARY.writeSlice(out, output);
    }
}
