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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;

import java.nio.ByteOrder;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;

public class ThetaSketchFunctions
{
    private ThetaSketchFunctions()
    {
    }

    @ScalarFunction(value = "sketch_theta_estimate")
    @Description("Get the estimate of unique values from a theta sketch")
    @SqlType("double")
    public static double thetaSketchEstimate(@SqlType("varbinary") Slice input)
    {
        CompactSketch sketch = CompactSketch.wrap(Memory.wrap(input.toByteBuffer(), ByteOrder.nativeOrder()));
        return sketch.getEstimate();
    }

    private static final RowType SUMMARY_TYPE = RowType.from(ImmutableList.of(
                new RowType.Field(Optional.of("estimate"), DOUBLE),
                new RowType.Field(Optional.of("theta"), DOUBLE),
                new RowType.Field(Optional.of("upper_bound_std"), DOUBLE),
                new RowType.Field(Optional.of("lower_bound_std"), DOUBLE),
                new RowType.Field(Optional.of("retained_entries"), INTEGER)));

    @ScalarFunction(value = "sketch_theta_summary")
    @Description("parses a brief summary from a theta sketch")
    @SqlType("row(estimate double, theta double, upper_bound_std double, lower_bound_std double, retained_entries int)")
    public static Block thetaSketchSummary(@SqlType("varbinary") Slice input)
    {
        CompactSketch sketch = CompactSketch.wrap(Memory.wrap(input.toByteBuffer(), ByteOrder.nativeOrder()));
        BlockBuilder output = SUMMARY_TYPE.createBlockBuilder(null, 1);
        BlockBuilder row = output.beginBlockEntry();
        DOUBLE.writeDouble(row, sketch.getEstimate());
        DOUBLE.writeDouble(row, sketch.getTheta());
        DOUBLE.writeDouble(row, sketch.getUpperBound(1));
        DOUBLE.writeDouble(row, sketch.getLowerBound(1));
        INTEGER.writeLong(row, sketch.getRetainedEntries());
        output.closeEntry();
        return output.build().getBlock(0);
    }
}
