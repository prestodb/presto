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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchAggregationUtils;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarFunctionConstantStats;
import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.type.SfmSketchType;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.function.PropagateSourceStats.SOURCE_STATS;

public final class SfmSketchFunctions
{
    private SfmSketchFunctions() {}

    @ScalarFunction
    @Description("estimated cardinality of an SfmSketch object")
    @SqlType(StandardTypes.BIGINT)
    @ScalarFunctionConstantStats(avgRowSize = 8.0, minValue = 0)
    public static long cardinality(@ScalarPropagateSourceStats(nullFraction = SOURCE_STATS) @SqlType(SfmSketchType.NAME) Slice serializedSketch)
    {
        return SfmSketch.deserialize(serializedSketch).cardinality();
    }

    @ScalarFunction(value = "merge_sfm", deterministic = false)
    @Description("merge the contents of an array of SfmSketch objects")
    @SqlType(SfmSketchType.NAME)
    @SqlNullable
    public static Slice scalarMerge(@SqlType("array(SfmSketch)") Block block)
    {
        if (block.getPositionCount() == 0) {
            return null;
        }

        SfmSketch merged = null;
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                continue;
            }
            SfmSketch sketch = SfmSketch.deserialize(block.getSlice(i, 0, block.getSliceLength(i)));
            if (merged == null) {
                merged = sketch;
            }
            else {
                merged.mergeWith(sketch);
            }
        }

        if (merged == null) {
            return null;
        }

        return merged.serialize();
    }

    @ScalarFunction(value = "noisy_empty_approx_set_sfm", deterministic = false)
    @Description("an SfmSketch object representing an empty set")
    @SqlType(SfmSketchType.NAME)
    public static Slice emptyApproxSet(@SqlType(StandardTypes.DOUBLE) double epsilon,
            @SqlType(StandardTypes.BIGINT) long numberOfBuckets,
            @SqlType(StandardTypes.BIGINT) long precision)
    {
        SfmSketchAggregationUtils.validateSketchParameters(epsilon, (int) numberOfBuckets, (int) precision);
        SfmSketch sketch = SfmSketch.create((int) numberOfBuckets, (int) precision);
        sketch.enablePrivacy(epsilon);
        return sketch.serialize();
    }

    @ScalarFunction(value = "noisy_empty_approx_set_sfm", deterministic = false)
    @Description("an SfmSketch object representing an empty set")
    @SqlType(SfmSketchType.NAME)
    public static Slice emptyApproxSet(@SqlType(StandardTypes.DOUBLE) double epsilon,
            @SqlType(StandardTypes.BIGINT) long numberOfBuckets)
    {
        return emptyApproxSet(epsilon, numberOfBuckets, SfmSketchAggregationUtils.DEFAULT_PRECISION);
    }

    @ScalarFunction(value = "noisy_empty_approx_set_sfm", deterministic = false)
    @Description("an SfmSketch object representing an empty set")
    @SqlType(SfmSketchType.NAME)
    public static Slice emptyApproxSet(@SqlType(StandardTypes.DOUBLE) double epsilon)
    {
        return emptyApproxSet(epsilon, SfmSketchAggregationUtils.DEFAULT_BUCKET_COUNT, SfmSketchAggregationUtils.DEFAULT_PRECISION);
    }
}
