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

import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

@AggregationFunctionMetadata("max")
public final class VarBinaryMaxAggregation
{
    public static final AggregationFunction VAR_BINARY_MAX = new AggregationCompiler().generateAggregationFunction(VarBinaryMaxAggregation.class);

    private VarBinaryMaxAggregation() {}

    @InputFunction
    @IntermediateInputFunction
    public static void max(SliceState state, @SqlType(VarcharType.class) Slice value)
    {
        state.setSlice(max(state.getSlice(), value));
    }

    private static Slice max(Slice a, Slice b)
    {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) > 0 ? a : b;
    }
}
