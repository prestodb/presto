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

import com.facebook.presto.operator.aggregation.ApproximateSetAggregation;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;

public final class HyperLogLogFunctions
{
    private HyperLogLogFunctions() {}

    @ScalarFunction
    @Description("compute the cardinality of a HyperLogLog instance")
    @SqlType(StandardTypes.BIGINT)
    public static long cardinality(@SqlType(StandardTypes.HYPER_LOG_LOG) Slice serializedHll)
    {
        return HyperLogLog.newInstance(serializedHll).cardinality();
    }

    @ScalarFunction
    @Description("an empty HyperLogLog instance")
    @SqlType(StandardTypes.HYPER_LOG_LOG)
    public static Slice empty_approx_set()
    {
        return ApproximateSetAggregation.newHyperLogLog().serialize();
    }
}
