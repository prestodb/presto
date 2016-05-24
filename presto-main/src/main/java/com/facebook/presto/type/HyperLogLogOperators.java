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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.annotations.ScalarOperator;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;

import static com.facebook.presto.metadata.OperatorType.CAST;

public final class HyperLogLogOperators
{
    private HyperLogLogOperators()
    {
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToBinary(@SqlType(StandardTypes.HYPER_LOG_LOG) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.HYPER_LOG_LOG)
    public static Slice castFromVarbinary(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.P4_HYPER_LOG_LOG)
    public static Slice castToP4Hll(@SqlType(StandardTypes.HYPER_LOG_LOG) Slice slice)
    {
        HyperLogLog hll = HyperLogLog.newInstance(slice);
        hll.makeDense();
        return hll.serialize();
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.HYPER_LOG_LOG)
    public static Slice castFromP4Hll(@SqlType(StandardTypes.P4_HYPER_LOG_LOG) Slice slice)
    {
        return slice;
    }
}
