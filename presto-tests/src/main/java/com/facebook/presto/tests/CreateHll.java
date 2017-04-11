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
package com.facebook.presto.tests;

import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;

public final class CreateHll
{
    private CreateHll() {}

    @ScalarFunction
    @SqlType(StandardTypes.HYPER_LOG_LOG)
    public static Slice createHll(@SqlType(StandardTypes.BIGINT) long value)
    {
        HyperLogLog hll = HyperLogLog.newInstance(4096);
        hll.add(value);
        return hll.serialize();
    }
}
