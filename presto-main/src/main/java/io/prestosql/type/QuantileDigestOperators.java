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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.function.OperatorType.CAST;

public final class QuantileDigestOperators
{
    private QuantileDigestOperators() {}

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToBinaryDouble(@SqlType("qdigest(double)") Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToBinaryBigint(@SqlType("qdigest(bigint)") Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARBINARY)
    public static Slice castToBinaryReal(@SqlType("qdigest(real)") Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType("qdigest(double)")
    public static Slice castFromVarbinaryDouble(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType("qdigest(bigint)")
    public static Slice castFromVarbinaryBigint(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return slice;
    }

    @ScalarOperator(CAST)
    @SqlType("qdigest(real)")
    public static Slice castFromVarbinaryReal(@SqlType(StandardTypes.VARBINARY) Slice slice)
    {
        return slice;
    }
}
