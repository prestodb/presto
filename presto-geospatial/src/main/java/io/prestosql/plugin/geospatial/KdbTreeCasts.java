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
package io.prestosql.plugin.geospatial;

import io.airlift.slice.Slice;
import io.prestosql.geospatial.KdbTreeUtils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;

import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.CAST;

public final class KdbTreeCasts
{
    private KdbTreeCasts() {}

    @LiteralParameters("x")
    @ScalarOperator(CAST)
    @SqlType(KdbTreeType.NAME)
    public static Object castVarcharToKdbTree(@SqlType("varchar(x)") Slice json)
    {
        try {
            return KdbTreeUtils.fromJson(json.toStringUtf8());
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Invalid JSON string for KDB tree", e);
        }
    }
}
