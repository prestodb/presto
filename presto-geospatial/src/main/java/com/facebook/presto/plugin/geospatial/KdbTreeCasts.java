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
package com.facebook.presto.plugin.geospatial;

import com.facebook.presto.common.type.KdbTreeType;
import com.facebook.presto.geospatial.KdbTreeUtils;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;

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
