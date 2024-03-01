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

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;

public class RowIdOperators
{
    private RowIdOperators() {}

    @ScalarOperator(IS_DISTINCT_FROM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @SqlType(StandardTypes.ROW_ID) Slice left,
            @IsNull boolean leftNull,
            @SqlType(StandardTypes.ROW_ID) Slice right,
            @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return !left.equals(right);
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.ROW_ID) Slice left, @SqlType(StandardTypes.ROW_ID) Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.ROW_ID) Slice left, @SqlType(StandardTypes.ROW_ID) Slice right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.ROW_ID) Slice value)
    {
        return XxHash64.hash(value);
    }
}
