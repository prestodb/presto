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
import com.facebook.presto.common.type.VarcharEnumType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;

import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR_ENUM;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;

public final class VarcharEnumOperators
{
    private VarcharEnumOperators() {}

    @ScalarOperator(EQUAL)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType("T") Slice left, @SqlType("T") Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(NOT_EQUAL)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType("T") Slice left, @SqlType("T") Slice right)
    {
        return !left.equals(right);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(BOOLEAN)
    public static boolean isDistinctFrom(
            @SqlType("T") Slice left,
            @IsNull boolean leftNull,
            @SqlType("T") Slice right,
            @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        return notEqual(left, right);
    }

    @ScalarOperator(HASH_CODE)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(BIGINT)
    public static long hashCode(@SqlType("T") Slice value)
    {
        return xxHash64(value);
    }

    @ScalarOperator(XX_HASH_64)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(BIGINT)
    public static long xxHash64(@SqlType("T") Slice value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(INDETERMINATE)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(BOOLEAN)
    public static boolean indeterminate(@SqlType("T") Slice value, @IsNull boolean isNull)
    {
        return isNull;
    }

    @ScalarOperator(LESS_THAN)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType("T") Slice left, @SqlType("T") Slice right)
    {
        return left.compareTo(right) < 0;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType("T") Slice left, @SqlType("T") Slice right)
    {
        return left.compareTo(right) <= 0;
    }

    @ScalarOperator(GREATER_THAN)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType("T") Slice left, @SqlType("T") Slice right)
    {
        return left.compareTo(right) > 0;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType("T") Slice left, @SqlType("T") Slice right)
    {
        return left.compareTo(right) >= 0;
    }

    @ScalarOperator(BETWEEN)
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType("T") Slice value, @SqlType("T") Slice min, @SqlType("T") Slice max)
    {
        return min.compareTo(value) <= 0 && value.compareTo(max) <= 0;
    }

    @Description("Get the key corresponding to an enum value")
    @ScalarFunction("enum_key")
    @TypeParameter(value = "T", boundedBy = VARCHAR_ENUM)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice enumKey(@TypeParameter("T") VarcharEnumType enumType, @SqlType("T") Slice value)
    {
        Optional<String> key = enumType.getEnumKeyForValue(value.toStringUtf8());
        if (!key.isPresent()) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("No value '%s' in enum type %s", value.toStringUtf8(), enumType.getTypeSignature().getBase()));
        }
        return utf8Slice(key.get());
    }
}
