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

import com.facebook.presto.common.type.DistinctType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.CodegenScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.DistinctType.hasAncestorRelationship;
import static com.facebook.presto.common.type.StandardTypes.DISTINCT_TYPE;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.identity;

public class DistinctTypeCasts
{
    private DistinctTypeCasts() {}

    @CodegenScalarOperator(CAST)
    @TypeParameter("T2")
    @TypeParameter(value = "T1", boundedBy = DISTINCT_TYPE)
    @SqlType("T2")
    public static MethodHandle castFromDistinct(@SqlType("T1") Type fromType, @TypeParameter("T2") Type toType)
    {
        return cast(fromType, toType);
    }

    @CodegenScalarOperator(CAST)
    @TypeParameter("T1")
    @TypeParameter(value = "T2", boundedBy = DISTINCT_TYPE)
    @SqlType("T2")
    public static MethodHandle castToDistinct(@SqlType("T1") Type fromType, @TypeParameter("T2") Type toType)
    {
        return cast(fromType, toType);
    }

    private static void checkCanCast(Type fromType, Type toType)
    {
        if (fromType instanceof DistinctType && toType instanceof DistinctType) {
            DistinctType fromDistinctType = (DistinctType) fromType;
            DistinctType toDistinctType = (DistinctType) toType;

            if (!hasAncestorRelationship(fromDistinctType, toDistinctType)) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast %s to %s", fromDistinctType.getName(), toDistinctType.getName()));
            }
        }
        else if (fromType instanceof DistinctType) {
            DistinctType fromDistinctType = (DistinctType) fromType;
            if (!fromDistinctType.getBaseType().equals(toType)) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast %s to %s", fromDistinctType.getName(), toType));
            }
        }
        else if (toType instanceof DistinctType) {
            DistinctType toDistinctType = (DistinctType) toType;
            if (!toDistinctType.getBaseType().equals(fromType)) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast %s to %s", fromType, toDistinctType.getName()));
            }
        }
    }

    private static MethodHandle cast(Type fromType, Type toType)
    {
        checkCanCast(fromType, toType);
        return identity(fromType.getJavaType());
    }
}
