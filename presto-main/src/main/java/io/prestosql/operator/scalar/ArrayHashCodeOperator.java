package io.prestosql.operator.scalar;
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

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.function.TypeParameterSpecialization;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.type.TypeUtils.NULL_HASH_CODE;
import static io.prestosql.util.Failures.internalError;

@ScalarOperator(HASH_CODE)
public final class ArrayHashCodeOperator
{
    private ArrayHashCodeOperator() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long hash(
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block)
    {
        try {
            long hash = 0;
            for (int i = 0; i < block.getPositionCount(); i++) {
                hash = CombineHashFunction.getHash(hash, block.isNull(i) ? NULL_HASH_CODE : (long) hashFunction.invoke(readNativeValue(type, block, i)));
            }
            return hash;
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType(StandardTypes.BIGINT)
    public static long hashLong(
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block)
    {
        try {
            long hash = 0;
            for (int i = 0; i < block.getPositionCount(); i++) {
                hash = CombineHashFunction.getHash(hash, block.isNull(i) ? NULL_HASH_CODE : (long) hashFunction.invokeExact(type.getLong(block, i)));
            }
            return hash;
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType(StandardTypes.BIGINT)
    public static long hashBoolean(
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block)
    {
        try {
            long hash = 0;
            for (int i = 0; i < block.getPositionCount(); i++) {
                hash = CombineHashFunction.getHash(hash, block.isNull(i) ? NULL_HASH_CODE : (long) hashFunction.invokeExact(type.getBoolean(block, i)));
            }
            return hash;
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlType(StandardTypes.BIGINT)
    public static long hashSlice(
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block)
    {
        try {
            long hash = 0;
            for (int i = 0; i < block.getPositionCount(); i++) {
                hash = CombineHashFunction.getHash(hash, block.isNull(i) ? NULL_HASH_CODE : (long) hashFunction.invokeExact(type.getSlice(block, i)));
            }
            return hash;
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType(StandardTypes.BIGINT)
    public static long hashDouble(
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block)
    {
        try {
            long hash = 0;
            for (int i = 0; i < block.getPositionCount(); i++) {
                hash = CombineHashFunction.getHash(hash, block.isNull(i) ? NULL_HASH_CODE : (long) hashFunction.invokeExact(type.getDouble(block, i)));
            }
            return hash;
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }
}
