package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.HASH_CODE;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.type.TypeUtils.NULL_HASH_CODE;
import static com.facebook.presto.util.Failures.internalError;

@ScalarOperator(HASH_CODE)
public final class ArrayHashCodeOperator
{
    private ArrayHashCodeOperator() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BIGINT)
    public static long hash(
            @OperatorDependency(operator = HASH_CODE, argumentTypes = {"T"}) MethodHandle hashFunction,
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
            @OperatorDependency(operator = HASH_CODE, argumentTypes = {"T"}) MethodHandle hashFunction,
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
            @OperatorDependency(operator = HASH_CODE, argumentTypes = {"T"}) MethodHandle hashFunction,
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
            @OperatorDependency(operator = HASH_CODE, argumentTypes = {"T"}) MethodHandle hashFunction,
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
            @OperatorDependency(operator = HASH_CODE, argumentTypes = {"T"}) MethodHandle hashFunction,
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
