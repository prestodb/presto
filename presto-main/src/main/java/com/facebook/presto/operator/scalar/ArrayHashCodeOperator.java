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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.type.ArrayType.ARRAY_NULL_ELEMENT_MSG;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;

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
        int hash = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            checkElementNotNull(block.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            try {
                hash = (int) CombineHashFunction.getHash(hash, (long) hashFunction.invoke(readNativeValue(type, block, i)));
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, t);
            }
        }
        return hash;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType(StandardTypes.BIGINT)
    public static long hashLong(
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block)
    {
        int hash = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            checkElementNotNull(block.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            try {
                hash = (int) CombineHashFunction.getHash(hash, (long) hashFunction.invokeExact(type.getLong(block, i)));
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, t);
            }
        }
        return hash;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType(StandardTypes.BIGINT)
    public static long hashBoolean(
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block)
    {
        int hash = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            checkElementNotNull(block.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            try {
                hash = (int) CombineHashFunction.getHash(hash, (long) hashFunction.invokeExact(type.getBoolean(block, i)));
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, t);
            }
        }
        return hash;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlType(StandardTypes.BIGINT)
    public static long hashSlice(
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block)
    {
        int hash = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            checkElementNotNull(block.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            try {
                hash = (int) CombineHashFunction.getHash(hash, (long) hashFunction.invokeExact(type.getSlice(block, i)));
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, t);
            }
        }
        return hash;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType(StandardTypes.BIGINT)
    public static long hashDouble(
            @OperatorDependency(operator = HASH_CODE, returnType = StandardTypes.BIGINT, argumentTypes = {"T"}) MethodHandle hashFunction,
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block)
    {
        int hash = 0;
        for (int i = 0; i < block.getPositionCount(); i++) {
            checkElementNotNull(block.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            try {
                hash = (int) CombineHashFunction.getHash(hash, (long) hashFunction.invokeExact(type.getDouble(block, i)));
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, t);
            }
        }
        return hash;
    }
}
