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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.Failures.internalError;

@ScalarFunction("array_remove")
@Description("Remove specified values from the given array")
public final class ArrayRemoveFunction
{
    @TypeParameter("E")
    public ArrayRemoveFunction(@TypeParameter("E") Type elementType) {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block remove(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType("E") long value)
    {
        return remove(equalsFunction, type, array, (Object) value);
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block remove(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType("E") double value)
    {
        return remove(equalsFunction, type, array, (Object) value);
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block remove(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType("E") boolean value)
    {
        return remove(equalsFunction, type, array, (Object) value);
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block remove(
            @OperatorDependency(operator = EQUAL, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType("E") Object value)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, array.getPositionCount());
        for (int i = 0; i < array.getPositionCount(); i++) {
            Object element = readNativeValue(type, array, i);
            if (element == null) {
                blockBuilder.appendNull();
                continue;
            }
            try {
                Boolean result = (Boolean) equalsFunction.invoke(element, value);

                if (result == null) {
                    throw new PrestoException(NOT_SUPPORTED, "array_remove does not support arrays with elements that are null or contain null");
                }

                if (!result) {
                    type.appendTo(array, i, blockBuilder);
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }

        return blockBuilder.build();
    }
}
