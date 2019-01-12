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

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.util.Failures.internalError;

@ScalarFunction("array_remove")
@Description("Remove specified values from the given array")
public final class ArrayRemoveFunction
{
    private final PageBuilder pageBuilder;

    @TypeParameter("E")
    public ArrayRemoveFunction(@TypeParameter("E") Type elementType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(elementType));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block remove(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType("E") long value)
    {
        return remove(equalsFunction, type, array, (Object) value);
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block remove(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType("E") double value)
    {
        return remove(equalsFunction, type, array, (Object) value);
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block remove(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType("E") boolean value)
    {
        return remove(equalsFunction, type, array, (Object) value);
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block remove(
            @OperatorDependency(operator = EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle equalsFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType("E") Object value)
    {
        List<Integer> positions = new ArrayList<>();

        for (int i = 0; i < array.getPositionCount(); i++) {
            Object element = readNativeValue(type, array, i);

            try {
                if (element == null) {
                    positions.add(i);
                    continue;
                }
                Boolean result = (Boolean) equalsFunction.invoke(element, value);
                if (result == null) {
                    throw new PrestoException(NOT_SUPPORTED, "array_remove does not support arrays with elements that are null or contain null");
                }
                if (!result) {
                    positions.add(i);
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }

        if (array.getPositionCount() == positions.size()) {
            return array;
        }

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);

        for (int position : positions) {
            type.appendTo(array, position, blockBuilder);
        }

        pageBuilder.declarePositions(positions.size());
        return blockBuilder.getRegion(blockBuilder.getPositionCount() - positions.size(), positions.size());
    }
}
