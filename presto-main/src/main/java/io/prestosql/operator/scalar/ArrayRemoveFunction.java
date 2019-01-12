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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.util.Failures.internalError;

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
