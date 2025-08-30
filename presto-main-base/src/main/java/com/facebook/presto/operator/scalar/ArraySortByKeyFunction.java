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
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.sql.gen.lambda.UnaryFunctionInterface;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

@ScalarFunction("array_sort")
@Description("Sorts the given array using a lambda function to extract the sorting key")
public final class ArraySortByKeyFunction
{
    private ArraySortByKeyFunction() {}

    @TypeParameter("E")
    @TypeParameter("K")
    @TypeParameterSpecialization(name = "K", nativeContainerType = long.class)
    @SqlType("array(E)")
    public static Block sortLong(
            @OperatorDependency(operator = LESS_THAN, argumentTypes = {"K", "K"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type elementType,
            @TypeParameter("K") Type keyType,
            @SqlType("array(E)") Block array,
            @SqlType("function(E, K)") UnaryFunctionInterface function)
    {
        return sort(elementType, array, function);
    }

    @TypeParameter("E")
    @TypeParameter("K")
    @TypeParameterSpecialization(name = "K", nativeContainerType = double.class)
    @SqlType("array(E)")
    public static Block sortDouble(
            @OperatorDependency(operator = LESS_THAN, argumentTypes = {"K", "K"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type elementType,
            @TypeParameter("K") Type keyType,
            @SqlType("array(E)") Block array,
            @SqlType("function(E, K)") UnaryFunctionInterface function)
    {
        return sort(elementType, array, function);
    }

    @TypeParameter("E")
    @TypeParameter("K")
    @TypeParameterSpecialization(name = "K", nativeContainerType = boolean.class)
    @SqlType("array(E)")
    public static Block sortBoolean(
            @OperatorDependency(operator = LESS_THAN, argumentTypes = {"K", "K"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type elementType,
            @TypeParameter("K") Type keyType,
            @SqlType("array(E)") Block array,
            @SqlType("function(E, K)") UnaryFunctionInterface function)
    {
        return sort(elementType, array, function);
    }

    @TypeParameter("E")
    @TypeParameter("K")
    @TypeParameterSpecialization(name = "K", nativeContainerType = Slice.class)
    @SqlType("array(E)")
    public static Block sortSlice(
            @OperatorDependency(operator = LESS_THAN, argumentTypes = {"K", "K"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type elementType,
            @TypeParameter("K") Type keyType,
            @SqlType("array(E)") Block array,
            @SqlType("function(E, K)") UnaryFunctionInterface function)
    {
        return sort(elementType, array, function);
    }

    @TypeParameter("E")
    @TypeParameter("K")
    @TypeParameterSpecialization(name = "K", nativeContainerType = Block.class)
    @SqlType("array(E)")
    public static Block sortBlock(
            @OperatorDependency(operator = LESS_THAN, argumentTypes = {"K", "K"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type elementType,
            @TypeParameter("K") Type keyType,
            @SqlType("array(E)") Block array,
            @SqlType("function(E, K)") UnaryFunctionInterface function)
    {
        return sort(elementType, array, function);
    }

    private static Block sort(
            Type elementType,
            Block array,
            UnaryFunctionInterface function)
    {
        int arrayLength = array.getPositionCount();

        if (arrayLength < 2) {
            return array;
        }

        List<ElementWithPosition> elementsWithPositions = new ArrayList<>(arrayLength);
        for (int position = 0; position < arrayLength; position++) {
            if (array.isNull(position)) {
                elementsWithPositions.add(new ElementWithPosition(position, null, true));
                continue;
            }

            Object element = getElementAtPosition(elementType, array, position);
            elementsWithPositions.add(new ElementWithPosition(position, element, false));
        }

        elementsWithPositions.sort(new Comparator<ElementWithPosition>() {
            @Override
            public int compare(ElementWithPosition e1, ElementWithPosition e2)
            {
                if (e1.isNull) {
                    return e2.isNull ? 0 : 1;
                }
                else if (e2.isNull) {
                    return -1;
                }

                try {
                    Object key1 = function.apply(e1.element);
                    Object key2 = function.apply(e2.element);

                    if (key1 == null) {
                        return key2 == null ? 0 : 1;
                    }
                    else if (key2 == null) {
                        return -1;
                    }

                    return compareKeys(key1, key2);
                }
                catch (Throwable throwable) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key function failed with exception", throwable);
                }
            }
        });

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, arrayLength);
        for (ElementWithPosition elementWithPosition : elementsWithPositions) {
            if (elementWithPosition.isNull) {
                blockBuilder.appendNull();
            }
            else {
                elementType.appendTo(array, elementWithPosition.position, blockBuilder);
            }
        }

        return blockBuilder.build();
    }

    private static int compareKeys(Object key1, Object key2)
    {
        if (key1 instanceof Long && key2 instanceof Long) {
            return Long.compare((Long) key1, (Long) key2);
        }
        else if (key1 instanceof Number && key2 instanceof Number) {
            return Double.compare(((Number) key1).doubleValue(), ((Number) key2).doubleValue());
        }
        else if (key1 instanceof Boolean && key2 instanceof Boolean) {
            return Boolean.compare((Boolean) key1, (Boolean) key2);
        }
        else if (key1 instanceof Slice && key2 instanceof Slice) {
            return ((Slice) key1).compareTo((Slice) key2);
        }
        else if (key1 instanceof String && key2 instanceof String) {
            return ((String) key1).compareTo((String) key2);
        }
        else {
            return key1.toString().compareTo(key2.toString());
        }
    }

    private static Object getElementAtPosition(Type elementType, Block array, int position)
    {
        if (elementType.getJavaType() == long.class) {
            return elementType.getLong(array, position);
        }
        else if (elementType.getJavaType() == double.class) {
            return elementType.getDouble(array, position);
        }
        else if (elementType.getJavaType() == boolean.class) {
            return elementType.getBoolean(array, position);
        }
        else if (elementType.getJavaType() == Slice.class) {
            return elementType.getSlice(array, position);
        }
        else if (elementType.getJavaType() == Block.class) {
            return elementType.getObject(array, position);
        }
        else {
            return elementType.getObject(array, position);
        }
    }

    private static class ElementWithPosition
    {
        private final int position;
        private final Object element;
        private final boolean isNull;

        public ElementWithPosition(int position, Object element, boolean isNull)
        {
            this.position = position;
            this.element = element;
            this.isNull = isNull;
        }
    }
}
