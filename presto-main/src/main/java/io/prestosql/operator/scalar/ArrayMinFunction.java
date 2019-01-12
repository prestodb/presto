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

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.prestosql.operator.scalar.ArrayMinMaxUtils.booleanArrayMinMax;
import static io.prestosql.operator.scalar.ArrayMinMaxUtils.doubleArrayMinMax;
import static io.prestosql.operator.scalar.ArrayMinMaxUtils.longArrayMinMax;
import static io.prestosql.operator.scalar.ArrayMinMaxUtils.sliceArrayMinMax;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.util.Failures.internalError;

@ScalarFunction("array_min")
@Description("Get minimum value of array")
public final class ArrayMinFunction
{
    private ArrayMinFunction() {}

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Long longArrayMin(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return longArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Boolean booleanArrayMin(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return booleanArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Double doubleArrayMin(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return doubleArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Slice sliceArrayMin(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        return sliceArrayMinMax(compareMethodHandle, elementType, block);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Block blockArrayMin(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"T", "T"}) MethodHandle compareMethodHandle,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block)
    {
        try {
            if (block.getPositionCount() == 0) {
                return null;
            }

            Block selectedValue = (Block) elementType.getObject(block, 0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return null;
                }
                Block value = (Block) elementType.getObject(block, i);
                if ((boolean) compareMethodHandle.invokeExact(value, selectedValue)) {
                    selectedValue = value;
                }
            }

            return selectedValue;
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }
}
