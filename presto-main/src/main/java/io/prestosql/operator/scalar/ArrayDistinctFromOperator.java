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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.function.BlockIndex;
import io.prestosql.spi.function.BlockPosition;
import io.prestosql.spi.function.Convention;
import io.prestosql.spi.function.IsNull;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.util.Failures.internalError;

@ScalarOperator(IS_DISTINCT_FROM)
public final class ArrayDistinctFromOperator
{
    private ArrayDistinctFromOperator() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @OperatorDependency(
                    operator = IS_DISTINCT_FROM,
                    returnType = StandardTypes.BOOLEAN,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) MethodHandle function,
            @TypeParameter("array(E)") Type type,
            @SqlType("array(E)") Block left,
            @IsNull boolean leftNull,
            @SqlType("array(E)") Block right,
            @IsNull boolean rightNull)
    {
        if (leftNull != rightNull) {
            return true;
        }
        if (leftNull) {
            return false;
        }
        if (left.getPositionCount() != right.getPositionCount()) {
            return true;
        }
        for (int i = 0; i < left.getPositionCount(); i++) {
            try {
                if ((boolean) function.invokeExact(
                        left,
                        i,
                        right,
                        i)) {
                    return true;
                }
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        return false;
    }

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @OperatorDependency(
                    operator = IS_DISTINCT_FROM,
                    returnType = StandardTypes.BOOLEAN,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) MethodHandle elementIsDistinctFrom,
            @TypeParameter("array(E)") Type type,
            @BlockPosition @SqlType(value = "array(E)", nativeContainerType = Block.class) Block left,
            @BlockIndex int leftPosition,
            @BlockPosition @SqlType(value = "array(E)", nativeContainerType = Block.class) Block right,
            @BlockIndex int rightPosition)
    {
        return isDistinctFrom(
                elementIsDistinctFrom,
                type,
                (Block) type.getObject(left, leftPosition),
                left.isNull(leftPosition),
                (Block) type.getObject(right, rightPosition),
                right.isNull(rightPosition));
    }
}
