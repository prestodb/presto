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
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.Convention;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.util.Failures.internalError;

@ScalarOperator(IS_DISTINCT_FROM)
public final class ArrayDistinctFromOperator
{
    private ArrayDistinctFromOperator() {}

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @OperatorDependency(
                    operator = IS_DISTINCT_FROM,
                    argumentTypes = {"E", "E"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL)) MethodHandle function,
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
                (Block) type.getObject(left, leftPosition),
                left.isNull(leftPosition),
                (Block) type.getObject(right, rightPosition),
                right.isNull(rightPosition));
    }
}
