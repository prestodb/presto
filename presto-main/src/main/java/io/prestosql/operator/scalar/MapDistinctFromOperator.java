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
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.prestosql.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;

@ScalarOperator(IS_DISTINCT_FROM)
public final class MapDistinctFromOperator
{
    private MapDistinctFromOperator() {}

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @OperatorDependency(operator = IS_DISTINCT_FROM, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"}, convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL))
                    MethodHandle valueDistinctFromFunction,
            @TypeParameter("map(K, V)") Type mapType,
            @SqlType("map(K,V)") Block leftMapBlock,
            @IsNull boolean leftMapNull,
            @SqlType("map(K,V)") Block rightMapBlock,
            @IsNull boolean rightMapNull)
    {
        if (leftMapNull != rightMapNull) {
            return true;
        }
        if (leftMapNull) {
            return false;
        }
        // Note that we compare to NOT distinct here and so negate the result.
        return !MapGenericEquality.genericEqual(
                ((MapType) mapType).getKeyType(),
                leftMapBlock,
                rightMapBlock,
                (leftMapIndex, rightMapIndex) -> !(boolean) valueDistinctFromFunction.invokeExact(leftMapBlock, leftMapIndex, rightMapBlock, rightMapIndex));
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isDistinctFrom(
            @OperatorDependency(operator = IS_DISTINCT_FROM, returnType = StandardTypes.BOOLEAN, argumentTypes = {"V", "V"}, convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = FAIL_ON_NULL))
                    MethodHandle valueDistinctFromFunction,
            @TypeParameter("map(K, V)") Type mapType,
            @BlockPosition @SqlType(value = "map(K,V)", nativeContainerType = Block.class) Block left,
            @BlockIndex int leftPosition,
            @BlockPosition @SqlType(value = "map(K,V)", nativeContainerType = Block.class) Block right,
            @BlockIndex int rightPosition)
    {
        return isDistinctFrom(
                valueDistinctFromFunction,
                mapType,
                (Block) mapType.getObject(left, leftPosition),
                left.isNull(leftPosition),
                (Block) mapType.getObject(right, rightPosition),
                right.isNull(rightPosition));
    }
}
