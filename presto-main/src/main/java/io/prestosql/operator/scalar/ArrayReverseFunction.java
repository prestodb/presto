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
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.Type;

@ScalarFunction("reverse")
@Description("Returns an array which has the reversed order of the given array.")
public final class ArrayReverseFunction
{
    private final PageBuilder pageBuilder;

    @TypeParameter("E")
    public ArrayReverseFunction(@TypeParameter("E") Type elementType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(elementType));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block reverse(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block block)
    {
        int arrayLength = block.getPositionCount();

        if (arrayLength < 2) {
            return block;
        }

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        for (int i = arrayLength - 1; i >= 0; i--) {
            type.appendTo(block, i, blockBuilder);
        }
        pageBuilder.declarePositions(arrayLength);

        return blockBuilder.getRegion(blockBuilder.getPositionCount() - arrayLength, arrayLength);
    }
}
