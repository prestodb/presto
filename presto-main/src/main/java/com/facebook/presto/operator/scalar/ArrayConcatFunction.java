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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.operator.scalar.annotations.TypeParameter;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;

@ScalarFunction("concat")
@Description("Concatenates given arrays")
public final class ArrayConcatFunction
{
    private final PageBuilder pageBuilder;

    @TypeParameter("E")
    public ArrayConcatFunction(@TypeParameter("E") Type elementType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(elementType));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block concat(@TypeParameter("E") Type elementType, @SqlType("array(E)") Block leftBlock, @SqlType("array(E)") Block rightBlock)
    {
        if (leftBlock.getPositionCount() == 0) {
            return rightBlock;
        }
        if (rightBlock.getPositionCount() == 0) {
            return leftBlock;
        }

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        for (int i = 0; i < leftBlock.getPositionCount(); i++) {
            elementType.appendTo(leftBlock, i, blockBuilder);
        }
        for (int i = 0; i < rightBlock.getPositionCount(); i++) {
            elementType.appendTo(rightBlock, i, blockBuilder);
        }
        int total = leftBlock.getPositionCount() + rightBlock.getPositionCount();
        pageBuilder.declarePositions(total);
        return blockBuilder.getRegion(blockBuilder.getPositionCount() - total, total);
    }
}
