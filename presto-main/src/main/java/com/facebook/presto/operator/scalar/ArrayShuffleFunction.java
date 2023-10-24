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
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.util.concurrent.ThreadLocalRandom;

@ScalarFunction(value = "shuffle", deterministic = false)
@Description("Generates a random permutation of the given array.")
public final class ArrayShuffleFunction
{
    private static final int INITIAL_LENGTH = 128;

    @TypeParameter("E")
    public ArrayShuffleFunction(@TypeParameter("E") Type elementType) {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block shuffle(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block block)
    {
        int length = block.getPositionCount();
        int[] positions = new int[length];
        for (int i = 0; i < length; i++) {
            positions[i] = i;
        }

        // Fisher-Yates shuffle
        // Randomly swap a pair of positions
        for (int i = length - 1; i > 0; i--) {
            int index = ThreadLocalRandom.current().nextInt(i + 1);
            int swap = positions[i];
            positions[i] = positions[index];
            positions[index] = swap;
        }
        BlockBuilder blockBuilder = type.createBlockBuilder(null, block.getPositionCount());

        for (int i = 0; i < length; i++) {
            type.appendTo(block, positions[i], blockBuilder);
        }

        return blockBuilder.build();
    }
}
