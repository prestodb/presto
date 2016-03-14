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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import com.google.common.primitives.Ints;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.metadata.OperatorType.LESS_THAN;

@ScalarFunction("array_sort")
@Description("Sorts the given array in ascending order according to the natural ordering of its elements.")
public final class ArraySortFunction
{
    private ArraySortFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block sort(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block block)
    {
        List<Integer> positions = Ints.asList(new int[block.getPositionCount()]);
        for (int i = 0; i < block.getPositionCount(); i++) {
            positions.set(i, i);
        }

        Collections.sort(positions, new Comparator<Integer>()
        {
            @Override
            public int compare(Integer p1, Integer p2)
            {
                //TODO: This could be quite slow, it should use parametric equals
                return type.compareTo(block, p1, block, p2);
            }
        });

        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());

        for (int position : positions) {
            type.appendTo(block, position, blockBuilder);
        }

        return blockBuilder.build();
    }
}
