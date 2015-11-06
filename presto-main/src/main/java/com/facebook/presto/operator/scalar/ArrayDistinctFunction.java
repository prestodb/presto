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

import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;

import java.lang.invoke.MethodHandle;

@ScalarFunction("array_distinct")
@Description("Remove duplicate values from the given array")
public final class ArrayDistinctFunction
{
    private ArrayDistinctFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block distinct(
            @OperatorDependency(operator = OperatorType.EQUAL, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle equals,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array)
    {
        if (array.getPositionCount() < 2) {
            return array;
        }

        if (array.getPositionCount() == 2) {
            if (type.equalTo(array, 0, array, 1)) {
                return array.getSingleValueBlock(0);
            }
            else {
                return array;
            }
        }

        TypedSet typedSet = new TypedSet(type, array.getPositionCount());
        BlockBuilder distinctElementBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), array.getPositionCount());
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (!typedSet.contains(array, i)) {
                typedSet.add(array, i);
                type.appendTo(array, i, distinctElementBlockBuilder);
            }
        }

        return distinctElementBlockBuilder.build();
    }
}
