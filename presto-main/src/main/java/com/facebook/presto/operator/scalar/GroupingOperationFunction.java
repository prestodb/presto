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
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.SqlType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;

public final class GroupingOperationFunction
{
    private GroupingOperationFunction() {}

    @ScalarFunction
    @Description("Returns a bitmap as an integer, indicating which columns are present in the grouping")
    @SqlType(BIGINT)
    public static long grouping(
            @SqlType(BIGINT) long groupId,
            @SqlType("array(integer)") Block groupingOrdinalsBlock,
            @SqlType("array(array(integer))") Block groupingSetOrdinalsBlock)
    {
        List<Integer> groupingOrdinals = new ArrayList<>(groupingOrdinalsBlock.getPositionCount());
        for (int i = 0; i < groupingOrdinalsBlock.getPositionCount(); i++) {
            groupingOrdinals.add(groupingOrdinalsBlock.getInt(i, 0));
        }

        ArrayType arrayType = new ArrayType(INTEGER);
        List<List<Integer>> groupingSetOrdinals = new ArrayList<>(groupingSetOrdinalsBlock.getPositionCount());
        for (int i = 0; i < groupingSetOrdinalsBlock.getPositionCount(); i++) {
            Block subList = arrayType.getObject(groupingSetOrdinalsBlock, i);
            List<Integer> ordinals = new ArrayList<>(subList.getPositionCount());
            for (int j = 0; j < subList.getPositionCount(); j++) {
                ordinals.add(subList.getInt(j, 0));
            }
            groupingSetOrdinals.add(ordinals);
        }

        BitSet groupingBinary = new BitSet(groupingOrdinals.size());
        groupingBinary.set(0, groupingOrdinals.size());
        List<Integer> groupingSet = groupingSetOrdinals.get((int) groupId);
        for (Integer groupingColumn : groupingSet) {
            if (groupingOrdinals.contains(groupingColumn)) {
                groupingBinary.clear(groupingOrdinals.indexOf(groupingColumn));
            }
        }

        int grouping = 0;
        // Rightmost argument to grouping() is represented by the least significant bit
        // so we start the conversion from binary to decimal from the left.
        for (int i = groupingOrdinals.size() - 1, j = 0; i >= 0; i--, j++) {
            if (groupingBinary.get(i)) {
                grouping += Math.pow(2, j);
            }
        }

        return grouping;
    }
}
