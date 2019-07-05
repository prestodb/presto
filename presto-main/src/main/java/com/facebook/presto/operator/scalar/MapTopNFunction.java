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

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;

@ScalarFunction("map_top_n")
@Description("construct an array of entries from a given map")
public class MapTopNFunction
{
    private final PageBuilder pageBuilder;

    @TypeParameter("K")
    @TypeParameter("V")
    public MapTopNFunction(@TypeParameter("map(K,V)") Type mapType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(mapType));
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("map(K,V)")
    @SqlNullable
    public Block mapTopN(
            @TypeParameter("map(K,V)") MapType rowType,
            @SqlType("map(K,V)") Block block,
            @SqlType(StandardTypes.INTEGER) long n
    )
    {
        checkCondition(n > 0 && n == (int) n, INVALID_FUNCTION_ARGUMENT, "N must be positive integer greater than zero");

        int entryCount = block.getPositionCount() / 2;

        // n >= map length then return early
        if (n >= entryCount) {
            return block;
        }

        Type keyType = rowType.getTypeParameters().get(0);
        Type valueType = rowType.getTypeParameters().get(1);

        // step 1 - convert map to list of map storing index
        ArrayList<AbstractMap.SimpleEntry<Integer, Integer>> list = new ArrayList<AbstractMap.SimpleEntry<Integer, Integer>>();
        for (int i = 0; i < entryCount; i++) {
            list.add(new AbstractMap.SimpleEntry(2 * i, 2 * i + 1));
        }

        // step 2 - sort on value in descending order
        Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>()
        {
            @Override
            public int compare(Map.Entry o1,
                    Map.Entry o2)
            {
                return -valueType.compareTo(block, (int) o1.getValue(), block, (int) o2.getValue());
            }
        });

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        // step 3 - build map back with top n
        BlockBuilder mapBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder resultBuilder = mapBlockBuilder.beginBlockEntry();
        for (int i = 0; i < entryCount; i++) {
            if (i == n) {
                break;
            }
            keyType.appendTo(block, list.get(i).getKey(), resultBuilder);
            valueType.appendTo(block, list.get(i).getValue(), resultBuilder);
        }
        mapBlockBuilder.closeEntry();
        pageBuilder.declarePosition();

        return rowType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }
}
