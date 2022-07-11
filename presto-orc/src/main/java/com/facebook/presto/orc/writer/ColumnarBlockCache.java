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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.ColumnarRow;

import java.util.IdentityHashMap;
import java.util.Map;

import static com.facebook.presto.common.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static java.util.Objects.requireNonNull;

/**
 * Write-through cache used by flat map column writers to avoid converting the
 * same block to columnar format multiple times.
 */
public class ColumnarBlockCache
{
    private final Map<Block, ColumnarMap> maps = new IdentityHashMap<>();
    private final Map<Block, ColumnarRow> rows = new IdentityHashMap<>();
    private final Map<Block, ColumnarArray> arrays = new IdentityHashMap<>();

    public void clear()
    {
        maps.clear();
        rows.clear();
        arrays.clear();
    }

    public ColumnarMap getColumnarMap(Block block)
    {
        requireNonNull(block, "block is null");
        ColumnarMap columnarMap = maps.get(block);
        if (columnarMap == null) {
            columnarMap = toColumnarMap(block);
            maps.put(block, columnarMap);
        }
        return columnarMap;
    }

    public ColumnarRow getColumnarRow(Block block)
    {
        requireNonNull(block, "block is null");
        ColumnarRow columnarRow = rows.get(block);
        if (columnarRow == null) {
            columnarRow = toColumnarRow(block);
            rows.put(block, columnarRow);
        }
        return columnarRow;
    }

    public ColumnarArray getColumnarArray(Block block)
    {
        requireNonNull(block, "block is null");
        ColumnarArray columnarArray = arrays.get(block);
        if (columnarArray == null) {
            columnarArray = toColumnarArray(block);
            arrays.put(block, columnarArray);
        }
        return columnarArray;
    }
}
