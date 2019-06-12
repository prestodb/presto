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
package com.facebook.presto.parquet.writer;

import com.facebook.presto.parquet.writer.repdef.DefLevelIterable;
import com.facebook.presto.parquet.writer.repdef.RepLevelIterable;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ColumnChunk
{
    private final Block block;
    private final List<DefLevelIterable> defLevelIterables;
    private final List<RepLevelIterable> repLevelIterables;

    ColumnChunk(Block block)
    {
        this(block, ImmutableList.of(), ImmutableList.of());
    }

    ColumnChunk(Block block, List<DefLevelIterable> defLevelIterables, List<RepLevelIterable> repLevelIterables)
    {
        this.block = requireNonNull(block, "block is null");
        this.defLevelIterables = ImmutableList.copyOf(defLevelIterables);
        this.repLevelIterables = ImmutableList.copyOf(repLevelIterables);
    }

    List<DefLevelIterable> getDefLevelIterables()
    {
        return defLevelIterables;
    }

    List<RepLevelIterable> getRepLevelIterables()
    {
        return repLevelIterables;
    }

    public Block getBlock()
    {
        return block;
    }
}
