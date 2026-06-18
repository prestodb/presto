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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.parquet.writer.levels.DefinitionLevelIterable;
import com.facebook.presto.parquet.writer.levels.RepetitionLevelIterable;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ColumnChunk
{
    private final Block block;
    private final List<DefinitionLevelIterable> definitionLevelIterables;
    private final List<RepetitionLevelIterable> repetitionLevelIterables;

    public ColumnChunk(Block block)
    {
        this(block, ImmutableList.of(), ImmutableList.of());
    }

    public ColumnChunk(Block block, List<DefinitionLevelIterable> definitionLevelIterables, List<RepetitionLevelIterable> repetitionLevelIterables)
    {
        this.block = requireNonNull(block, "block is null");
        this.definitionLevelIterables = ImmutableList.copyOf(definitionLevelIterables);
        this.repetitionLevelIterables = ImmutableList.copyOf(repetitionLevelIterables);
    }

    public List<DefinitionLevelIterable> getDefinitionLevelIterables()
    {
        return definitionLevelIterables;
    }

    public List<RepetitionLevelIterable> getRepetitionLevelIterables()
    {
        return repetitionLevelIterables;
    }

    public Block getBlock()
    {
        return block;
    }
}
