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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.operator.RowPageBuilder.rowPageBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RowPagesBuilderWithHash
{
    public static RowPagesBuilderWithHash rowPagesBuilderWithHash(List<Integer> hashChannels, Type... types)
    {
        return new RowPagesBuilderWithHash(hashChannels, ImmutableList.copyOf(types));
    }

    public static RowPagesBuilderWithHash rowPagesBuilderWithHash(List<Integer> hashChannels, Iterable<Type> types)
    {
        return new RowPagesBuilderWithHash(hashChannels, types);
    }

    private final ImmutableList.Builder<Page> pages = ImmutableList.builder();
    private final List<Integer> hashChannels;
    private final List<Type> types;
    private final List<Type> hashTypes;
    private RowPageBuilder builder;

    RowPagesBuilderWithHash(List<Integer> hashChannels, Iterable<Type> types)
    {
        this.hashChannels = hashChannels;
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        List<Type> hashTypes = new ArrayList<Type>(hashChannels.size());
        for (int channel : hashChannels) {
            hashTypes.add(this.types.get(channel));
        }
        this.hashTypes = hashTypes;
        builder = rowPageBuilder(types);
    }

    public RowPagesBuilderWithHash addSequencePage(int length, int... initialValues)
    {
        checkArgument(length > 0, "length must be at least 1");
        checkNotNull(initialValues, "initialValues is null");
        checkArgument(initialValues.length == types.size(), "Expected %s initialValues, but got %s", types.size(), initialValues.length);

        pageBreak();
        Page page = SequencePageBuilder.createSequencePageWithHash(hashChannels, types, length, initialValues);
        pages.add(page);
        return this;
    }

    public RowPagesBuilderWithHash addBlocksPage(Block... blocks)
    {
        Block[] hashBlocks = getBlocksToHash(blocks);
        Block[] outputBlocks = Arrays.copyOf(blocks, blocks.length + 1);
        outputBlocks[blocks.length] = TypeUtils.getHashBlock(hashTypes, hashBlocks);
        pages.add(new Page(outputBlocks));
        return this;
    }

    public RowPagesBuilderWithHash row(Object... values)
    {
        builder.row(values);
        return this;
    }

    public RowPagesBuilderWithHash pageBreak()
    {
        if (!builder.isEmpty()) {
            pages.add(builder.buildWithHash(hashChannels));
            builder = rowPageBuilder(types);
        }
        return this;
    }

    public List<Page> build()
    {
        pageBreak();
        return pages.build();
    }

    private Block[] getBlocksToHash(Block[] blocks)
    {
        Block[] hashBlocks = new Block[hashChannels.size()];
        int hashIndex = 0;
        for (int channel : hashChannels) {
            hashBlocks[hashIndex++] = blocks[channel];
        }
        return hashBlocks;
    }
}
