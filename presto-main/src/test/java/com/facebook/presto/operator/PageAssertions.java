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

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockAssertions.Encoding;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.Encoding.DICTIONARY;
import static com.facebook.presto.block.BlockAssertions.Encoding.RUN_LENGTH;
import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createAllNullsBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomBlockForType;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public final class PageAssertions
{
    private PageAssertions()
    {
    }

    public static void assertPageEquals(List<? extends Type> types, Page actualPage, Page expectedPage)
    {
        assertEquals(types.size(), actualPage.getChannelCount());
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        for (int i = 0; i < actualPage.getChannelCount(); i++) {
            assertBlockEquals(types.get(i), actualPage.getBlock(i), expectedPage.getBlock(i));
        }
    }

    public static Page createPageWithRandomData(List<Type> types, int positionCount, float primitiveNullRate, float nestedNullRate)
    {
        return createPageWithRandomData(types, positionCount, true, false, primitiveNullRate, nestedNullRate, false, ImmutableList.of());
    }

    public static Page createDictionaryPageWithRandomData(List<Type> types, int positionCount, float primitiveNullRate, float nestedNullRate)
    {
        return createPageWithRandomData(types, positionCount, true, false, primitiveNullRate, nestedNullRate, false, ImmutableList.of(DICTIONARY));
    }

    public static Page createRlePageWithRandomData(List<Type> types, int positionCount, float primitiveNullRate, float nestedNullRate)
    {
        return createPageWithRandomData(types, positionCount, true, false, primitiveNullRate, nestedNullRate, false, ImmutableList.of(RUN_LENGTH));
    }

    public static Page createPageWithRandomData(
            List<Type> types,
            int positionCount,
            boolean addPreComputedHashBlock,
            boolean addNullBlock,
            float primitiveNullRate,
            float nestedNullRate,
            boolean useBlockView,
            List<Encoding> wrappings)
    {
        int channelCount = types.size();
        int preComputedChannelCount = (addPreComputedHashBlock ? 1 : 0);
        int nullChannelCount = (addNullBlock ? 1 : 0);

        Block[] blocks = new Block[channelCount + preComputedChannelCount + nullChannelCount];

        if (addPreComputedHashBlock) {
            blocks[0] = BlockAssertions.createRandomLongsBlock(positionCount, 0.0f);
        }

        for (int i = 0; i < channelCount; i++) {
            blocks[i + preComputedChannelCount] = createRandomBlockForType(types.get(i), positionCount, primitiveNullRate, nestedNullRate, useBlockView, wrappings);
        }

        if (addNullBlock) {
            blocks[channelCount + preComputedChannelCount] = createAllNullsBlock(BIGINT, positionCount);
        }

        return new Page(positionCount, blocks);
    }

    public static Page mergePages(List<Type> types, List<Page> pages)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        int totalPositionCount = 0;
        for (Page page : pages) {
            verify(page.getChannelCount() == types.size(), format("Number of channels in page %d is not equal to number of types %d", page.getChannelCount(), types.size()));

            for (int i = 0; i < types.size(); i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                Block block = page.getBlock(i);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        block.writePositionTo(position, blockBuilder);
                    }
                }
            }
            totalPositionCount += page.getPositionCount();
        }
        pageBuilder.declarePositions(totalPositionCount);
        return pageBuilder.build();
    }

    /**
     * Create a new types list that prepends the BIGINT type in front if addPreComputedHashBlock is true, and append the BIGINT type at the end if addNullBlock is true.
     */
    public static List<Type> updateBlockTypesWithHashBlockAndNullBlock(List<Type> types, boolean addPreComputedHashBlock, boolean addNullBlock)
    {
        ImmutableList.Builder<Type> newTypes = ImmutableList.builder();

        if (addPreComputedHashBlock) {
            newTypes.add(BIGINT);
        }

        newTypes.addAll(types);

        if (addNullBlock) {
            newTypes.add(BIGINT);
        }

        return newTypes.build();
    }
}
