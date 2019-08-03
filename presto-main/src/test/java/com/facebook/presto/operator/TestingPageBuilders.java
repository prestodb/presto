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
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class TestingPageBuilders
{
    private TestingPageBuilders()
    {}

    static Page buildPage(List<Type> types, int positionCount, boolean allowNulls)
    {
        return buildPage(types, positionCount, true, false, allowNulls, false, Optional.empty());
    }

    static Page buildDictionaryPage(List<Type> types, int positionCount, boolean allowNulls)
    {
        return buildPage(types, positionCount, true, false, allowNulls, false, Optional.of("D"));
    }

    static Page buildRlePage(List<Type> types, int positionCount, boolean allowNulls)
    {
        return buildPage(types, positionCount, true, false, allowNulls, false, Optional.of("R"));
    }

    static Page buildPage(List<Type> types, int positionCount, boolean addPreComputedHashBlockAtBegin, boolean addNullBlockAtEnd, boolean useBlockView, Optional<String> wrappings)
    {
        return buildPage(types, positionCount, addPreComputedHashBlockAtBegin, addNullBlockAtEnd, true, useBlockView, wrappings);
    }

    static Page buildPage(List<Type> types, int positionCount, boolean addPreComputedHashBlockAtBegin, boolean addNullBlockAtEnd, boolean allowNulls, boolean useBlockView, Optional<String> wrappings)
    {
        TestingBlockBuilders blockBuilders = new TestingBlockBuilders();

        int channelCount = types.size();
        int preComputedChannelCount = (addPreComputedHashBlockAtBegin ? 1 : 0);
        int nullChannelCount = (addNullBlockAtEnd ? 1 : 0);

        Block[] blocks = new Block[channelCount + preComputedChannelCount + nullChannelCount];

        if (addPreComputedHashBlockAtBegin) {
            blocks[0] = blockBuilders.buildBigintBlock(positionCount, false);
        }

        for (int i = 0; i < channelCount; i++) {
            blocks[i + preComputedChannelCount] = blockBuilders.buildBlockWithType(types.get(i), positionCount, allowNulls, useBlockView, wrappings);
        }

        if (addNullBlockAtEnd) {
            blocks[channelCount + preComputedChannelCount] = blockBuilders.buildNullBlock(BIGINT, positionCount);
        }

        return new Page(positionCount, blocks);
    }

    static Page mergePages(List<Type> types, List<Page> pages)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        int rowCount = 0;
        for (Page page : pages) {
            for (int i = 0; i < types.size(); i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                Block block = page.getBlock(i);
                for (int j = 0; j < page.getPositionCount(); j++) {
                    if (block.isNull(j)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        page.getBlock(i).writePositionTo(j, blockBuilder);
                    }
                }
            }
            rowCount += page.getPositionCount();
        }
        pageBuilder.declarePositions(rowCount);
        return pageBuilder.build();
    }

    static List<Type> buildTypes(List<Type> types, boolean addPreComputedHashBlockAtBegin, boolean addNullBlockAtEnd)
    {
        ArrayList<Type> updatedTypes = new ArrayList<>(types);

        if (addPreComputedHashBlockAtBegin) {
            updatedTypes.add(0, BIGINT);
        }

        if (addNullBlockAtEnd) {
            updatedTypes.add(BIGINT);
        }

        return updatedTypes;
    }
}
