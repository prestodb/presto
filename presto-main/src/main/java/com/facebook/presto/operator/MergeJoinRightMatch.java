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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MergeJoinRightMatch
{
    private final List<Type> types;
    private final List<Integer> joinChannels;
    private final List<Integer> outputChannels;
    private final Page matchSingleValuePage;
    private final List<Page> pages;
    private boolean complete;

    public MergeJoinRightMatch(List<Type> types, List<Integer> joinChannels, List<Integer> outputChannels, Page matchPage)
    {
        this.types = types;
        this.joinChannels = joinChannels;
        this.outputChannels = outputChannels;
        this.matchSingleValuePage = matchPage;
        this.pages = new ArrayList<>();
    }

    public void addPage(Page page, int startPosition, int endPosition)
    {
        if (endPosition < page.getPositionCount()) {
            complete = true;
        }

        Block[] blocks = new Block[outputChannels.size()];
        for (int i = 0; i < outputChannels.size(); i++) {
            Block block = page.getBlock(outputChannels.get(i));
            block = block.copyRegion(startPosition, endPosition - startPosition);
            blocks[i] = block;
        }
        pages.add(new Page(endPosition - startPosition, blocks));
    }

    public int findEndOfMatch(Page page)
    {
        if (complete) {
            return 0;
        }

        int numPositions = page.getPositionCount();
        int endPosition = 0;

        while (endPosition < numPositions && isPositionMatch(page, endPosition)) {
            ++endPosition;
        }

        if (endPosition > 0) {
            addPage(page, 0, endPosition);
        }

        complete = endPosition < numPositions;
        return endPosition;
    }

    public boolean isPositionMatch(Page page, int position)
    {
        for (int channel : joinChannels) {
            int compare = types.get(channel).compareTo(page.getBlock(channel), position, matchSingleValuePage.getBlock(channel), 0);
            if (compare != 0) {
                return false;
            }
        }
        return true;
    }

    public Iterator<Page> getPages()
    {
        return pages.listIterator();
    }

    public void complete()
    {
        complete = true;
    }
}
