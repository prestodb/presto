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
package com.facebook.presto.operator.mergeJoin;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RightMatchingData
{
    private final List<Type> types;
    private final List<Integer> joinChannels;
    private final List<Integer> outputChannels;

    private List<Page> pages;
    private int offset;
    private Page currentMatchSingleValuePage;
    private boolean complete;

    public RightMatchingData(List<Type> types, List<Integer> joinChannels, List<Integer> outputChannels)
    {
        this.types = types;
        this.joinChannels = joinChannels;
        this.outputChannels = outputChannels;
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

    public boolean isPositionMatch(Page page, int position)
    {
        if (pages.isEmpty()) {
            return false;
        }
        for (int channel : joinChannels) {
            int compare = types.get(channel).compareTo(page.getBlock(channel), position, currentMatchSingleValuePage.getBlock(channel), 0);
            if (compare != 0) {
                return false;
            }
        }
        return true;
    }

    public boolean isComplete()
    {
        return complete;
    }

    public void complete()
    {
        complete = true;
    }

    public boolean hasNextPage()
    {
        return offset < pages.size();
    }

    public Page getNextPage()
    {
        return pages.get(offset++);
    }

    public void reset()
    {
        offset = 0;
    }

    public void clear()
    {
        complete = false;
        pages = new ArrayList<>();
    }
}
