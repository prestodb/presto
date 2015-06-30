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

public class PageChunkCursor
{
    private final PagePositionEqualitor equalitor;
    private final Page page;

    private int chunkStartInclusive;
    private int chunkEndExclusive;

    public PageChunkCursor(PagePositionEqualitor equalitor, Page page)
    {
        this.equalitor = equalitor;
        this.page = page;
    }

    public boolean advance()
    {
        if (chunkEndExclusive >= page.getPositionCount()) {
            return false;
        }
        chunkStartInclusive = chunkEndExclusive;
        chunkEndExclusive = Pages.findClusterEnd(page, chunkStartInclusive, equalitor);
        return true;
    }

    public Page getChunk()
    {
        return page.getRegion(chunkStartInclusive, chunkEndExclusive - chunkStartInclusive);
    }

    public Page getPage()
    {
        return page;
    }

    public int getChunkStartInclusive()
    {
        return chunkStartInclusive;
    }

    public int getChunkEndExclusive()
    {
        return chunkEndExclusive;
    }
}
