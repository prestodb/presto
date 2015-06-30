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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MultiPageChunkCursor
{
    private final PageChunkCursor chunkCursor;
    private boolean pageExhausted;
    private boolean newPartition;

    public MultiPageChunkCursor(PagePositionEqualitor equalitor, Page page, boolean continuation)
    {
        requireNonNull(equalitor, "equalitor is null");
        requireNonNull(page, "page is null");
        checkArgument(page.getPositionCount() > 0);

        this.chunkCursor = new PageChunkCursor(equalitor, page);

        // Load up the first chunk (must have at least one)
        checkState(chunkCursor.advance());
        newPartition = !continuation;
    }

    public static MultiPageChunkCursor initial(Page page, PagePositionEqualitor equalitor)
    {
        return new MultiPageChunkCursor(equalitor, page, false);
    }

    public static MultiPageChunkCursor next(Page page, PagePositionEqualitor equalitor, Page previousPage)
    {
        checkArgument(page.getPositionCount() > 0);
        checkArgument(previousPage.getPositionCount() > 0);
        boolean continuation = equalitor.rowEqualsRow(0, page, previousPage.getPositionCount() - 1, previousPage);
        return new MultiPageChunkCursor(equalitor, page, continuation);
    }

    public boolean isPageExhausted()
    {
        return pageExhausted;
    }

    public boolean advance()
    {
        checkState(!pageExhausted);
        if (!chunkCursor.advance()) {
            pageExhausted = true;
            return false;
        }
        newPartition = true;
        return true;
    }

    public Page getChunk()
    {
        return chunkCursor.getChunk();
    }

    public Page getRawPage()
    {
        return chunkCursor.getPage();
    }

    public int getChunkStartInclusive()
    {
        return chunkCursor.getChunkStartInclusive();
    }

    public int getChunkEndExclusive()
    {
        return chunkCursor.getChunkEndExclusive();
    }

    public boolean isNewPartition()
    {
        return newPartition;
    }
}
