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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.Driver;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class StreamingIndexedData
        implements IndexedData
{
    private final List<Type> outputTypes;
    private final List<Type> indexKeyTypes;
    private final Page indexKeyTuple;
    private final PageBuffer pageBuffer;
    private final Driver driver;

    private boolean started;
    private Page currentPage;

    public StreamingIndexedData(List<Type> outputTypes, List<Type> indexKeyTypes, Page indexKeyTuple, PageBuffer pageBuffer, Driver driver)
    {
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        this.indexKeyTypes = ImmutableList.copyOf(requireNonNull(indexKeyTypes, "indexKeyTypes is null"));
        this.indexKeyTuple = requireNonNull(indexKeyTuple, "indexKeyTuple is null");
        checkArgument(indexKeyTuple.getPositionCount() == 1, "indexKeyTuple Page should only have one position");
        checkArgument(indexKeyTypes.size() == indexKeyTuple.getChannelCount(), "indexKeyTypes doesn't match indexKeyTuple columns");
        this.pageBuffer = requireNonNull(pageBuffer, "pageBuffer is null");
        this.driver = requireNonNull(driver, "driver is null");
    }

    @Override
    public long getJoinPosition(int position, Page page)
    {
        checkArgument(page.getChannelCount() == indexKeyTypes.size(), "Number of blocks does not match the number of key columns");
        if (started || !matchesExpectedKey(position, page)) {
            return IndexedData.UNLOADED_INDEX_KEY;
        }
        started = true;
        if (!loadNextPage()) {
            return IndexedData.NO_MORE_POSITIONS;
        }
        return 0;
    }

    // TODO: use the code generator here
    private boolean matchesExpectedKey(int position, Page page)
    {
        for (int i = 0; i < indexKeyTypes.size(); i++) {
            if (!indexKeyTypes.get(i).equalTo(page.getBlock(i), position, indexKeyTuple.getBlock(i), 0)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long getNextJoinPosition(long currentPosition)
    {
        checkState(currentPage != null, "getJoinPosition not called first");
        long nextPosition = currentPosition + 1;
        if (nextPosition >= currentPage.getPositionCount()) {
            if (!loadNextPage()) {
                return IndexedData.NO_MORE_POSITIONS;
            }
            nextPosition = 0;
        }
        return nextPosition;
    }

    private boolean loadNextPage()
    {
        Page nextPage = extractNonEmptyPage(pageBuffer);
        while (nextPage == null) {
            if (driver.isFinished()) {
                return false;
            }
            driver.process();
            nextPage = extractNonEmptyPage(pageBuffer);
        }
        currentPage = nextPage;
        return true;
    }

    /**
     * Return the next page from pageBuffer that has a non-zero position count, or null if none available
     */
    private static Page extractNonEmptyPage(PageBuffer pageBuffer)
    {
        Page page = pageBuffer.poll();
        while (page != null && page.getPositionCount() == 0) {
            page = pageBuffer.poll();
        }
        return page;
    }

    @Override
    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        // TODO: use the code generator here
        checkState(currentPage != null, "getJoinPosition not called first");
        int intPosition = toIntExact(position);
        for (int i = 0; i < outputTypes.size(); i++) {
            Type type = outputTypes.get(i);
            Block block = currentPage.getBlock(i);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i + outputChannelOffset);
            type.appendTo(block, intPosition, blockBuilder);
        }
    }

    @Override
    public void close()
    {
        driver.close();
        currentPage = null;
    }
}
