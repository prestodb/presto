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
package com.facebook.presto.raptorx.storage.organization;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.facebook.presto.raptorx.storage.organization.ChunkCompactor.isNullOrEmptyPage;
import static java.util.Objects.requireNonNull;

public class SortedRowSource
        implements Iterator<PageIndexInfo>, Comparable<SortedRowSource>, Closeable
{
    private final ConnectorPageSource pageSource;
    private final List<Type> columnTypes;
    private final List<Integer> sortIndexes;
    private final List<SortOrder> sortOrders;

    private Page currentPage;
    private int currentPosition;

    public SortedRowSource(ConnectorPageSource pageSource, List<Type> columnTypes, List<Integer> sortIndexes, List<SortOrder> sortOrders)
    {
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.sortIndexes = ImmutableList.copyOf(requireNonNull(sortIndexes, "sortIndexes is null"));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));

        currentPage = pageSource.getNextPage();
        currentPosition = 0;
    }

    @Override
    public boolean hasNext()
    {
        if (hasMorePositions(currentPage, currentPosition)) {
            return true;
        }

        Page page = getNextPage(pageSource);
        if (isNullOrEmptyPage(page)) {
            return false;
        }
        currentPage = page.getLoadedPage();
        currentPosition = 0;
        return true;
    }

    private static Page getNextPage(ConnectorPageSource pageSource)
    {
        Page page = null;
        while (isNullOrEmptyPage(page) && !pageSource.isFinished()) {
            page = pageSource.getNextPage();
            if (page != null) {
                page = page.getLoadedPage();
            }
        }
        return page;
    }

    @Override
    public PageIndexInfo next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        PageIndexInfo pageIndexInfo = new PageIndexInfo(currentPage, currentPosition);
        currentPosition++;
        return pageIndexInfo;
    }

    @Override
    public int compareTo(SortedRowSource other)
    {
        if (!hasNext()) {
            return 1;
        }

        if (!other.hasNext()) {
            return -1;
        }

        for (int i = 0; i < sortIndexes.size(); i++) {
            int channel = sortIndexes.get(i);
            Type type = columnTypes.get(channel);

            Block leftBlock = currentPage.getBlock(channel);
            int leftBlockPosition = currentPosition;

            Block rightBlock = other.currentPage.getBlock(channel);
            int rightBlockPosition = other.currentPosition;

            int compare = sortOrders.get(i).compareBlockValue(type, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    private static boolean hasMorePositions(Page currentPage, int currentPosition)
    {
        return currentPage != null && currentPosition < currentPage.getPositionCount();
    }

    void closeQuietly()
    {
        try {
            close();
        }
        catch (IOException ignored) {
        }
    }

    @Override
    public void close()
            throws IOException
    {
        pageSource.close();
    }
}
