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
package com.facebook.presto.ducklake.reader.delete;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.ConnectorPageSource;

import java.io.IOException;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Wraps a data page source and drops the rows a {@link PositionDeleteFilter} reports as deleted,
 * reading positions off a row-position channel of the delegate. Mirrors Iceberg's use of {@code
 * RowPredicate#filterPage} in {@code IcebergPageSourceProvider}, minus the general predicate
 * machinery Iceberg needs for equality deletes (DuckLake has no equivalent). When the row-position
 * channel was appended to the delegate's columns only so this class could filter (the query itself
 * did not request {@code $row_position} or {@code $row_id}), it is stripped from every returned
 * page so the output still matches the columns the query asked for.
 */
public class DeleteFilterPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final PositionDeleteFilter filter;
    private final int positionChannel;
    private final boolean dropPositionChannel;

    public DeleteFilterPageSource(ConnectorPageSource delegate, PositionDeleteFilter filter, int positionChannel, boolean dropPositionChannel)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.filter = requireNonNull(filter, "filter is null");
        checkArgument(positionChannel >= 0, "positionChannel is negative: %s", positionChannel);
        this.positionChannel = positionChannel;
        this.dropPositionChannel = dropPositionChannel;
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        Page page = delegate.getNextPage();
        if (page == null) {
            return null;
        }
        Page filtered = filterDeletedPositions(page);
        return dropPositionChannel ? filtered.dropColumn(positionChannel) : filtered;
    }

    private Page filterDeletedPositions(Page page)
    {
        if (filter.isEmpty()) {
            return page;
        }
        int positionCount = page.getPositionCount();
        Block positionBlock = page.getBlock(positionChannel);
        int[] retained = new int[positionCount];
        int retainedCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (!filter.isDeleted(BIGINT.getLong(positionBlock, position))) {
                retained[retainedCount] = position;
                retainedCount++;
            }
        }
        if (retainedCount == positionCount) {
            return page;
        }
        return page.getPositions(retained, 0, retainedCount);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }
}
