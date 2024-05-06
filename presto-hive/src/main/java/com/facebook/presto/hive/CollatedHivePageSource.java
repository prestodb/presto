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
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CollatedHivePageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(CollatedHivePageSource.class);
    private final List<ConnectorPageSource> pageSources = new ArrayList<>();
    private int pageSourceIndex;

    public CollatedHivePageSource(HivePageSourceProvider hivePageSourceProvider,
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            CollatedHiveSplit collatedHiveSplit,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        List<HiveSplit> hiveSplits = collatedHiveSplit.getHiveSplits();
        for (HiveSplit split : hiveSplits) {
            pageSources.add(hivePageSourceProvider.createPageSource(transaction, session, split, layout, columns, splitContext));
        }

        log.info("NIKHIL CollatedHivePageSource one split reading from : %d files", pageSources.size());
    }

    @Override
    public long getCompletedBytes()
    {
        long completedBytes = 0;
        for (ConnectorPageSource pageSource : pageSources) {
            completedBytes += pageSource.getCompletedBytes();
        }
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        long completedPositions = 0;
        for (ConnectorPageSource pageSource : pageSources) {
            completedPositions += pageSource.getCompletedPositions();
        }
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        long readTimeNanos = 0;
        for (ConnectorPageSource pageSource : pageSources) {
            readTimeNanos += pageSource.getReadTimeNanos();
        }
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        boolean isFinished = true;
        for (ConnectorPageSource pageSource : pageSources) {
            isFinished = isFinished && pageSource.isFinished();
        }
        return isFinished;
    }

    @Override
    public Page getNextPage()
    {
        ConnectorPageSource pageSource = pageSources.get(pageSourceIndex);
        Page nextPage = pageSource.getNextPage();

        if (nextPage != null) {
            return nextPage;
        }

        if (pageSourceIndex < (pageSources.size() - 1)) {
            pageSourceIndex++;
            return getNextPage();
        }
        return null;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        long systemMemoryUsage = 0;
        for (ConnectorPageSource pageSource : pageSources) {
            systemMemoryUsage += pageSource.getSystemMemoryUsage();
        }
        return systemMemoryUsage;
    }

    @Override
    public void close()
            throws IOException
    {
        for (ConnectorPageSource pageSource : pageSources) {
            pageSource.close();
        }
    }
}
