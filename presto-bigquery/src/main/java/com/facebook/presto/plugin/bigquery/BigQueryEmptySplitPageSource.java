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
package com.facebook.presto.plugin.bigquery;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.spi.ConnectorPageSource;
import com.google.common.collect.ImmutableList;

import java.io.IOException;

public class BigQueryEmptySplitPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(BigQueryEmptySplitPageSource.class);
    private final long numberOfRows;
    private boolean finished;

    //TODO: will merge BigQueryEmptySplitPageSource into BigQueryResultPageSource to simplify the logic
    public BigQueryEmptySplitPageSource(long numberOfRows)
    {
        this.numberOfRows = numberOfRows;
        this.finished = false;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedPositions()
    {
        return finished ? numberOfRows : 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        log.debug("[%s] creating %d empty rows", Thread.currentThread(), numberOfRows);
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of());
        for (long i = 0; i < numberOfRows; i++) {
            pageBuilder.declarePosition();
        }
        finished = true;
        return pageBuilder.build();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        // nothing to do
    }
}
