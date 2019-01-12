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
package io.prestosql.spi.connector;

import io.prestosql.spi.Page;

import java.io.IOException;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public class FixedPageSource
        implements ConnectorPageSource
{
    private final Iterator<Page> pages;

    private long completedBytes;
    private long memoryUsageBytes;
    private boolean closed;

    public FixedPageSource(Iterable<Page> pages)
    {
        this.pages = requireNonNull(pages, "pages is null").iterator();

        long memoryUsageBytes = 0;
        for (Page page : pages) {
            memoryUsageBytes += page.getRetainedSizeInBytes();
        }
        this.memoryUsageBytes = memoryUsageBytes;
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return closed || !pages.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }
        Page page = pages.next();
        completedBytes += page.getSizeInBytes();
        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return memoryUsageBytes;
    }
}
