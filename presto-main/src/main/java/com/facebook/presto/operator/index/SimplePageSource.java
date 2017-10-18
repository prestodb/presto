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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SimplePageSource
        implements ConnectorPageSource
{
    private final List<Page> pages;
    private int currentPage;
    private long completedByte;
    private boolean closed;
    private final long systemMemoryUsage;

    public SimplePageSource(List<Page> pages)
    {
        this.pages = requireNonNull(pages, "pages is null");
        long systemMemoryUsage = 0;
        for (Page page : pages) {
            systemMemoryUsage += page.getRetainedSizeInBytes();
        }
        this.currentPage = 0;
        this.systemMemoryUsage = systemMemoryUsage;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedByte;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return closed || currentPage >= pages.size();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }
        Page page = pages.get(currentPage);
        currentPage++;
        completedByte += page.getSizeInBytes();
        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryUsage;
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
    }
}
