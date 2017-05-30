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
package com.facebook.presto.raptorx.util;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;

import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public class IteratorPageSource
        implements ConnectorPageSource
{
    private final Iterator<Page> iterator;

    private long completedBytes;
    private boolean closed;

    public IteratorPageSource(Iterator<Page> iterator)
    {
        this.iterator = requireNonNull(iterator, "iterator is null");
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
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return closed || !iterator.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }
        Page page = iterator.next();
        if (page != null) {
            completedBytes += page.getSizeInBytes();
        }
        return page;
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
