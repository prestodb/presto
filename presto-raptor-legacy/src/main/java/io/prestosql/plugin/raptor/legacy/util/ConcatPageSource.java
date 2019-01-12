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
package io.prestosql.plugin.raptor.legacy.util;

import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public class ConcatPageSource
        implements ConnectorPageSource
{
    private final Iterator<ConnectorPageSource> iterator;

    private ConnectorPageSource current;
    private long completedBytes;
    private long readTimeNanos;

    public ConcatPageSource(Iterator<ConnectorPageSource> iterator)
    {
        this.iterator = requireNonNull(iterator, "iterator is null");
    }

    @Override
    public long getCompletedBytes()
    {
        setup();
        return completedBytes + ((current != null) ? current.getCompletedBytes() : 0);
    }

    @Override
    public long getReadTimeNanos()
    {
        setup();
        return readTimeNanos + ((current != null) ? current.getReadTimeNanos() : 0);
    }

    @Override
    public boolean isFinished()
    {
        setup();
        return current == null;
    }

    @Override
    public Page getNextPage()
    {
        while (true) {
            setup();

            if (current == null) {
                return null;
            }
            if (!current.isFinished()) {
                return current.getNextPage();
            }

            completedBytes += current.getCompletedBytes();
            readTimeNanos += current.getReadTimeNanos();
            current = null;
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return (current != null) ? current.getSystemMemoryUsage() : 0;
    }

    @Override
    public void close()
            throws IOException
    {
        if (current != null) {
            current.close();
        }
    }

    private void setup()
    {
        if ((current == null) && iterator.hasNext()) {
            current = iterator.next();
        }
    }
}
