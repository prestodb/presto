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
package com.facebook.presto;

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.session.SessionLogger;
import com.google.common.base.Ticker;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SizeLimitedSessionLogger
        implements SessionLogger
{
    private final QueryId queryId;
    private final int maxSize;
    private final Ticker ticker = Ticker.systemTicker();
    private final Queue<Entry> entries = new ConcurrentLinkedQueue<>();

    public SizeLimitedSessionLogger(QueryId queryId, int maxSize)
    {
        this.queryId = queryId;
        this.maxSize = maxSize;
    }

    @Override
    public Queue<Entry> getEntries()
    {
        // Not taking a copy explicitly because this isn't racy
        // plus then we have to worry about getting the most up to date copy
        return entries;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("numEntries", entries.size())
                .add("maxSize", maxSize)
                .toString();
    }

    @Override
    public void log(Supplier<String> messageSupplier)
    {
        if (entries.size() < maxSize) {
            String message = messageSupplier.get();
            if (message != null) {
                entries.add(new Entry(message,
                        ticker.read(),
                        Thread.currentThread().getName()));
            }
        }
    }
}
