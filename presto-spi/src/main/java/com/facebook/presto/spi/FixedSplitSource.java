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
package com.facebook.presto.spi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class FixedSplitSource
        implements ConnectorSplitSource
{
    private final String dataSourceName;
    private final List<ConnectorSplit> splits;
    private int offset;

    public FixedSplitSource(String dataSourceName, Iterable<? extends ConnectorSplit> splits)
    {
        this.dataSourceName = dataSourceName;
        if (splits == null) {
            throw new NullPointerException("splits is null");
        }
        List<ConnectorSplit> splitsList = new ArrayList<>();
        for (ConnectorSplit split : splits) {
            splitsList.add(split);
        }
        this.splits = Collections.unmodifiableList(splitsList);
    }

    @Override
    public String getDataSourceName()
    {
        return dataSourceName;
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        int remainingSplits = splits.size() - offset;
        int size = Math.min(remainingSplits, maxSize);
        List<ConnectorSplit> results = splits.subList(offset, offset + size);
        offset += size;

        return completedFuture(results);
    }

    @Override
    public boolean isFinished()
    {
        return offset >= splits.size();
    }

    @Override
    public void close()
    {
    }
}
