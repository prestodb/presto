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
package com.facebook.presto.split;

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConnectorAwareSplitSource
        implements SplitSource
{
    private final String connectorId;
    private final ConnectorSplitSource source;

    public ConnectorAwareSplitSource(String connectorId, ConnectorSplitSource source)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.source = checkNotNull(source, "source is null");
    }

    @Override
    public String getDataSourceName()
    {
        return source.getDataSourceName();
    }

    @Override
    public CompletableFuture<List<Split>> getNextBatch(int maxSize)
    {
        return source.getNextBatch(maxSize)
                .thenApply(splits -> Lists.transform(splits, split -> new Split(connectorId, split)));
    }

    @Override
    public void close()
    {
        source.close();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + source;
    }
}
