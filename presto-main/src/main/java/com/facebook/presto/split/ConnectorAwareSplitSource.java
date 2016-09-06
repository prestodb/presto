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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class ConnectorAwareSplitSource
        implements SplitSource
{
    private final ConnectorId connectorId;
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorSplitSource source;

    public ConnectorAwareSplitSource(ConnectorId connectorId, ConnectorTransactionHandle transactionHandle, ConnectorSplitSource source)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public ConnectorId getConnectorId()
    {
        return connectorId;
    }

    @Override
    public CompletableFuture<List<Split>> getNextBatch(int maxSize)
    {
        return source.getNextBatch(maxSize)
                .thenApply(splits -> Lists.transform(splits, split -> new Split(connectorId, transactionHandle, split)));
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
