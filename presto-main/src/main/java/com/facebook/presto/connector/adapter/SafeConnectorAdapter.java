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
package com.facebook.presto.connector.adapter;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SafeConnector;

import static com.google.common.base.Preconditions.checkNotNull;

public class SafeConnectorAdapter<
        TH extends ConnectorTableHandle,
        CH extends ConnectorColumnHandle,
        S extends ConnectorSplit,
        IH extends ConnectorIndexHandle,
        OTH extends ConnectorOutputTableHandle,
        ITH extends ConnectorInsertTableHandle,
        P extends ConnectorPartition>
        implements Connector
{
    private final String connectorId;
    private final SafeConnector<TH, CH, S, IH, OTH, ITH, P> delegate;
    private final SafeTypeAdapter<TH, CH, S, IH, OTH, ITH, P> typeAdapter;

    public SafeConnectorAdapter(String connectorId, SafeConnector<TH, CH, S, IH, OTH, ITH, P> delegate, SafeTypeAdapter<TH, CH, S, IH, OTH, ITH, P> typeAdapter)
    {
        this.connectorId = connectorId;
        this.delegate = checkNotNull(delegate, "delegate is null");
        this.typeAdapter = checkNotNull(typeAdapter, "typeAdapter is null");
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return typeAdapter;
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return new SafeMetadataAdapter<>(delegate.getMetadata(), typeAdapter);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new SafeSplitManagerAdapter<>(connectorId, delegate.getSplitManager(), typeAdapter);
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return new SafeRecordSetProviderAdapter<>(delegate.getRecordSetProvider(), typeAdapter);
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return new SafeRecordSinkProviderAdapter<>(delegate.getRecordSinkProvider(), typeAdapter);
    }

    @Override
    public ConnectorIndexResolver getIndexResolver()
    {
        return new SafeIndexResolverAdapter<>(delegate.getIndexResolver(), typeAdapter);
    }
}
