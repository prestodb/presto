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
package com.facebook.presto.connector;

import com.facebook.presto.metadata.NativeHandleResolver;
import com.facebook.presto.metadata.NativeMetadata;
import com.facebook.presto.metadata.NativeRecordSinkProvider;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputHandleResolver;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.split.NativeDataStreamProvider;
import com.facebook.presto.split.NativeSplitManager;

import javax.inject.Inject;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class NativeConnectorFactory
        implements ConnectorFactory
{
    private final NativeMetadata metadata;
    private final NativeSplitManager splitManager;
    private final NativeDataStreamProvider dataStreamProvider;
    private final NativeRecordSinkProvider recordSinkProvider;

    @Inject
    public NativeConnectorFactory(
            NativeMetadata metadata,
            NativeSplitManager splitManager,
            NativeDataStreamProvider dataStreamProvider,
            NativeRecordSinkProvider recordSinkProvider)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.dataStreamProvider = checkNotNull(dataStreamProvider, "dataStreamProvider is null");
        this.recordSinkProvider = checkNotNull(recordSinkProvider, "recordSinkProvider is null");
    }

    @Override
    public String getName()
    {
        return "native";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> properties)
    {
        return new InternalConnector() {
            @Override
            public ConnectorDataStreamProvider getDataStreamProvider()
            {
                return dataStreamProvider;
            }

            @Override
            public ConnectorHandleResolver getHandleResolver()
            {
                return new NativeHandleResolver();
            }

            @Override
            public ConnectorMetadata getMetadata()
            {
                return metadata;
            }

            @Override
            public ConnectorSplitManager getSplitManager()
            {
                return splitManager;
            }

            @Override
            public ConnectorRecordSinkProvider getRecordSinkProvider()
            {
                return recordSinkProvider;
            }

            @Override
            public ConnectorOutputHandleResolver getOutputHandleResolver()
            {
                return new NativeHandleResolver();
            }

            @Override
            public ConnectorRecordSetProvider getRecordSetProvider()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ConnectorIndexResolver getIndexResolver()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
