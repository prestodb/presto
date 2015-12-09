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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.NodeManager;

import static java.util.Objects.requireNonNull;

public class InformationSchemaConnector
        implements Connector
{
    private final ConnectorHandleResolver handleResolver;
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;

    public InformationSchemaConnector(String connectorId, String catalogName, NodeManager nodeManager, Metadata metadata)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(metadata, "metadata is null");

        this.handleResolver = new InformationSchemaHandleResolver(connectorId);
        this.metadata = new InformationSchemaMetadata(connectorId, catalogName);
        this.splitManager = new InformationSchemaSplitManager(nodeManager);
        this.pageSourceProvider = new InformationSchemaPageSourceProvider(metadata);
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
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
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }
}
