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
package com.facebook.presto.hive;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;

import static com.google.common.base.Preconditions.checkNotNull;

public class HiveConnector
        implements Connector
{
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorRecordSinkProvider recordSinkProvider;
    private final ConnectorHandleResolver handleResolver;

    public HiveConnector(
            ConnectorMetadata metadata,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorRecordSinkProvider recordSinkProvider,
            ConnectorHandleResolver handleResolver)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.pageSourceProvider = checkNotNull(pageSourceProvider, "pageSourceProvider is null");
        this.recordSinkProvider = checkNotNull(recordSinkProvider, "recordSinkProvider is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
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

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return recordSinkProvider;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
    }
}
