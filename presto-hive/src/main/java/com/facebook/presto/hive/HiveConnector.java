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
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.security.ConnectorAccessControl;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class HiveConnector
        implements Connector
{
    private static final Logger log = Logger.get(HiveConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final ConnectorHandleResolver handleResolver;
    private final Set<SystemTable> systemTables;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> tableProperties;
    private final ConnectorAccessControl accessControl;

    public HiveConnector(
            LifeCycleManager lifeCycleManager,
            ConnectorMetadata metadata,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            ConnectorHandleResolver handleResolver,
            Set<SystemTable> systemTables,
            List<PropertyMetadata<?>> sessionProperties,
            List<PropertyMetadata<?>> tableProperties,
            ConnectorAccessControl accessControl)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.systemTables = ImmutableSet.copyOf(requireNonNull(systemTables, "systemTables is null"));
        this.sessionProperties = ImmutableList.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
        this.tableProperties = ImmutableList.copyOf(requireNonNull(tableProperties, "tableProperties is null"));
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
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
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return handleResolver;
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
