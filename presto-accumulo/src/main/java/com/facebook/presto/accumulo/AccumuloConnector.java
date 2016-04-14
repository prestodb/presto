/*
 * Copyright 2016 Bloomberg L.P.
 *
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
package com.facebook.presto.accumulo;

import com.facebook.presto.accumulo.conf.AccumuloSessionProperties;
import com.facebook.presto.accumulo.conf.AccumuloTableProperties;
import com.facebook.presto.accumulo.io.AccumuloPageSinkProvider;
import com.facebook.presto.accumulo.io.AccumuloRecordSetProvider;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.session.PropertyMetadata;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Presto Connector for Accumulo. Defines several high-level classes for properties, metadata,
 * retrieving splits, providing I/O operations, etc.
 */
public class AccumuloConnector
        implements Connector
{
    private static final Logger LOG = Logger.get(AccumuloConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final AccumuloMetadata metadata;
    private final AccumuloSplitManager splitManager;
    private final AccumuloRecordSetProvider recordSetProvider;
    private final AccumuloHandleResolver handleResolver;
    private final AccumuloPageSinkProvider pageSinkProvider;
    private final AccumuloSessionProperties sessionProperties;
    private final AccumuloTableProperties tableProperties;

    /**
     * Creates a new instance of AccumuloConnector, brought to you by Guice.
     *
     * @param lifeCycleManager Manages the life cycle
     * @param metadata Provides metadata operations for creating tables, dropping tables, returing table
     * metadata, etc
     * @param splitManager Splits tables into parallel operations for scans
     * @param recordSetProvider Converts splits into rows of data
     * @param handleResolver Defines various handles for Presto to instantiate
     * @param pageSinkProvider Provides a means to write data to Accumulo via INSERTs
     * @param sessionProperties Defines all Accumulo session properties
     * @param tableProperties Defines all Accumulo table properties
     */
    @Inject
    public AccumuloConnector(LifeCycleManager lifeCycleManager, AccumuloMetadata metadata,
            AccumuloSplitManager splitManager, AccumuloRecordSetProvider recordSetProvider,
            AccumuloHandleResolver handleResolver, AccumuloPageSinkProvider pageSinkProvider,
            AccumuloSessionProperties sessionProperties, AccumuloTableProperties tableProperties)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.tableProperties = requireNonNull(tableProperties, "tableProperties is null");
    }

    /**
     * Gets the metadata, an instance of {@link AccumuloMetadata}
     *
     * @return Metadata
     */
    @Override
    public ConnectorMetadata getMetadata()
    {
        return metadata;
    }

    /**
     * Gets the split manager, an instance of {@link AccumuloSplitManager}
     *
     * @return Split manager
     */
    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    /**
     * Gets the record set provider, an instance of {@link AccumuloRecordSetProvider}
     *
     * @return Record set provider
     */
    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    /**
     * Gets the page sink provider, an instance of {@link AccumuloPageSinkProvider}
     *
     * @return Page sink provider
     */
    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    /**
     * Gets the valid table properties
     *
     * @return List of table properties
     * @see AccumuloTableProperties
     */
    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties.getTableProperties();
    }

    /**
     * Gets the valid session properties
     *
     * @return List of session properties
     * @see AccumuloSessionProperties
     */
    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    /**
     * Shuts down the connector
     */
    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            LOG.error(e, "Error shutting down connector");
        }
    }
}
