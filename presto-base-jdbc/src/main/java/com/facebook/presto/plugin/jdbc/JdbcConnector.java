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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorRecordSinkProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class JdbcConnector
        implements Connector
{
    private static final Logger log = Logger.get(JdbcConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final JdbcMetadata jdbcMetadata;
    private final JdbcSplitManager jdbcSplitManager;
    private final JdbcRecordSetProvider jdbcRecordSetProvider;
    private final JdbcRecordSinkProvider jdbcRecordSinkProvider;

    @Inject
    public JdbcConnector(
            LifeCycleManager lifeCycleManager,
            JdbcMetadata jdbcMetadata,
            JdbcSplitManager jdbcSplitManager,
            JdbcRecordSetProvider jdbcRecordSetProvider,
            JdbcRecordSinkProvider jdbcRecordSinkProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.jdbcMetadata = requireNonNull(jdbcMetadata, "jdbcMetadata is null");
        this.jdbcSplitManager = requireNonNull(jdbcSplitManager, "jdbcSplitManager is null");
        this.jdbcRecordSetProvider = requireNonNull(jdbcRecordSetProvider, "jdbcRecordSetProvider is null");
        this.jdbcRecordSinkProvider = requireNonNull(jdbcRecordSinkProvider, "jdbcRecordSinkProvider is null");
    }

    @Override
    public ConnectorMetadata getMetadata()
    {
        return jdbcMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return jdbcSplitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return jdbcRecordSetProvider;
    }

    @Override
    public ConnectorRecordSinkProvider getRecordSinkProvider()
    {
        return jdbcRecordSinkProvider;
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
