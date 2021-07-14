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
package com.facebook.presto.tablestore;

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.tablestore.adapter.RecordPageSinkProvider;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TablestoreConnector
        implements Connector
{
    private static final Logger log = Logger.get(TablestoreConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final TablestoreMetadata tablestoreMetadata;
    private final TablestoreSplitManager tablestoreSplitManager;
    private final TablestoreRecordSetProvider tablestoreRecordSetProvider;
    private final TablestoreRecordSinkProvider tablestoreRecordSinkProvider;
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public TablestoreConnector(LifeCycleManager lifeCycleManager, TablestoreMetadata tablestoreMetadata, TablestoreSplitManager tablestoreSplitManager,
            TablestoreRecordSetProvider tablestoreRecordSetProvider, TablestoreRecordSinkProvider tablestoreRecordSinkProvider,
            TablestoreSessionProperties tablestoreSessionProperties)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.tablestoreMetadata = requireNonNull(tablestoreMetadata, "tablestoreMetadata is null");
        this.tablestoreSplitManager = requireNonNull(tablestoreSplitManager, "tablestoreSplitManager is null");
        this.tablestoreRecordSetProvider = requireNonNull(tablestoreRecordSetProvider, "tablestoreRecordSetProvider is null");
        this.tablestoreRecordSinkProvider = requireNonNull(tablestoreRecordSinkProvider, "tablestoreRecordSinkProvider is null");
        this.sessionProperties = requireNonNull(tablestoreSessionProperties.getSessionProperties(), "sessionProperties is null");
    }

    public ConnectorMetadata getMetadata()
    {
        return tablestoreMetadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return tablestoreSplitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return tablestoreRecordSetProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return new RecordPageSinkProvider(tablestoreRecordSinkProvider);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return new ArrayList<>();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return new ArrayList<>();
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error("shutdown failed.", e);
        }
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return new TablestoreTransactionHandle();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return tablestoreMetadata;
    }
}
