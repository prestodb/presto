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
package com.facebook.presto.lark.sheets;

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.presto.lark.sheets.api.CachingLarkSheetsApi;
import com.facebook.presto.lark.sheets.api.LarkSheetsApi;
import com.facebook.presto.lark.sheets.api.LarkSheetsSchemaStore;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;
import java.util.function.Supplier;

import static com.facebook.presto.spi.connector.EmptyConnectorCommitHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class LarkSheetsConnector
        implements Connector
{
    private final LarkSheetsTransactionManager transactionManager;
    private final LarkSheetsSchemaProperties schemaProperties;
    private final Supplier<LarkSheetsApi> apiFactory;
    private final LarkSheetsSchemaStore schemaStore;
    private final LarkSheetsSplitManager splitManager;
    private final LarkSheetsRecordSetProvider recordSetProvider;
    private final LifeCycleManager lifeCycleManager;

    @Inject
    public LarkSheetsConnector(
            LarkSheetsTransactionManager transactionManager,
            LarkSheetsSchemaProperties schemaProperties,
            LarkSheetsSchemaStore schemaStore,
            Supplier<LarkSheetsApi> apiFactory,
            LarkSheetsSplitManager splitManager,
            LarkSheetsRecordSetProvider recordSetProvider,
            LifeCycleManager lifeCycleManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.schemaProperties = requireNonNull(schemaProperties, "schemaProperties is null");
        this.apiFactory = requireNonNull(apiFactory, "apiFactory is null");
        this.schemaStore = requireNonNull(schemaStore, "schemaStore is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        LarkSheetsTransactionHandle transaction = new LarkSheetsTransactionHandle();

        LarkSheetsApi api = new CachingLarkSheetsApi(apiFactory.get());
        LarkSheetsMetadata metadata = new LarkSheetsMetadata(api, schemaStore);
        transactionManager.put(transaction, metadata);

        return transaction;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return transactionManager.get(transactionHandle);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties.getSchemaProperties();
    }

    @Override
    public ConnectorCommitHandle commit(ConnectorTransactionHandle transactionHandle)
    {
        transactionManager.remove(transactionHandle);
        return INSTANCE;
    }

    @Override
    public void rollback(ConnectorTransactionHandle transactionHandle)
    {
        transactionManager.remove(transactionHandle);
    }

    @Override
    public void shutdown()
    {
        lifeCycleManager.stop();
    }
}
