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
package com.facebook.presto.transaction;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.CatalogMetadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DelegatingTransactionManager
        implements TransactionManager
{
    private final TransactionManager delegate;

    public TransactionManager getDelegate()
    {
        return delegate;
    }

    public DelegatingTransactionManager(TransactionManager delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public TransactionInfo getTransactionInfo(TransactionId transactionId)
    {
        return delegate.getTransactionInfo(transactionId);
    }

    @Override
    public Optional<TransactionInfo> getOptionalTransactionInfo(TransactionId transactionId)
    {
        return delegate.getOptionalTransactionInfo(transactionId);
    }

    @Override
    public List<TransactionInfo> getAllTransactionInfos()
    {
        return delegate.getAllTransactionInfos();
    }

    @Override
    public TransactionId beginTransaction(boolean autoCommitContext)
    {
        return delegate.beginTransaction(autoCommitContext);
    }

    @Override
    public TransactionId beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext)
    {
        return delegate.beginTransaction(isolationLevel, readOnly, autoCommitContext);
    }

    @Override
    public Map<String, ConnectorId> getCatalogNames(TransactionId transactionId)
    {
        return delegate.getCatalogNames(transactionId);
    }

    @Override
    public Optional<CatalogMetadata> getOptionalCatalogMetadata(TransactionId transactionId, String catalogName)
    {
        return delegate.getOptionalCatalogMetadata(transactionId, catalogName);
    }

    @Override
    public CatalogMetadata getCatalogMetadata(TransactionId transactionId, ConnectorId connectorId)
    {
        return delegate.getCatalogMetadata(transactionId, connectorId);
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, ConnectorId connectorId)
    {
        return delegate.getCatalogMetadataForWrite(transactionId, connectorId);
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, String catalogName)
    {
        return delegate.getCatalogMetadataForWrite(transactionId, catalogName);
    }

    @Override
    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, ConnectorId connectorId)
    {
        return delegate.getConnectorTransaction(transactionId, connectorId);
    }

    @Override
    public void checkAndSetActive(TransactionId transactionId)
    {
        delegate.checkAndSetActive(transactionId);
    }

    @Override
    public void trySetActive(TransactionId transactionId)
    {
        delegate.trySetActive(transactionId);
    }

    @Override
    public void trySetInactive(TransactionId transactionId)
    {
        delegate.trySetInactive(transactionId);
    }

    @Override
    public ListenableFuture<?> asyncCommit(TransactionId transactionId)
    {
        return delegate.asyncCommit(transactionId);
    }

    @Override
    public ListenableFuture<?> asyncAbort(TransactionId transactionId)
    {
        return delegate.asyncAbort(transactionId);
    }

    @Override
    public void fail(TransactionId transactionId)
    {
        delegate.fail(transactionId);
    }

    @Override
    public void activateTransaction(Session session, boolean transactionControl, AccessControl accessControl)
    {
        delegate.activateTransaction(session, transactionControl, accessControl);
    }

    @Override
    public void registerFunctionNamespaceManager(String catalogName, FunctionNamespaceManager<?> functionNamespaceManager)
    {
        delegate.registerFunctionNamespaceManager(catalogName, functionNamespaceManager);
    }

    @Override
    public FunctionNamespaceTransactionHandle getFunctionNamespaceTransaction(TransactionId transactionId, String catalogName)
    {
        return delegate.getFunctionNamespaceTransaction(transactionId, catalogName);
    }
}
