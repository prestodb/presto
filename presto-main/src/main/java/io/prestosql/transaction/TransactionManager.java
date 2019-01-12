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
package io.prestosql.transaction;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.connector.ConnectorId;
import io.prestosql.metadata.CatalogMetadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface TransactionManager
{
    IsolationLevel DEFAULT_ISOLATION = IsolationLevel.READ_UNCOMMITTED;
    boolean DEFAULT_READ_ONLY = false;

    boolean transactionExists(TransactionId transactionId);

    default boolean isAutoCommit(TransactionId transactionId)
    {
        return getTransactionInfo(transactionId).isAutoCommitContext();
    }

    TransactionInfo getTransactionInfo(TransactionId transactionId);

    List<TransactionInfo> getAllTransactionInfos();

    TransactionId beginTransaction(boolean autoCommitContext);

    TransactionId beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext);

    Map<String, ConnectorId> getCatalogNames(TransactionId transactionId);

    Optional<CatalogMetadata> getOptionalCatalogMetadata(TransactionId transactionId, String catalogName);

    CatalogMetadata getCatalogMetadata(TransactionId transactionId, ConnectorId connectorId);

    CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, ConnectorId connectorId);

    CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, String catalogName);

    ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, ConnectorId connectorId);

    void checkAndSetActive(TransactionId transactionId);

    void trySetActive(TransactionId transactionId);

    void trySetInactive(TransactionId transactionId);

    ListenableFuture<?> asyncCommit(TransactionId transactionId);

    ListenableFuture<?> asyncAbort(TransactionId transactionId);

    void fail(TransactionId transactionId);

    default void activateTransaction(Session session, boolean transactionControl, AccessControl accessControl)
    {
        if (!session.getTransactionId().isPresent()) {
            return;
        }

        // reactivate existing transaction
        TransactionId transactionId = session.getTransactionId().get();
        if (transactionControl) {
            trySetActive(transactionId);
        }
        else {
            checkAndSetActive(transactionId);
        }
    }
}
