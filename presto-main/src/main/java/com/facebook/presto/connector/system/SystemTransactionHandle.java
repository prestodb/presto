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
package com.facebook.presto.connector.system;

import com.facebook.presto.connector.CatalogName;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.transaction.TransactionId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SystemTransactionHandle
        implements ConnectorTransactionHandle
{
    private final CatalogName catalogName;
    private final TransactionId transactionId;
    private final Supplier<ConnectorTransactionHandle> connectorTransactionHandle;

    SystemTransactionHandle(
            CatalogName catalogName,
            TransactionId transactionId,
            Function<TransactionId, ConnectorTransactionHandle> transactionHandleFunction)
    {
        this.catalogName = requireNonNull(catalogName, "connectorId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        requireNonNull(transactionHandleFunction, "transactionHandleFunction is null");
        this.connectorTransactionHandle = Suppliers.memoize(() -> transactionHandleFunction.apply(transactionId));
    }

    @JsonCreator
    public SystemTransactionHandle(
            @JsonProperty("connectorId") CatalogName catalogName,
            @JsonProperty("transactionId") TransactionId transactionId,
            @JsonProperty("connectorTransactionHandle") ConnectorTransactionHandle connectorTransactionHandle)
    {
        this.catalogName = requireNonNull(catalogName, "connectorId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        requireNonNull(connectorTransactionHandle, "connectorTransactionHandle is null");
        this.connectorTransactionHandle = () -> connectorTransactionHandle;
    }

    @JsonProperty
    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public TransactionId getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public ConnectorTransactionHandle getConnectorTransactionHandle()
    {
        return connectorTransactionHandle.get();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, transactionId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SystemTransactionHandle other = (SystemTransactionHandle) obj;
        return Objects.equals(this.catalogName, other.catalogName) &&
                Objects.equals(this.transactionId, other.transactionId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", catalogName)
                .add("transactionHandle", transactionId)
                .toString();
    }
}
