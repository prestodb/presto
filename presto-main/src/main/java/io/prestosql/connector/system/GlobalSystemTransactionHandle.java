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
package io.prestosql.connector.system;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.transaction.TransactionId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GlobalSystemTransactionHandle
        implements ConnectorTransactionHandle
{
    private final String connectorId;
    private final TransactionId transactionId;

    @JsonCreator
    public GlobalSystemTransactionHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("transactionId") TransactionId transactionId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public TransactionId getTransactionId()
    {
        return transactionId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, transactionId);
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
        final GlobalSystemTransactionHandle other = (GlobalSystemTransactionHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.transactionId, other.transactionId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("transactionId", transactionId)
                .toString();
    }
}
