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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TransactionInfo
{
    private final TransactionId transactionId;
    private final IsolationLevel isolationLevel;
    private final boolean readOnly;
    private final boolean autoCommitContext;
    private final DateTime createTime;
    private final Duration idleTime;
    private final List<ConnectorId> connectorIds;
    private final Optional<ConnectorId> writtenConnectorId;

    public TransactionInfo(
            TransactionId transactionId,
            IsolationLevel isolationLevel,
            boolean readOnly,
            boolean autoCommitContext,
            DateTime createTime,
            Duration idleTime,
            List<ConnectorId> connectorIds,
            Optional<ConnectorId> writtenConnectorId)
    {
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.isolationLevel = requireNonNull(isolationLevel, "isolationLevel is null");
        this.readOnly = readOnly;
        this.autoCommitContext = autoCommitContext;
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.idleTime = requireNonNull(idleTime, "idleTime is null");
        this.connectorIds = ImmutableList.copyOf(requireNonNull(connectorIds, "connectorIds is null"));
        this.writtenConnectorId = requireNonNull(writtenConnectorId, "writtenConnectorId is null");
    }

    public TransactionId getTransactionId()
    {
        return transactionId;
    }

    public IsolationLevel getIsolationLevel()
    {
        return isolationLevel;
    }

    public boolean isReadOnly()
    {
        return readOnly;
    }

    public boolean isAutoCommitContext()
    {
        return autoCommitContext;
    }

    public DateTime getCreateTime()
    {
        return createTime;
    }

    public Duration getIdleTime()
    {
        return idleTime;
    }

    public List<ConnectorId> getConnectorIds()
    {
        return connectorIds;
    }

    public Optional<ConnectorId> getWrittenConnectorId()
    {
        return writtenConnectorId;
    }
}
