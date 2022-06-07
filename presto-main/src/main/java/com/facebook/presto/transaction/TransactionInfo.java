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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@ThriftStruct
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

    @ThriftConstructor
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

    @ThriftField(value = 1)
    public TransactionId getTransactionId()
    {
        return transactionId;
    }

    @ThriftField(value = 2)
    public IsolationLevel getIsolationLevel()
    {
        return isolationLevel;
    }

    @ThriftField(value = 3)
    public boolean isReadOnly()
    {
        return readOnly;
    }

    @ThriftField(value = 4)
    public boolean isAutoCommitContext()
    {
        return autoCommitContext;
    }

    @ThriftField(value = 5)
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @ThriftField(value = 6)
    public Duration getIdleTime()
    {
        return idleTime;
    }

    @ThriftField(value = 7)
    public List<ConnectorId> getConnectorIds()
    {
        return connectorIds;
    }

    @ThriftField(value = 8)
    public Optional<ConnectorId> getWrittenConnectorId()
    {
        return writtenConnectorId;
    }
}
