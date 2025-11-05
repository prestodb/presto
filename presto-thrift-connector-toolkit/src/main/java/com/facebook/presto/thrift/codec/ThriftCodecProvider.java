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
package com.facebook.presto.thrift.codec;

import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.lang.reflect.Type;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ThriftCodecProvider
        implements ConnectorCodecProvider
{
    private final ThriftCodecManager thriftCodecManager;
    private final Optional<Type> connectorSplitType;
    private final Optional<Type> connectorTransactionHandle;
    private final Optional<Type> connectorTableLayoutHandle;
    private final Optional<Type> connectorTableHandle;
    private final Optional<Type> connectorOutputTableHandle;
    private final Optional<Type> connectorInsertTableHandle;
    private final Optional<Type> connectorDeleteTableHandle;
    private final Optional<Type> connectorColumnHandle;

    private ThriftCodecProvider(Builder builder)
    {
        this.thriftCodecManager = requireNonNull(builder.thriftCodecManager, "thriftCodecManager is null");
        this.connectorSplitType = builder.connectorSplitType;
        this.connectorTransactionHandle = builder.connectorTransactionHandle;
        this.connectorTableLayoutHandle = builder.connectorTableLayoutHandle;
        this.connectorTableHandle = builder.connectorTableHandle;
        this.connectorOutputTableHandle = builder.connectorOutputTableHandle;
        this.connectorInsertTableHandle = builder.connectorInsertTableHandle;
        this.connectorDeleteTableHandle = builder.connectorDeleteTableHandle;
        this.connectorColumnHandle = builder.connectorColumnHandle;
    }

    @Override
    public Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        return connectorSplitType.map(type -> new GenericThriftCodec<>(thriftCodecManager, type, ConnectorSplit.class));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        return connectorTransactionHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type, ConnectorTransactionHandle.class));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getConnectorTableLayoutHandleCodec()
    {
        return connectorTableLayoutHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type, ConnectorTableLayoutHandle.class));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableHandle>> getConnectorTableHandleCodec()
    {
        return connectorTableHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type, ConnectorTableHandle.class));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorOutputTableHandle>> getConnectorOutputTableHandleCodec()
    {
        return connectorOutputTableHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type, ConnectorOutputTableHandle.class));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorInsertTableHandle>> getConnectorInsertTableHandleCodec()
    {
        return connectorInsertTableHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type, ConnectorInsertTableHandle.class));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorDeleteTableHandle>> getConnectorDeleteTableHandleCodec()
    {
        return connectorDeleteTableHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type, ConnectorDeleteTableHandle.class));
    }

    @Override
    public Optional<ConnectorCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return connectorColumnHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type, ColumnHandle.class));
    }

    public ThriftCodecManager getThriftCodecManager()
    {
        return thriftCodecManager;
    }

    public static class Builder
    {
        private ThriftCodecManager thriftCodecManager;
        private Optional<Type> connectorSplitType = Optional.empty();
        private Optional<Type> connectorTransactionHandle = Optional.empty();
        private Optional<Type> connectorTableLayoutHandle = Optional.empty();
        private Optional<Type> connectorTableHandle = Optional.empty();
        private Optional<Type> connectorOutputTableHandle = Optional.empty();
        private Optional<Type> connectorInsertTableHandle = Optional.empty();
        private Optional<Type> connectorDeleteTableHandle = Optional.empty();
        private Optional<Type> connectorColumnHandle = Optional.empty();

        public Builder setThriftCodecManager(ThriftCodecManager thriftCodecManager)
        {
            this.thriftCodecManager = thriftCodecManager;
            return this;
        }

        public Builder setConnectorSplitType(Class<? extends ConnectorSplit> type)
        {
            this.connectorSplitType = Optional.ofNullable(type);
            return this;
        }

        public Builder setConnectorTransactionHandle(Class<? extends ConnectorTransactionHandle> type)
        {
            this.connectorTransactionHandle = Optional.ofNullable(type);
            return this;
        }

        public Builder setConnectorTableLayoutHandle(Class<? extends ConnectorTableLayoutHandle> type)
        {
            this.connectorTableLayoutHandle = Optional.ofNullable(type);
            return this;
        }

        public Builder setConnectorTableHandle(Class<? extends ConnectorTableHandle> type)
        {
            this.connectorTableHandle = Optional.ofNullable(type);
            return this;
        }

        public Builder setConnectorOutputTableHandle(Class<? extends ConnectorOutputTableHandle> type)
        {
            this.connectorOutputTableHandle = Optional.ofNullable(type);
            return this;
        }

        public Builder setConnectorInsertTableHandle(Class<? extends ConnectorInsertTableHandle> type)
        {
            this.connectorInsertTableHandle = Optional.ofNullable(type);
            return this;
        }

        public Builder setConnectorDeleteTableHandle(Class<? extends ConnectorDeleteTableHandle> type)
        {
            this.connectorDeleteTableHandle = Optional.ofNullable(type);
            return this;
        }

        public <T extends ColumnHandle> Builder setConnectorColumnHandle(Class<? extends ColumnHandle> type)
        {
            this.connectorColumnHandle = Optional.ofNullable(type);
            return this;
        }

        public ThriftCodecProvider build()
        {
            return new ThriftCodecProvider(this);
        }
    }
}
