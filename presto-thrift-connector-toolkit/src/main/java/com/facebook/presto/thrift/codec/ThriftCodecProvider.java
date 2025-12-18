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
    private final Optional<ConnectorCodec<ConnectorSplit>> connectorSplitCodec;
    private final Optional<ConnectorCodec<ConnectorTransactionHandle>> connectorTransactionHandleCodec;
    private final Optional<ConnectorCodec<ConnectorTableLayoutHandle>> connectorTableLayoutHandleCodec;
    private final Optional<ConnectorCodec<ConnectorTableHandle>> connectorTableHandleCodec;
    private final Optional<ConnectorCodec<ConnectorOutputTableHandle>> connectorOutputTableHandleCodec;
    private final Optional<ConnectorCodec<ConnectorInsertTableHandle>> connectorInsertTableHandleCodec;
    private final Optional<ConnectorCodec<ConnectorDeleteTableHandle>> connectorDeleteTableHandleCodec;

    private ThriftCodecProvider(Builder builder)
    {
        this.thriftCodecManager = requireNonNull(builder.thriftCodecManager, "thriftCodecManager is null");
        this.connectorSplitCodec = builder.connectorSplitType.map(type -> new GenericThriftCodec<>(thriftCodecManager, type));
        this.connectorTransactionHandleCodec = builder.connectorTransactionHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type));
        this.connectorTableLayoutHandleCodec = builder.connectorTableLayoutHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type));
        this.connectorTableHandleCodec = builder.connectorTableHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type));
        this.connectorOutputTableHandleCodec = builder.connectorOutputTableHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type));
        this.connectorInsertTableHandleCodec = builder.connectorInsertTableHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type));
        this.connectorDeleteTableHandleCodec = builder.connectorDeleteTableHandle.map(type -> new GenericThriftCodec<>(thriftCodecManager, type));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        return connectorSplitCodec;
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        return connectorTransactionHandleCodec;
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getConnectorTableLayoutHandleCodec()
    {
        return connectorTableLayoutHandleCodec;
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableHandle>> getConnectorTableHandleCodec()
    {
        return connectorTableHandleCodec;
    }

    @Override
    public Optional<ConnectorCodec<ConnectorOutputTableHandle>> getConnectorOutputTableHandleCodec()
    {
        return connectorOutputTableHandleCodec;
    }

    @Override
    public Optional<ConnectorCodec<ConnectorInsertTableHandle>> getConnectorInsertTableHandleCodec()
    {
        return connectorInsertTableHandleCodec;
    }

    @Override
    public Optional<ConnectorCodec<ConnectorDeleteTableHandle>> getConnectorDeleteTableHandleCodec()
    {
        return connectorDeleteTableHandleCodec;
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

        public ThriftCodecProvider build()
        {
            requireNonNull(thriftCodecManager, "ThriftCodecManager not set");
            return new ThriftCodecProvider(this);
        }
    }
}
