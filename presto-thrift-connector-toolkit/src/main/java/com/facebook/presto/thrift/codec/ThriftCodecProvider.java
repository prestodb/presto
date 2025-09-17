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
    private final Optional<Type> connectorSplitType;
    private final Optional<Type> connectorTransactionHandle;
    private final Optional<Type> connectorTableLayoutHandle;
    private final Optional<Type> connectorTableHandle;
    private final Optional<Type> connectorOutputTableHandle;
    private final Optional<Type> connectorInsertTableHandle;
    private final Optional<Type> connectorDeleteTableHandle;

    public ThriftCodecProvider(ThriftCodecManager thriftCodecManager,
                               Optional<Type> connectorSplitType,
                               Optional<Type> connectorTransactionHandle,
                               Optional<Type> connectorTableLayoutHandle,
                               Optional<Type> connectorTableHandle,
                               Optional<Type> connectorOutputTableHandle,
                               Optional<Type> connectorInsertTableHandle,
                               Optional<Type> connectorDeleteTableHandle)
    {
        this.thriftCodecManager = requireNonNull(thriftCodecManager, "thriftCodecManager is null");
        this.connectorSplitType = connectorSplitType;
        this.connectorTransactionHandle = connectorTransactionHandle;
        this.connectorTableLayoutHandle = connectorTableLayoutHandle;
        this.connectorTableHandle = connectorTableHandle;
        this.connectorOutputTableHandle = connectorOutputTableHandle;
        this.connectorInsertTableHandle = connectorInsertTableHandle;
        this.connectorDeleteTableHandle = connectorDeleteTableHandle;
    }

    @Override
    public Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        return connectorSplitType.map(type -> new ThriftSplitCodec(thriftCodecManager, type));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        return connectorTransactionHandle.map(type -> new ThriftTransactionHandleCodec(thriftCodecManager, type));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getConnectorTableLayoutHandleCodec()
    {
        return connectorTableLayoutHandle.map(type -> new ThriftTableLayoutHandleCodec(thriftCodecManager, type));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableHandle>> getConnectorTableHandleCodec()
    {
        return connectorTableHandle.map(type -> new ThriftTableHandleCodec(thriftCodecManager, type));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorOutputTableHandle>> getConnectorOutputTableHandleCodec()
    {
        return connectorOutputTableHandle.map(type -> new ThriftOutputTableHandleCodec(thriftCodecManager, type));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorInsertTableHandle>> getConnectorInsertTableHandleCodec()
    {
        return connectorInsertTableHandle.map(type -> new ThriftInsertTableHandleCodec(thriftCodecManager, type));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorDeleteTableHandle>> getConnectorDeleteTableHandleCodec()
    {
        return connectorDeleteTableHandle.map(type -> new ThriftDeleteTableHandleCodec(thriftCodecManager, type));
    }
}
