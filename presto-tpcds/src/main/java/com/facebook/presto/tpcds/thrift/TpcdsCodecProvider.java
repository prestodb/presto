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
package com.facebook.presto.tpcds.thrift;

import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.presto.common.thrift.ByteBufferPoolManager;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Provider;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TpcdsCodecProvider
        implements ConnectorCodecProvider
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;
    private final ByteBufferPoolManager byteBufferPoolManager;

    public TpcdsCodecProvider(Provider<ThriftCodecManager> thriftCodecManagerProvider, ByteBufferPoolManager byteBufferPoolManager)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManagerProvider is null");
        this.byteBufferPoolManager = requireNonNull(byteBufferPoolManager, "byteBufferPoolManager is null");
    }

    @Override
    public Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        return Optional.of(new TpcdsSplitCodec(thriftCodecManagerProvider, byteBufferPoolManager));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        return Optional.of(new TpcdsTransactionHandleCodec(thriftCodecManagerProvider, byteBufferPoolManager));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getConnectorTableLayoutHandleCodec()
    {
        return Optional.of(new TpcdsTableLayoutHandleCodec(thriftCodecManagerProvider, byteBufferPoolManager));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableHandle>> getConnectorTableHandleCodec()
    {
        return Optional.of(new TpcdsTableHandleCodec(thriftCodecManagerProvider, byteBufferPoolManager));
    }
}
