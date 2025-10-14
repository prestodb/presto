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
package com.facebook.presto.thrift;

import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.presto.common.thrift.ByteBufferPoolManager;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.split.RemoteSplit;
import com.google.inject.Provider;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;

import static com.facebook.presto.server.thrift.ThriftCodecUtils.deserializeConcreteValue;
import static com.facebook.presto.server.thrift.ThriftCodecUtils.serializeConcreteValue;
import static java.util.Objects.requireNonNull;

public class RemoteSplitCodec
        implements ConnectorCodec<ConnectorSplit>
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;
    private final ByteBufferPoolManager byteBufferPoolManager;

    public RemoteSplitCodec(Provider<ThriftCodecManager> thriftCodecManagerProvider, ByteBufferPoolManager byteBufferPoolManager)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManagerProvider is null");
        this.byteBufferPoolManager = requireNonNull(byteBufferPoolManager, "byteBufferPoolManager is null");
    }

    @Override
    public void serialize(ConnectorSplit connectorSplit, Consumer<List<ByteBuffer>> consumer)
            throws Exception
    {
        requireNonNull(connectorSplit, "split is null");
        requireNonNull(consumer, "consumer is null");

        RemoteSplit remoteSplit = (RemoteSplit) connectorSplit;

        serializeConcreteValue(remoteSplit, thriftCodecManagerProvider.get().getCodec(RemoteSplit.class), byteBufferPoolManager.getPool(), consumer);
    }

    @Override
    public ConnectorSplit deserialize(List<ByteBuffer> byteBuffers)
            throws Exception
    {
        requireNonNull(byteBuffers, "byteBuffers is null");

        return deserializeConcreteValue(byteBuffers, thriftCodecManagerProvider.get().getCodec(RemoteSplit.class));
    }
}
