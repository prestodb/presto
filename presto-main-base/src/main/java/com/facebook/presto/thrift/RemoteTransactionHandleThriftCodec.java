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
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.spi.ConnectorThriftCodec;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Provider;

import static com.facebook.presto.server.thrift.ThriftCodecUtils.fromThrift;
import static com.facebook.presto.server.thrift.ThriftCodecUtils.toThrift;
import static java.util.Objects.requireNonNull;

public class RemoteTransactionHandleThriftCodec
        implements ConnectorThriftCodec<ConnectorTransactionHandle>
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;

    public RemoteTransactionHandleThriftCodec(Provider<ThriftCodecManager> thriftCodecManagerProvider)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManagerProvider is null");
    }

    @Override
    public byte[] serialize(ConnectorTransactionHandle handle)
    {
        return toThrift((RemoteTransactionHandle) handle, thriftCodecManagerProvider.get().getCodec(RemoteTransactionHandle.class));
    }

    @Override
    public ConnectorTransactionHandle deserialize(byte[] bytes)
    {
        return fromThrift(bytes, thriftCodecManagerProvider.get().getCodec(RemoteTransactionHandle.class));
    }
}
