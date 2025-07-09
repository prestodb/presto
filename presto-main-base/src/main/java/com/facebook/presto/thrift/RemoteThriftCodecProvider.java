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
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorThriftCodec;
import com.facebook.presto.spi.connector.ConnectorThriftCodecProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Provider;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RemoteThriftCodecProvider
        implements ConnectorThriftCodecProvider
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;

    public RemoteThriftCodecProvider(Provider<ThriftCodecManager> thriftCodecManagerProvider)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManagerProvider is null");
    }

    @Override
    public Optional<ConnectorThriftCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        return Optional.of(new RemoteSplitThriftCodec(thriftCodecManagerProvider));
    }

    @Override
    public Optional<ConnectorThriftCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        return Optional.of(new RemoteTransactionHandleThriftCodec(thriftCodecManagerProvider));
    }
}
