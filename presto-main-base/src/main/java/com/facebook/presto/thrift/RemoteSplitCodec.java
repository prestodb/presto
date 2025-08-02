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
import com.facebook.drift.protocol.TProtocolException;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.split.RemoteSplit;
import com.google.inject.Provider;

import static com.facebook.presto.server.thrift.ThriftCodecUtils.fromThrift;
import static com.facebook.presto.server.thrift.ThriftCodecUtils.toThrift;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.util.Objects.requireNonNull;

public class RemoteSplitCodec
        implements ConnectorCodec<ConnectorSplit>
{
    private final Provider<ThriftCodecManager> thriftCodecManagerProvider;

    public RemoteSplitCodec(Provider<ThriftCodecManager> thriftCodecManagerProvider)
    {
        this.thriftCodecManagerProvider = requireNonNull(thriftCodecManagerProvider, "thriftCodecManagerProvider is null");
    }

    @Override
    public byte[] serialize(ConnectorSplit split)
    {
        try {
            return toThrift((RemoteSplit) split, thriftCodecManagerProvider.get().getCodec(RemoteSplit.class));
        }
        catch (TProtocolException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Can not serialize remote split", e);
        }
    }

    @Override
    public ConnectorSplit deserialize(byte[] bytes)
    {
        try {
            return fromThrift(bytes, thriftCodecManagerProvider.get().getCodec(RemoteSplit.class));
        }
        catch (TProtocolException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Can not deserialize remote split", e);
        }
    }
}
