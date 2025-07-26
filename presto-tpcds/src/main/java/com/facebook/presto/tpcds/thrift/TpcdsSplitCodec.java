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

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.protocol.TProtocolException;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.tpcds.TpcdsSplit;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.tpcds.thrift.ThriftCodecUtils.fromThrift;
import static com.facebook.presto.tpcds.thrift.ThriftCodecUtils.toThrift;
import static java.util.Objects.requireNonNull;

public class TpcdsSplitCodec
        implements ConnectorCodec<ConnectorSplit>
{
    private final ThriftCodec<TpcdsSplit> thriftCodec;

    public TpcdsSplitCodec(ThriftCodecManager thriftCodecManager)
    {
        this.thriftCodec = requireNonNull(thriftCodecManager, "thriftCodecManager is null").getCodec(TpcdsSplit.class);
    }

    @Override
    public byte[] serialize(ConnectorSplit split)
    {
        try {
            return toThrift((TpcdsSplit) split, thriftCodec);
        }
        catch (TProtocolException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Can not serialize tpcds split", e);
        }
    }

    @Override
    public ConnectorSplit deserialize(byte[] bytes)
    {
        try {
            return fromThrift(bytes, thriftCodec);
        }
        catch (TProtocolException e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Can not deserialize tpcds split", e);
        }
    }
}
