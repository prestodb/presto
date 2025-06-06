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
package com.facebook.presto.hive;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.presto.spi.ConnectorSpecificCodec;
import com.facebook.presto.spi.ConnectorSplit;

import static java.util.Objects.requireNonNull;

public class HiveSplitCodec
        implements ConnectorSpecificCodec<ConnectorSplit>
{
    private final ThriftCodec<HiveSplit> thriftCodec;

    public HiveSplitCodec(ThriftCodecManager thriftCodecManager)
    {
        this.thriftCodec = requireNonNull(thriftCodecManager, "thriftCodecManager is null").getCodec(HiveSplit.class);
    }

    @Override
    public byte[] serialize(ConnectorSplit split)
    {
        HiveSplit hiveSplit = (HiveSplit) split;
        TMemoryBuffer transport = new TMemoryBuffer(1024);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        try {
            thriftCodec.write(hiveSplit, protocol);
            return transport.getBytes();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorSplit deserialize(byte[] bytes)
    {
        try {
            TMemoryBuffer transport = new TMemoryBuffer(bytes.length);
            transport.write(bytes);
            TBinaryProtocol protocol = new TBinaryProtocol(transport);
            return thriftCodec.read(protocol);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
