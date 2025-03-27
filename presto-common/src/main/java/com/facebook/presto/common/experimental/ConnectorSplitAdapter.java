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
package com.facebook.presto.common.experimental;

import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorSplit;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import static com.google.common.base.Preconditions.checkNotNull;

public class ConnectorSplitAdapter
{
    private ConnectorSplitAdapter() {}

    public static byte[] serialize(Object obj)
    {
        ThriftSerializable serializable = (ThriftSerializable) obj;
        byte[] data = ThriftSerializationRegistry.serialize(serializable);

        ThriftConnectorSplit thriftSplit = new ThriftConnectorSplit();
        thriftSplit.setType(serializable.getImplementationType());
        thriftSplit.setSerializedSplit(data);

        try {
            return new TSerializer(new TBinaryProtocol.Factory()).serialize(thriftSplit);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object deserialize(byte[] data)
    {
        try {
            ThriftConnectorSplit thriftSplit = new ThriftConnectorSplit();
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(thriftSplit, data);

            return fromThrift(thriftSplit);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object fromThrift(ThriftConnectorSplit thriftSplit)
    {
        checkNotNull(thriftSplit, "ThriftConnectorSplit is null");
        return ThriftSerializationRegistry.deserialize(
                thriftSplit.getType(),
                thriftSplit.getSerializedSplit());
    }
}
