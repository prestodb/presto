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

import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorOutputTableHandle;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

public class ConnectorOutputTableHandleAdapter
{
    private ConnectorOutputTableHandleAdapter() {}

    public static byte[] serialize(Object obj)
    {
        if (obj instanceof ThriftSerializable) {
            ThriftSerializable serializable = (ThriftSerializable) obj;
            byte[] data = ThriftSerializationRegistry.serialize(serializable);

            ThriftConnectorOutputTableHandle thriftHandle = new ThriftConnectorOutputTableHandle();
            thriftHandle.setType(serializable.getImplementationType());
            thriftHandle.setSerializedOutputTableHandle(data);

            try {
                return new TSerializer(new TJSONProtocol.Factory()).serialize(thriftHandle);
            }
            catch (TException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IllegalArgumentException("Unsupported type: " + obj.getClass());
    }

    public static Object deserialize(byte[] data)
    {
        try {
            ThriftConnectorOutputTableHandle thriftHandle = new ThriftConnectorOutputTableHandle();
            new TDeserializer(new TJSONProtocol.Factory()).deserialize(thriftHandle, data);

            return fromThrift(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object fromThrift(ThriftConnectorOutputTableHandle thriftElement)
    {
        if (thriftElement.getType() == null) {
            return null;
        }
        return ThriftSerializationRegistry.deserialize(
                thriftElement.getType(),
                thriftElement.getSerializedOutputTableHandle());
    }
}
