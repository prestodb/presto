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

import com.facebook.presto.common.experimental.auto_gen.ThriftBlock;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

public class BlockAdapter
{
    private BlockAdapter() {}

    public static byte[] serialize(Object obj)
    {
        if (obj instanceof ThriftSerializable) {
            ThriftSerializable serializable = (ThriftSerializable) obj;
            byte[] data = ThriftSerializationRegistry.serialize(serializable);

            ThriftBlock thriftBlock = new ThriftBlock();
            thriftBlock.setType(serializable.getImplementationType());
            thriftBlock.setSerializedBlock(data);

            try {
                return new TSerializer(new TJSONProtocol.Factory()).serialize(thriftBlock);
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
            ThriftBlock thriftBlock = new ThriftBlock();
            new TDeserializer(new TJSONProtocol.Factory()).deserialize(thriftBlock, data);

            return fromThrift(thriftBlock);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object fromThrift(ThriftBlock thriftBlock)
    {
        return ThriftSerializationRegistry.deserialize(
                thriftBlock.getType(),
                thriftBlock.getSerializedBlock());
    }
}
