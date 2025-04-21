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

import static java.util.Objects.requireNonNull;

public class ConnectorSplitAdapter
{
    private ConnectorSplitAdapter() {}

    public static byte[] serialize(Object obj)
    {
        if (obj instanceof ThriftSerializable) {
            ThriftSerializable serializable = (ThriftSerializable) obj;
            byte[] bytes = ThriftSerializationRegistry.serialize(serializable);

            return FbThriftUtils.serialize(ThriftConnectorSplit.builder()
                    .setType(serializable.getImplementationType())
                    .setSerializedSplit(bytes).build());
        }
        throw new IllegalArgumentException("Unsupported type: " + obj.getClass());
    }

    public static Object deserialize(byte[] bytes)
    {
        return fromThrift(FbThriftUtils.deserialize(ThriftConnectorSplit.class, bytes));
    }

    public static Object fromThrift(ThriftConnectorSplit thriftSplit)
    {
        requireNonNull(thriftSplit, "ThriftConnectorSplit is null");
        return ThriftSerializationRegistry.deserialize(
                thriftSplit.getType(),
                thriftSplit.getSerializedSplit());
    }
}
