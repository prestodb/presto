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

import com.facebook.presto.common.experimental.ThriftSerializable;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorTransactionHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftHiveTransactionHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

import java.util.Objects;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class HiveTransactionHandle
        implements ConnectorTransactionHandle, ThriftSerializable
{
    static {
        ThriftSerializationRegistry.registerSerializer(HiveTransactionHandle.class, HiveTransactionHandle::toThrift, null);
        ThriftSerializationRegistry.registerDeserializer(HiveTransactionHandle.class, ThriftHiveTransactionHandle.class, HiveTransactionHandle::deserialize, null);
    }

    private final UUID uuid;

    public HiveTransactionHandle(ThriftHiveTransactionHandle thriftHandle)
    {
        this(new UUID(thriftHandle.getTaskInstanceIdMostSignificantBits(), thriftHandle.getTaskInstanceIdLeastSignificantBits()));
    }

    public HiveTransactionHandle()
    {
        this(UUID.randomUUID());
    }

    @JsonCreator
    public HiveTransactionHandle(@JsonProperty("uuid") UUID uuid)
    {
        this.uuid = requireNonNull(uuid, "uuid is null");
    }

    @JsonProperty
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        HiveTransactionHandle other = (HiveTransactionHandle) obj;
        return Objects.equals(uuid, other.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uuid);
    }

    @Override
    public String toString()
    {
        return uuid.toString();
    }

    @Override
    public ThriftConnectorTransactionHandle toThriftInterface()
    {
        try {
            TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
            ThriftConnectorTransactionHandle thriftHandle = new ThriftConnectorTransactionHandle();
            thriftHandle.setType(getImplementationType());
            thriftHandle.setSerializedConnectorTransactionHandle(serializer.serialize(toThrift()));
            return thriftHandle;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ThriftHiveTransactionHandle toThrift()
    {
        return new ThriftHiveTransactionHandle(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
    }

    public static HiveTransactionHandle deserialize(byte[] bytes)
    {
        try {
            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            ThriftHiveTransactionHandle thriftHandle = new ThriftHiveTransactionHandle();
            deserializer.deserialize(thriftHandle, bytes);
            return new HiveTransactionHandle(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
