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

import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftBaseHiveTableHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import javax.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

public class BaseHiveTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;

    static {
        ThriftSerializationRegistry.registerSerializer(BaseHiveTableHandle.class, BaseHiveTableHandle::serialize);
        ThriftSerializationRegistry.registerDeserializer("BASE_HIVE_HANDLE", BaseHiveTableHandle::deserialize);
    }

    public BaseHiveTableHandle(@NotNull ThriftBaseHiveTableHandle thriftHandle)
    {
        this(thriftHandle.getSchemaName(), thriftHandle.getTableName());
    }

    @Override
    public ThriftConnectorTableHandle toThriftInterface()
    {
        try {
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            ThriftConnectorTableHandle thriftHandle = new ThriftConnectorTableHandle();
            thriftHandle.setType(getImplementationType());
            thriftHandle.setSerializedConnectorTableHandle(serializer.serialize(new ThriftBaseHiveTableHandle(schemaName, tableName)));
            return thriftHandle;
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public BaseHiveTableHandle(String schemaName, String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String getImplementationType()
    {
        return "BASE_HIVE_HANDLE";
    }

    @Override
    public byte[] serialize()
    {
        try {
            ThriftBaseHiveTableHandle thriftHandle = new ThriftBaseHiveTableHandle(schemaName, tableName);

            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            return serializer.serialize(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static BaseHiveTableHandle deserialize(byte[] bytes)
    {
        try {
            ThriftBaseHiveTableHandle thriftHandle = new ThriftBaseHiveTableHandle();
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            deserializer.deserialize(thriftHandle, bytes);
            return new BaseHiveTableHandle(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
