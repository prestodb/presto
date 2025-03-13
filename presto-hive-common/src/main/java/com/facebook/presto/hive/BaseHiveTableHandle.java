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
import org.apache.thrift.protocol.TJSONProtocol;

import javax.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

public class BaseHiveTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;

    static {
        ThriftSerializationRegistry.registerSerializer(BaseHiveTableHandle.class, BaseHiveTableHandle::toThrift, null);
        ThriftSerializationRegistry.registerDeserializer(BaseHiveTableHandle.class, ThriftBaseHiveTableHandle.class, BaseHiveTableHandle::deserialize, null);
    }

    public BaseHiveTableHandle(@NotNull ThriftBaseHiveTableHandle thriftHandle)
    {
        this(thriftHandle.getSchemaName(), thriftHandle.getTableName());
    }

    @Override
    public ThriftConnectorTableHandle toThriftInterface()
    {
        ThriftConnectorTableHandle thriftHandle = new ThriftConnectorTableHandle();
        thriftHandle.setType(this.getClass().getName());
        try {
            TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
            thriftHandle.setSerializedConnectorTableHandle(serializer.serialize(this.toThrift()));
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
        return thriftHandle;
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

    public static BaseHiveTableHandle deserialize(byte[] bytes)
    {
        try {
            ThriftBaseHiveTableHandle thriftHandle = new ThriftBaseHiveTableHandle();
            TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
            deserializer.deserialize(thriftHandle, bytes);
            return new BaseHiveTableHandle(thriftHandle);
        }
        catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}
