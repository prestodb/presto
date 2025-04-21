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

import com.facebook.presto.common.experimental.FbThriftUtils;
import com.facebook.presto.common.experimental.ThriftSerializationRegistry;
import com.facebook.presto.common.experimental.auto_gen.ThriftBaseHiveTableHandle;
import com.facebook.presto.common.experimental.auto_gen.ThriftConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonProperty;

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

    public BaseHiveTableHandle(ThriftBaseHiveTableHandle thriftHandle)
    {
        this(thriftHandle.getSchemaName(), thriftHandle.getTableName());
    }

    @Override
    public ThriftConnectorTableHandle toThriftInterface()
    {
        return new ThriftConnectorTableHandle(getImplementationType(), FbThriftUtils.serialize(this.toThrift()));
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
        return new BaseHiveTableHandle(FbThriftUtils.deserialize(ThriftBaseHiveTableHandle.class, bytes));
    }
}
