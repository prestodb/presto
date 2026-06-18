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
package com.facebook.presto.kudu;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.kudu.client.KuduTable;

import java.util.List;

public class KuduOutputTableHandle
        extends KuduTableHandle
        implements ConnectorOutputTableHandle, KuduTableMapping
{
    private final boolean generateUUID;
    private final List<Type> columnTypes;
    private final List<Type> originalColumnTypes;

    @JsonCreator
    public KuduOutputTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("originalColumnTypes") List<Type> originalColumnTypes,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("generateUUID") boolean generateUUID)
    {
        this(connectorId, schemaTableName, originalColumnTypes, columnTypes, generateUUID, null);
    }

    public KuduOutputTableHandle(
            String connectorId,
            SchemaTableName schemaTableName,
            List<Type> originalColumnTypes,
            List<Type> columnTypes,
            boolean generateUUID,
            KuduTable table)
    {
        super(connectorId, schemaTableName, table);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
        this.originalColumnTypes = ImmutableList.copyOf(originalColumnTypes);
        this.generateUUID = generateUUID;
    }

    @JsonProperty
    public boolean isGenerateUUID()
    {
        return generateUUID;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @JsonProperty
    public List<Type> getOriginalColumnTypes()
    {
        return originalColumnTypes;
    }
}
