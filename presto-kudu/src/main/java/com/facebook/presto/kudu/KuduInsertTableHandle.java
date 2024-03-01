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
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.kudu.client.KuduTable;

import java.util.List;

public class KuduInsertTableHandle
        extends KuduTableHandle
        implements ConnectorInsertTableHandle, KuduTableMapping
{
    private final List<Type> columnTypes;

    @JsonCreator
    public KuduInsertTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("columnTypes") List<Type> columnTypes)
    {
        this(connectorId, schemaTableName, columnTypes, null);
    }

    public KuduInsertTableHandle(
            String connectorId,
            SchemaTableName schemaTableName,
            List<Type> columnTypes,
            KuduTable table)
    {
        super(connectorId, schemaTableName, table);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    public List<Type> getOriginalColumnTypes()
    {
        return columnTypes;
    }

    public boolean isGenerateUUID()
    {
        return false;
    }
}
