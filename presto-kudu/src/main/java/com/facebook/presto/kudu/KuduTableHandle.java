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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kudu.client.KuduTable;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class KuduTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final SchemaTableName schemaTableName;
    private transient KuduTable table;

    @JsonCreator
    public KuduTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
    {
        this(connectorId, schemaTableName, null);
    }

    public KuduTableHandle(
            String connectorId,
            SchemaTableName schemaTableName,
            KuduTable table)
    {
        this.connectorId = requireNonNull(connectorId.toLowerCase(ENGLISH), "connectorId is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.table = table;
    }

    public KuduTable getTable(KuduClientSession session)
    {
        if (table == null) {
            table = session.openTable(schemaTableName);
        }
        return table;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaTableName);
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

        KuduTableHandle other = (KuduTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) && this.schemaTableName
                .equals(other.getSchemaTableName());
    }

    @Override
    public String toString()
    {
        return connectorId + ":" + schemaTableName;
    }
}
