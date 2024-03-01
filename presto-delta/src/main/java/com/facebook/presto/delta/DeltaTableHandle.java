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
package com.facebook.presto.delta;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class DeltaTableHandle
        implements ConnectorTableHandle
{
    private final String connectorId;
    private final DeltaTable deltaTable;

    @JsonCreator
    public DeltaTableHandle(@JsonProperty("connectorId") String connectorId, @JsonProperty("deltaTable") DeltaTable deltaTable)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.deltaTable = requireNonNull(deltaTable, "deltaTable is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public DeltaTable getDeltaTable()
    {
        return deltaTable;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(deltaTable.getSchemaName(), deltaTable.getTableName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, deltaTable);
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

        DeltaTableHandle other = (DeltaTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.deltaTable, other.deltaTable);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("table", toSchemaTableName())
                .toString();
    }
}
