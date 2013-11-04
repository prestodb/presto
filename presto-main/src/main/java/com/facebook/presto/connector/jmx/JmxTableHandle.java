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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.connector.jmx.JmxColumnHandle.columnMetadataGetter;
import static com.google.common.collect.Iterables.transform;

public class JmxTableHandle
        implements TableHandle
{
    private final String connectorId;
    private final String objectName;
    private final List<JmxColumnHandle> columns;

    @JsonCreator
    public JmxTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("objectName") String objectName,
            @JsonProperty("columns") List<JmxColumnHandle> columns)
    {
        this.connectorId = connectorId;
        this.objectName = objectName;
        this.columns = columns;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getObjectName()
    {
        return objectName;
    }

    @JsonProperty
    public List<JmxColumnHandle> getColumns()
    {
        return columns;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(connectorId, objectName, columns);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final JmxTableHandle other = (JmxTableHandle) obj;
        return Objects.equal(this.connectorId, other.connectorId) && Objects.equal(this.objectName, other.objectName) && Objects.equal(this.columns, other.columns);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("connectorId", connectorId)
                .add("objectName", objectName)
                .add("columns", columns)
                .toString();
    }

    public ConnectorTableMetadata getTableMetadata()
    {
        return new ConnectorTableMetadata(new SchemaTableName(JmxMetadata.SCHEMA_NAME, objectName), ImmutableList.copyOf(transform(columns, columnMetadataGetter())));
    }
}

