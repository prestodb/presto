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
package com.facebook.presto.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TableAlias
{
    private final String sourceConnectorId;
    private final String sourceSchemaName;
    private final String sourceTableName;
    private final String destinationConnectorId;
    private final String destinationSchemaName;
    private final String destinationTableName;

    @JsonCreator
    public TableAlias(@JsonProperty("sourceConnectorId") String sourceConnectorId,
            @JsonProperty("sourceSchemaName") String sourceSchemaName,
            @JsonProperty("sourceTableName") String sourceTableName,
            @JsonProperty("destinationConnectorId") String destinationConnectorId,
            @JsonProperty("destinationSchemaName") String destinationSchemaName,
            @JsonProperty("destinationTableName") String destinationTableName)
    {
        this.sourceConnectorId = checkNotNull(sourceConnectorId, "sourceConnectorId is null");
        this.sourceSchemaName = checkNotNull(sourceSchemaName, "sourceSchemaName is null");
        this.sourceTableName = checkNotNull(sourceTableName, "sourceTableName is null");
        this.destinationConnectorId = checkNotNull(destinationConnectorId, "destinationConnectorId is null");
        this.destinationSchemaName = checkNotNull(destinationSchemaName, "destinationSchemaName is null");
        this.destinationTableName = checkNotNull(destinationTableName, "destinationTableName is null");
    }

    @JsonProperty
    public String getSourceConnectorId()
    {
        return sourceConnectorId;
    }

    @JsonProperty
    public String getSourceSchemaName()
    {
        return sourceSchemaName;
    }

    @JsonProperty
    public String getSourceTableName()
    {
        return sourceTableName;
    }

    @JsonProperty
    public String getDestinationConnectorId()
    {
        return destinationConnectorId;
    }

    @JsonProperty
    public String getDestinationSchemaName()
    {
        return destinationSchemaName;
    }

    @JsonProperty
    public String getDestinationTableName()
    {
        return destinationTableName;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("sourceConnectorId", sourceConnectorId)
                .add("sourceSchemaName", sourceSchemaName)
                .add("sourceTableName", sourceTableName)
                .add("destinationConnectorId", destinationConnectorId)
                .add("destinationSchemaName", destinationSchemaName)
                .add("destinationTableName", destinationTableName)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sourceConnectorId,
                sourceSchemaName,
                sourceTableName,
                destinationConnectorId,
                destinationSchemaName,
                destinationTableName);
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
        final TableAlias other = (TableAlias) obj;
        return Objects.equal(this.sourceConnectorId, other.sourceConnectorId) &&
                Objects.equal(this.sourceSchemaName, other.sourceSchemaName) &&
                Objects.equal(this.sourceTableName, other.sourceTableName) &&
                Objects.equal(this.destinationConnectorId, other.destinationConnectorId) &&
                Objects.equal(this.destinationSchemaName, other.destinationSchemaName) &&
                Objects.equal(this.destinationTableName, other.destinationTableName);
    }

    public static class TableAliasMapper
            implements ResultSetMapper<TableAlias>
    {
        @Override
        public TableAlias map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            String sourceConnectorId = r.getString("source_connector_id");
            String sourceSchemaName = r.getString("source_schema_name");
            String sourceTableName = r.getString("source_table_name");

            String destinationConnectorId = r.getString("destination_connector_id");
            String destinationSchemaName = r.getString("destination_schema_name");
            String destinationTableName = r.getString("destination_table_name");

            return new TableAlias(
                    sourceConnectorId,
                    sourceSchemaName,
                    sourceTableName,
                    destinationConnectorId,
                    destinationSchemaName,
                    destinationTableName);
        }
    }
}
