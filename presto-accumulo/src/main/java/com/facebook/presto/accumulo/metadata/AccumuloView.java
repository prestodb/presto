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
package com.facebook.presto.accumulo.metadata;

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * This class encapsulates metadata regarding an Accumulo view in Presto. It is a jackson
 * serializable object.
 */
public class AccumuloView
{
    private final String schema;
    private final String table;
    private final String data;
    private final SchemaTableName schemaTableName;

    /**
     * Creates a new instance of {@link AccumuloView}
     *
     * @param schema Schema name of the view
     * @param table Table name of the view
     * @param data View data
     */
    @JsonCreator
    public AccumuloView(@JsonProperty("schema") String schema, @JsonProperty("table") String table, @JsonProperty("data") String data)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.data = requireNonNull(data, "data is null");
        this.schemaTableName = new SchemaTableName(schema, table);
    }

    /**
     * Gets the schema name of the view
     *
     * @return Schema name
     */
    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    /**
     * Gets the table name of the view
     *
     * @return Table name
     */
    @JsonProperty
    public String getTable()
    {
        return table;
    }

    /**
     * Gets the view data
     *
     * @return View data
     */
    @JsonProperty
    public String getData()
    {
        return data;
    }

    /**
     * Gets the table name as a {@link SchemaTableName}
     *
     * @return Schema table name
     */
    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, table, data);
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

        AccumuloView other = (AccumuloView) obj;
        return Objects.equals(this.schema, other.schema)
                && Objects.equals(this.table, other.table)
                && Objects.equals(this.data, other.data);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("schema", schema)
                .add("table", table).add("data", data).toString();
    }
}
