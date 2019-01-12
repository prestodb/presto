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
package io.prestosql.plugin.atop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorTableHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class AtopTableHandle
        implements ConnectorTableHandle
{
    private final String schema;
    private final AtopTable table;

    @JsonCreator
    public AtopTableHandle(@JsonProperty("schema") String schema, @JsonProperty("table") AtopTable table)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public AtopTable getTable()
    {
        return table;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, table);
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
        AtopTableHandle other = (AtopTableHandle) obj;
        return Objects.equals(this.schema, other.schema) &&
                Objects.equals(this.table, other.table);
    }

    @Override
    public String toString()
    {
        return schema + ":" + table;
    }
}
