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
package com.facebook.presto.plugin.clp;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ClpTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;
    private final String tablePath;

    @JsonCreator
    public ClpTableHandle(@JsonProperty("schemaTableName") SchemaTableName schemaTableName, @JsonProperty("tablePath") String tablePath)
    {
        this.schemaTableName = schemaTableName;
        this.tablePath = tablePath;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public String getTablePath()
    {
        return tablePath;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, tablePath);
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
        ClpTableHandle other = (ClpTableHandle) obj;
        return this.schemaTableName.equals(other.schemaTableName) && this.tablePath.equals(other.tablePath);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaTableName", schemaTableName)
                .add("tablePath", tablePath)
                .toString();
    }
}
