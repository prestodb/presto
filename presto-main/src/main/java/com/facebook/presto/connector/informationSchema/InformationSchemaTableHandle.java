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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class InformationSchemaTableHandle
        implements TableHandle
{
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    @JsonCreator
    public InformationSchemaTableHandle(@JsonProperty("catalogName") String catalogName, @JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName)
    {
        this.catalogName = checkNotNull(catalogName, "catalogName is null");
        this.schemaName = checkNotNull(schemaName, "schemaName is null");
        this.tableName = checkNotNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
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

    @Override
    public String toString()
    {
        return "information_schema:" + catalogName + ":" + schemaName + ":" + tableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(catalogName, schemaName, tableName);
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
        final InformationSchemaTableHandle other = (InformationSchemaTableHandle) obj;
        return Objects.equal(this.catalogName, other.catalogName) &&
                Objects.equal(this.schemaName, other.schemaName) &&
                Objects.equal(this.tableName, other.tableName);
    }
}
