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
package io.prestosql.plugin.thrift.api;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.thrift.api.NameValidationUtils.checkValidName;

@ThriftStruct
public final class PrestoThriftSchemaTableName
{
    private final String schemaName;
    private final String tableName;

    @ThriftConstructor
    public PrestoThriftSchemaTableName(String schemaName, String tableName)
    {
        this.schemaName = checkValidName(schemaName);
        this.tableName = checkValidName(tableName);
    }

    public PrestoThriftSchemaTableName(SchemaTableName schemaTableName)
    {
        this(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    @ThriftField(1)
    public String getSchemaName()
    {
        return schemaName;
    }

    @ThriftField(2)
    public String getTableName()
    {
        return tableName;
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
        PrestoThriftSchemaTableName other = (PrestoThriftSchemaTableName) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .toString();
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(getSchemaName(), getTableName());
    }
}
