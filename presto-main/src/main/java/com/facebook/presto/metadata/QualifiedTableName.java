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

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class QualifiedTableName
{
    @JsonCreator
    public static QualifiedTableName valueOf(String tableName)
    {
        checkNotNull(tableName, "tableName is null");

        ImmutableList<String> ids = ImmutableList.copyOf(Splitter.on('.').split(tableName));
        checkArgument(ids.size() == 3, "Invalid tableName %s", tableName);

        return new QualifiedTableName(ids.get(0), ids.get(1), ids.get(2));
    }

    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    public QualifiedTableName(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public SchemaTableName asSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QualifiedTableName o = (QualifiedTableName) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(tableName, o.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, tableName);
    }

    @JsonValue
    @Override
    public String toString()
    {
        return catalogName + '.' + schemaName + '.' + tableName;
    }

    public static Function<SchemaTableName, QualifiedTableName> convertFromSchemaTableName(final String catalogName)
    {
        return input -> new QualifiedTableName(catalogName, input.getSchemaName(), input.getTableName());
    }
}
