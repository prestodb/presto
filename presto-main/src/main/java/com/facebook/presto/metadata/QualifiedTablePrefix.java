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

import com.facebook.presto.spi.SchemaTablePrefix;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;

@Immutable
public class QualifiedTablePrefix
{
    private final String catalogName;
    private final Optional<String> schemaName;
    private final Optional<String> tableName;

    public QualifiedTablePrefix(String catalogName)
    {
        this.catalogName = checkCatalogName(catalogName);
        this.schemaName = Optional.empty();
        this.tableName = Optional.empty();
    }

    public QualifiedTablePrefix(String catalogName, String schemaName)
    {
        this.catalogName = checkCatalogName(catalogName);
        this.schemaName = Optional.of(checkSchemaName(schemaName));
        this.tableName = Optional.empty();
    }

    public QualifiedTablePrefix(String catalogName, String schemaName, String tableName)
    {
        this.catalogName = checkCatalogName(catalogName);
        this.schemaName = Optional.of(checkSchemaName(schemaName));
        this.tableName = Optional.of(checkTableName(tableName));
    }

    public QualifiedTablePrefix(String catalogName, Optional<String> schemaName, Optional<String> tableName)
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

    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    public Optional<String> getTableName()
    {
        return tableName;
    }

    public boolean hasSchemaName()
    {
        return schemaName.isPresent();
    }

    public boolean hasTableName()
    {
        return tableName.isPresent();
    }

    public SchemaTablePrefix asSchemaTablePrefix()
    {
        if (!schemaName.isPresent()) {
            return new SchemaTablePrefix();
        }
        else if (!tableName.isPresent()) {
            return new SchemaTablePrefix(schemaName.get());
        }
        else {
            return new SchemaTablePrefix(schemaName.get(), tableName.get());
        }
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
        QualifiedTablePrefix o = (QualifiedTablePrefix) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(schemaName, o.schemaName) &&
                Objects.equals(tableName, o.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return catalogName + '.' + schemaName.orElse("*") + '.' + tableName.orElse("*");
    }
}
