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
package com.facebook.presto.ranger;

import com.facebook.presto.ranger.PrestoAuthorizer.PrestoObjectType;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.Optional;

public class RangerPrestoResource
        extends RangerAccessResourceImpl
{
    public static final String KEY_CATALOG = "catalog";
    public static final String KEY_DATABASE = "database";
    public static final String KEY_TABLE = "table";
    public static final String KEY_COLUMN = "column";

    private PrestoObjectType objectType;

    public RangerPrestoResource(Optional<String> catalog)
    {
        this(PrestoObjectType.CATALOG, catalog, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public RangerPrestoResource(String catalog, Optional<String> database)
    {
        this(PrestoObjectType.DATABASE, Optional.of(catalog), database, Optional.empty(), Optional.empty());
    }

    public RangerPrestoResource(String catalog, String database, Optional<String> table)
    {
        this(PrestoObjectType.TABLE, Optional.of(catalog), Optional.of(database), table, Optional.empty());
    }

    public RangerPrestoResource(String catalog, String database, String table, Optional<String> column)
    {
        this(PrestoObjectType.COLUMN, Optional.of(catalog), Optional.of(database), Optional.of(table), column);
    }

    public RangerPrestoResource(PrestoObjectType objectType, Optional<String> catalog, Optional<String> database, Optional<String> table, Optional<String> column)
    {
        this.objectType = objectType;
        catalog.ifPresent(t -> setValue(KEY_CATALOG, t));
        database.ifPresent(t -> setValue(KEY_DATABASE, t));
        table.ifPresent(t -> setValue(KEY_TABLE, t));
        column.ifPresent(t -> setValue(KEY_COLUMN, t));
    }

    public String getColumn()
    {
        return getValue(KEY_COLUMN);
    }

    public String getTable()
    {
        return getValue(KEY_TABLE);
    }

    public String getDatabase()
    {
        return getValue(KEY_DATABASE);
    }

    public String getCatalog()
    {
        return getValue(KEY_CATALOG);
    }

    public SchemaTableName getSchemaTable()
    {
        return new SchemaTableName(getDatabase(), Optional.ofNullable(getTable()).orElse("*"));
    }

    public PrestoObjectType getObjectType()
    {
        return objectType;
    }
}
