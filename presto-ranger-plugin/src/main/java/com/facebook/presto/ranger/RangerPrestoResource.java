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

import com.facebook.presto.spi.SchemaTableName;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.Optional;

public class RangerPrestoResource
        extends RangerAccessResourceImpl
{
    private String catalogName;

    public static final String KEY_DATABASE = "database";
    public static final String KEY_TABLE = "table";
    public static final String KEY_COLUMN = "column";

    public RangerPrestoResource(String catalogName, Optional<String> schema, Optional<String> table)
    {
        this.catalogName = catalogName;
        schema.ifPresent(s -> setValue(KEY_DATABASE, s));
        table.ifPresent(t -> setValue(KEY_TABLE, t));
    }

    public RangerPrestoResource(String catalogName, Optional<String> schema, Optional<String> table, Optional<String> column)
    {
        this.catalogName = catalogName;
        schema.ifPresent(s -> setValue(KEY_DATABASE, s));
        table.ifPresent(t -> setValue(KEY_TABLE, t));
        column.ifPresent(c -> setValue(KEY_COLUMN, c));
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getTable()
    {
        return getValue(KEY_TABLE);
    }

    public String getDatabase()
    {
        return getValue(KEY_DATABASE);
    }

    public SchemaTableName getSchemaTable()
    {
        return new SchemaTableName(getDatabase(), Optional.ofNullable(getTable()).orElse("*"));
    }
}
