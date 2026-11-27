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
package com.facebook.presto.ducklake.catalog;

import static java.util.Objects.requireNonNull;

/**
 * A row of {@code ducklake_inlined_data_tables}, naming a physical catalog-database table (for
 * example {@code ducklake_inlined_data_21_21}) that holds rows inlined directly in the catalog
 * database instead of written out to a Parquet data file.
 */
public class DuckLakeInlinedTable
{
    private final String tableName;
    private final long schemaVersion;

    public DuckLakeInlinedTable(String tableName, long schemaVersion)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.schemaVersion = schemaVersion;
    }

    public String getTableName()
    {
        return tableName;
    }

    public long getSchemaVersion()
    {
        return schemaVersion;
    }
}
