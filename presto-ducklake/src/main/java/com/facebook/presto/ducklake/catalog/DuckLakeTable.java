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
 * A visible-at-snapshot row of {@code ducklake_table}. {@code path} has already been resolved by
 * the JDBC layer to an absolute location, joining a relative {@code path} with the schema's (or,
 * transitively, the {@code data_path} global {@code ducklake_metadata} entry's) location when
 * {@code path_is_relative} is set.
 */
public class DuckLakeTable
{
    private final long tableId;
    private final long schemaId;
    private final String tableName;
    private final String path;

    public DuckLakeTable(long tableId, long schemaId, String tableName, String path)
    {
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.path = requireNonNull(path, "path is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public long getSchemaId()
    {
        return schemaId;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getPath()
    {
        return path;
    }
}
