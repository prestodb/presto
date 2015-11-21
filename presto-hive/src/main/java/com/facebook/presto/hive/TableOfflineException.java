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
package com.facebook.presto.hive;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_TABLE_OFFLINE;
import static java.util.Objects.requireNonNull;

public class TableOfflineException
        extends PrestoException
{
    private final SchemaTableName tableName;

    public TableOfflineException(SchemaTableName tableName)
    {
        this(tableName, String.format("Table '%s' is offline", tableName));
    }

    public TableOfflineException(SchemaTableName tableName, String message)
    {
        super(HIVE_TABLE_OFFLINE, message);
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }
}
