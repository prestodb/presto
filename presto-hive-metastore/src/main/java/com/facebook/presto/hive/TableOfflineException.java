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

import com.facebook.presto.common.PrestoException;
import com.facebook.presto.common.SchemaTableName;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_TABLE_OFFLINE;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class TableOfflineException
        extends PrestoException
{
    private final SchemaTableName tableName;

    public TableOfflineException(SchemaTableName tableName, boolean forPresto, String offlineMessage)
    {
        super(HIVE_TABLE_OFFLINE, formatMessage(tableName, forPresto, offlineMessage));
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    private static String formatMessage(SchemaTableName tableName, boolean forPresto, String offlineMessage)
    {
        StringBuilder resultBuilder = new StringBuilder()
                .append("Table '").append(tableName).append("'")
                .append(" is offline");
        if (forPresto) {
            resultBuilder.append(" for Presto");
        }
        if (!isNullOrEmpty(offlineMessage)) {
            resultBuilder.append(": ").append(offlineMessage);
        }
        return resultBuilder.toString();
    }
}
