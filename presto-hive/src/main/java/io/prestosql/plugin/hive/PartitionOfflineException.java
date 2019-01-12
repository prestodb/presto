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
package io.prestosql.plugin.hive;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_PARTITION_OFFLINE;
import static java.util.Objects.requireNonNull;

public class PartitionOfflineException
        extends PrestoException
{
    private final SchemaTableName tableName;
    private final String partition;

    public PartitionOfflineException(SchemaTableName tableName, String partitionName, boolean forPresto, String offlineMessage)
    {
        super(HIVE_PARTITION_OFFLINE, formatMessage(tableName, partitionName, forPresto, offlineMessage));
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.partition = requireNonNull(partitionName, "partition is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public String getPartition()
    {
        return partition;
    }

    private static String formatMessage(SchemaTableName tableName, String partitionName, boolean forPresto, String offlineMessage)
    {
        StringBuilder resultBuilder = new StringBuilder()
                .append("Table '").append(tableName).append("'")
                .append(" partition '").append(partitionName).append("'")
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
