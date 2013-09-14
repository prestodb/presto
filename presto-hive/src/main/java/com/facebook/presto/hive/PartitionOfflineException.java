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

import com.facebook.presto.spi.SchemaTableName;

import static com.google.common.base.Preconditions.checkNotNull;

public class PartitionOfflineException
        extends RuntimeException
{
    private final SchemaTableName tableName;
    private final String partition;

    public PartitionOfflineException(SchemaTableName tableName, String partition)
    {
        this(tableName, partition, String.format("Table '%s' partition '%s' is offline", tableName, partition));
    }

    public PartitionOfflineException(SchemaTableName tableName,
            String partition,
            String message)
    {
        super(message);
        if (tableName == null) {
            throw new NullPointerException("tableName is null");
        }
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partition = checkNotNull(partition, "partition is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public String getPartition()
    {
        return partition;
    }
}
