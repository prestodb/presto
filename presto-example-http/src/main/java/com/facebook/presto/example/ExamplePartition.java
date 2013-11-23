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
package com.facebook.presto.example;

import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class ExamplePartition
        implements Partition
{
    private final String schemaName;
    private final String tableName;

    public ExamplePartition(String schemaName, String tableName)
    {
        this.schemaName = checkNotNull(schemaName, "schema name is null");
        this.tableName = checkNotNull(tableName, "table name is null");
    }

    @Override
    public String getPartitionId()
    {
        return schemaName + ":" + tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    @Override
    public TupleDomain getTupleDomain()
    {
        return TupleDomain.all();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .toString();
    }
}
