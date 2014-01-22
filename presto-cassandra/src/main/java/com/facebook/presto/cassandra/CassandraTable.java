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
package com.facebook.presto.cassandra;

import com.facebook.presto.cassandra.util.CassandraCqlUtils;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class CassandraTable
{
    private final CassandraTableHandle tableHandle;
    private final List<CassandraColumnHandle> columns;
    private final int partitionKeyColumns;

    public CassandraTable(CassandraTableHandle tableHandle, List<CassandraColumnHandle> columns)
    {
        this.tableHandle = tableHandle;
        this.columns = ImmutableList.copyOf(columns);
        int count = 0;
        while (count < columns.size() && columns.get(count).isPartitionKey()) {
            count++;
        }
        partitionKeyColumns = count;
    }

    public List<CassandraColumnHandle> getColumns()
    {
        return columns;
    }

    public CassandraTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public List<CassandraColumnHandle> getPartitionKeyColumns()
    {
        return columns.subList(0, partitionKeyColumns);
    }

    public String getTokenExpression()
    {
        StringBuilder sb = new StringBuilder();
        for (CassandraColumnHandle column : getPartitionKeyColumns()) {
            if (sb.length() == 0) {
                sb.append("token(");
            }
            else {
                sb.append(",");
            }
            sb.append(CassandraCqlUtils.validColumnName(column.getName()));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return tableHandle.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CassandraTable)) {
            return false;
        }
        CassandraTable that = (CassandraTable) obj;
        return this.tableHandle.equals(that.tableHandle);
    }
}
