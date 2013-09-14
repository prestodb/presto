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
package com.facebook.presto.metadata;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public class TablePartition
{
    private final long partitionId;
    private final String partitionName;
    private final long tableId;

    TablePartition(long partitionId,
            String partitionName,
            long tableId)
    {
        this.partitionId = partitionId;
        this.partitionName = checkNotNull(partitionName, "partitionName is null");
        this.tableId = tableId;
    }

    public long getPartitionId()
    {
        return partitionId;
    }

    public String getPartitionName()
    {
        return partitionName;
    }

    public long getTableId()
    {
        return tableId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(partitionId,
                partitionName,
                tableId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final TablePartition other = (TablePartition) obj;
        return Objects.equal(this.partitionId, other.partitionId) &&
                Objects.equal(this.partitionName, other.partitionName) &&
                Objects.equal(this.tableId, other.tableId);
    }

    public static Function<TablePartition, String> partitionNameGetter()
    {
        return new Function<TablePartition, String>()
        {
            @Override
            public String apply(TablePartition partition)
            {
                return partition.getPartitionName();
            }
        };
    }

    public static Function<TablePartition, Long> partitionIdGetter()
    {
        return new Function<TablePartition, Long>()
        {
            @Override
            public Long apply(TablePartition partition)
            {
                return partition.getPartitionId();
            }
        };
    }

    public static class Mapper
            implements ResultSetMapper<TablePartition>
    {
        @Override
        public TablePartition map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new TablePartition(r.getLong("partition_id"),
                    r.getString("partition_name"),
                    r.getLong("table_id"));
        }
    }
}
