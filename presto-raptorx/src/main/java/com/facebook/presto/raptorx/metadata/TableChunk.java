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
package com.facebook.presto.raptorx.metadata;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;

public class TableChunk
{
    private final long tableId;
    private final long chunkId;
    private final long size;

    public TableChunk(long tableId, long chunkId, long size)
    {
        checkArgument(tableId > 0, "tableId must be > 0");
        checkArgument(chunkId > 0, "chunkId must be > 0");
        checkArgument(size >= 0, "size must be >= 0");

        this.tableId = tableId;
        this.chunkId = chunkId;
        this.size = size;
    }

    public long getTableId()
    {
        return tableId;
    }

    public long getChunkId()
    {
        return chunkId;
    }

    public long getSize()
    {
        return size;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("chunkId", chunkId)
                .add("size", succinctBytes(size))
                .toString();
    }

    public static class Mapper
            implements RowMapper<TableChunk>
    {
        @Override
        public TableChunk map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new TableChunk(
                    rs.getLong("table_id"),
                    rs.getLong("chunk_id"),
                    rs.getLong("size"));
        }
    }
}
