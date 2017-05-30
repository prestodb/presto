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

public class TableSize
{
    private final long tableId;
    private final long chunkCount;
    private final long compressedSize;
    private final long uncompressedSize;

    public TableSize(long tableId, long chunkCount, long compressedSize, long uncompressedSize)
    {
        checkArgument(tableId > 0, "tableId must be > 0");
        checkArgument(chunkCount >= 0, "chunkCount must be >= 0");
        checkArgument(compressedSize >= 0, "compressedSize must be >= 0");
        checkArgument(uncompressedSize >= 0, "uncompressedSize must be >= 0");

        this.tableId = tableId;
        this.chunkCount = chunkCount;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
    }

    public long getTableId()
    {
        return tableId;
    }

    public long getChunkCount()
    {
        return chunkCount;
    }

    public long getCompressedSize()
    {
        return compressedSize;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("chunkCount", chunkCount)
                .add("compressedSize", compressedSize)
                .add("uncompressedSize", uncompressedSize)
                .toString();
    }

    public static class Mapper
            implements RowMapper<TableSize>
    {
        @Override
        public TableSize map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new TableSize(
                    rs.getLong("table_id"),
                    rs.getLong("chunk_count"),
                    rs.getLong("compressed_size"),
                    rs.getLong("uncompressed_size"));
        }
    }
}
