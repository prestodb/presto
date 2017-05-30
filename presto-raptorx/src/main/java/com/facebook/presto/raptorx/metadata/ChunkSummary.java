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

public class ChunkSummary
{
    private final long chunkCount;
    private final long rowCount;
    private final long compressedSize;
    private final long uncompressedSize;

    public ChunkSummary(long chunkCount, long rowCount, long compressedSize, long uncompressedSize)
    {
        this.chunkCount = chunkCount;
        this.rowCount = rowCount;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
    }

    public long getChunkCount()
    {
        return chunkCount;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getCompressedSize()
    {
        return compressedSize;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    public static class Mapper
            implements RowMapper<ChunkSummary>
    {
        @Override
        public ChunkSummary map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new ChunkSummary(
                    rs.getLong("chunk_count"),
                    rs.getLong("row_count"),
                    rs.getLong("compressed_size"),
                    rs.getLong("uncompressed_size"));
        }
    }
}
