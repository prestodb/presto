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
import static java.lang.String.format;

public class ChunkFile
{
    private final long chunkId;
    private final long size;
    private final long xxhash64;

    public ChunkFile(long chunkId, long size, long xxhash64)
    {
        checkArgument(chunkId > 0, "chunkId must be > 0");
        checkArgument(size >= 0, "size must be >= 0");

        this.chunkId = chunkId;
        this.size = size;
        this.xxhash64 = xxhash64;
    }

    public long getChunkId()
    {
        return chunkId;
    }

    public long getSize()
    {
        return size;
    }

    public long getXxhash64()
    {
        return xxhash64;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("chunkId", chunkId)
                .add("size", succinctBytes(size))
                .add("xxhash64", format("%016x", xxhash64))
                .toString();
    }

    public static class Mapper
            implements RowMapper<ChunkFile>
    {
        @Override
        public ChunkFile map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new ChunkFile(
                    rs.getLong("chunk_id"),
                    rs.getLong("compressed_size"),
                    rs.getLong("xxhash64"));
        }
    }
}
