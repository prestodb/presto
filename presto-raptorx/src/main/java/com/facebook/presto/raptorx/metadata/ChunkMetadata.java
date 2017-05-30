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

import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.facebook.presto.raptorx.storage.ColumnStats;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.OptionalLong;

import static com.facebook.presto.raptorx.util.DatabaseUtil.boxedLong;
import static com.facebook.presto.raptorx.util.DatabaseUtil.getOptionalLong;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ChunkMetadata
{
    private final long tableId;
    private final long chunkId;
    private final int bucketNumber;
    private final long rowCount;
    private final long compressedSize;
    private final long uncompressedSize;
    private final long xxhash64;
    private final OptionalLong temporalMin;
    private final OptionalLong temporalMax;

    public ChunkMetadata(
            long tableId,
            long chunkId,
            int bucketNumber,
            long rowCount,
            long compressedSize,
            long uncompressedSize,
            long xxhash64,
            OptionalLong temporalMin,
            OptionalLong temporalMax)
    {
        checkArgument(tableId > 0, "tableId must be > 0");
        checkArgument(chunkId > 0, "chunkId must be > 0");
        checkArgument(rowCount >= 0, "rowCount must be >= 0");
        checkArgument(compressedSize >= 0, "compressedSize must be >= 0");
        checkArgument(uncompressedSize >= 0, "uncompressedSize must be >= 0");

        this.tableId = tableId;
        this.chunkId = chunkId;
        this.bucketNumber = bucketNumber;
        this.rowCount = rowCount;
        this.compressedSize = compressedSize;
        this.uncompressedSize = uncompressedSize;
        this.xxhash64 = xxhash64;
        this.temporalMin = requireNonNull(temporalMin, "temporalMin is null");
        this.temporalMax = requireNonNull(temporalMax, "temporalMax is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public long getChunkId()
    {
        return chunkId;
    }

    public int getBucketNumber()
    {
        return bucketNumber;
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

    public long getXxhash64()
    {
        return xxhash64;
    }

    public OptionalLong getTemporalMin()
    {
        return temporalMin;
    }

    public OptionalLong getTemporalMax()
    {
        return temporalMax;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableId", tableId)
                .add("chunkId", chunkId)
                .add("bucketNumber", bucketNumber)
                .add("rowCount", rowCount)
                .add("compressedSize", succinctBytes(compressedSize))
                .add("uncompressedSize", succinctBytes(uncompressedSize))
                .add("xxhash64", format("%016x", xxhash64))
                .add("temporalMin", boxedLong(temporalMin))
                .add("temporalMax", boxedLong(temporalMax))
                .omitNullValues()
                .toString();
    }

    public static ChunkMetadata from(TableInfo table, ChunkInfo chunk)
    {
        OptionalLong temporalMin = OptionalLong.empty();
        OptionalLong temporalMax = OptionalLong.empty();
        if (table.getTemporalColumnId().isPresent()) {
            for (ColumnStats stats : chunk.getColumnStats()) {
                if (stats.getColumnId() == table.getTemporalColumnId().getAsLong()) {
                    temporalMin = OptionalLong.of((long) stats.getMin());
                    temporalMax = OptionalLong.of((long) stats.getMax());
                    break;
                }
            }
        }

        return new ChunkMetadata(
                table.getTableId(),
                chunk.getChunkId(),
                chunk.getBucketNumber(),
                chunk.getRowCount(),
                chunk.getCompressedSize(),
                chunk.getUncompressedSize(),
                chunk.getXxhash64(),
                temporalMin,
                temporalMax);
    }

    public static class Mapper
            implements RowMapper<ChunkMetadata>
    {
        @Override
        public ChunkMetadata map(ResultSet rs, StatementContext context)
                throws SQLException
        {
            return new ChunkMetadata(
                    rs.getLong("table_id"),
                    rs.getLong("chunk_id"),
                    rs.getInt("bucket_number"),
                    rs.getLong("row_count"),
                    rs.getLong("compressed_size"),
                    rs.getLong("uncompressed_size"),
                    rs.getLong("xxhash64"),
                    getOptionalLong(rs, "temporal_min"),
                    getOptionalLong(rs, "temporal_max"));
        }
    }
}
