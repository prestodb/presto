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

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindList;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.util.List;
import java.util.Set;

public interface ChunkManagerDao
{
    @SqlQuery("SELECT chunk_id, compressed_size, xxhash64\n" +
            "FROM chunks\n" +
            "WHERE table_id = :tableId\n" +
            "  AND bucket_number IN (<bucketNumbers>)")
    @UseRowMapper(ChunkFile.Mapper.class)
    List<ChunkFile> getChunks(
            @Bind long tableId,
            @BindList Set<Integer> bucketNumbers);

    @SqlQuery("SELECT chunk_id, compressed_size, xxhash64\n" +
            "FROM chunks\n" +
            "WHERE table_id = :tableId\n" +
            "  AND chunk_id = :chunkId ")
    @UseRowMapper(ChunkFile.Mapper.class)
    ChunkFile getChunk(
            @Bind long tableId,
            @Bind long chunkId);
}
