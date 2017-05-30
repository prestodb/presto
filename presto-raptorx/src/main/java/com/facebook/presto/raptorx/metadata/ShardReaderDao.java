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
import java.util.Optional;

public interface ShardReaderDao
{
    @SqlQuery("SELECT sum(row_count)\n" +
            "FROM chunks\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND chunk_id IN (<chunkIds>)\n")
    long getChunkRowCount(
            @Bind long commitId,
            @BindList List<Long> chunkIds);

    @SqlQuery("SELECT *\n" +
            "FROM table_sizes\n" +
            "WHERE start_commit_id <= :commitId\n" +
            "  AND (end_commit_id > :commitId OR end_commit_id IS NULL)\n" +
            "  AND (table_id = :tableId OR :tableId IS NULL)")
    @UseRowMapper(TableSize.Mapper.class)
    List<TableSize> getTableSizes(
            @Bind long commitId,
            @Bind Optional<Long> tableId);
}
