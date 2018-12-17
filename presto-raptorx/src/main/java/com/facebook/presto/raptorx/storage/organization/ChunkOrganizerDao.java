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
package com.facebook.presto.raptorx.storage.organization;

import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;

import java.util.Set;

public interface ChunkOrganizerDao
{
    @SqlUpdate("INSERT INTO chunk_organizer_jobs (node_id, table_id, last_start_time)\n" +
            "VALUES (:nodeId, :tableId, NULL)")
    void insertNode(@Bind("nodeId") long nodeId, @Bind("tableId") long tableId);

    @SqlUpdate("UPDATE chunk_organizer_jobs SET last_start_time = :lastStartTime\n" +
            "   WHERE node_id = :nodeId\n" +
            "     AND table_id = :tableId")
    void updateLastStartTime(
            @Bind("nodeId") long nodeId,
            @Bind("tableId") long tableId,
            @Bind("lastStartTime") long lastStartTime);

    @SqlQuery("SELECT table_id, last_start_time\n" +
            "   FROM chunk_organizer_jobs\n" +
            "   WHERE node_id = :nodeId")
    @UseRowMapper(TableOrganizationInfo.Mapper.class)
    Set<TableOrganizationInfo> getNodeTableOrganizationInfo(@Bind("nodeId") long nodeId);
}
