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
package io.prestosql.plugin.raptor.legacy.storage.organization;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.Set;

public interface ShardOrganizerDao
{
    @SqlUpdate("INSERT INTO shard_organizer_jobs (node_identifier, table_id, last_start_time)\n" +
            "VALUES (:nodeIdentifier, :tableId, NULL)")
    void insertNode(@Bind("nodeIdentifier") String nodeIdentifier, @Bind("tableId") long tableId);

    @SqlUpdate("UPDATE shard_organizer_jobs SET last_start_time = :lastStartTime\n" +
            "   WHERE node_identifier = :nodeIdentifier\n" +
            "     AND table_id = :tableId")
    void updateLastStartTime(
            @Bind("nodeIdentifier") String nodeIdentifier,
            @Bind("tableId") long tableId,
            @Bind("lastStartTime") long lastStartTime);

    @SqlQuery("SELECT table_id, last_start_time\n" +
            "   FROM shard_organizer_jobs\n" +
            "   WHERE node_identifier = :nodeIdentifier")
    @Mapper(TableOrganizationInfo.Mapper.class)
    Set<TableOrganizationInfo> getNodeTableOrganizationInfo(@Bind("nodeIdentifier") String nodeIdentifier);

    @SqlUpdate("DELETE FROM shard_organizer_jobs WHERE table_id = :tableId")
    void dropOrganizerJobs(@Bind("tableId") long tableId);
}
