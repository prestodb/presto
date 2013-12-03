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

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterArgumentFactory;

import java.util.List;
import java.util.UUID;

import static com.facebook.presto.metadata.UuidArguments.UuidArgumentFactory;

@RegisterArgumentFactory(UuidArgumentFactory.class)
public interface StorageManagerDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS columns (\n" +
            "  shard_uuid BINARY(16) NOT NULL,\n" +
            "  column_id BIGINT NOT NULL,\n" +
            "  filename VARCHAR(255) NOT NULL,\n" +
            "  PRIMARY KEY (shard_uuid, column_id)\n" +
            ")")
    void createTableColumns();

    @SqlUpdate("INSERT INTO columns (shard_uuid, column_id, filename)\n" +
            "VALUES (:shardUuid, :columnId, :filename)")
    void insertColumn(
            @Bind("shardUuid") UUID shardUuid,
            @Bind("columnId") long columnId,
            @Bind("filename") String filename);

    @SqlQuery("SELECT filename\n" +
            "FROM columns\n" +
            "WHERE shard_uuid = :shardUuid\n" +
            "  AND column_id = :columnId")
    String getColumnFilename(
            @Bind("shardUuid") UUID shardUuid,
            @Bind("columnId") long columnId);

    @SqlQuery("SELECT filename\n" +
            "FROM columns\n" +
            "WHERE shard_uuid = :shardUuid")
    List<String> getShardFiles(@Bind("shardUuid") UUID shardUuid);

    @SqlQuery("SELECT COUNT(*) > 0\n" +
            "FROM columns\n" +
            "WHERE shard_uuid = :shardUuid")
    boolean shardExists(@Bind("shardUuid") UUID shardUuid);

    @SqlUpdate("DELETE FROM columns\n" +
            "WHERE shard_uuid = :shardUuid")
    void dropShard(@Bind("shardUuid") UUID shardUuid);
}
