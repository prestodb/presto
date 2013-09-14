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

import java.util.List;

public interface StorageManagerDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS columns (\n" +
            "  shard_id BIGINT NOT NULL,\n" +
            "  column_id BIGINT NOT NULL,\n" +
            "  filename VARCHAR(255) NOT NULL,\n" +
            "  PRIMARY KEY (shard_id, column_id)\n" +
            ")")
    void createTableColumns();

    @SqlUpdate("INSERT INTO columns (shard_id, column_id, filename)\n" +
            "VALUES (:shardId, :columnId, :filename)")
    void insertColumn(
            @Bind("shardId") long shardId,
            @Bind("columnId") long columnId,
            @Bind("filename") String filename);

    @SqlQuery("SELECT filename\n" +
            "FROM columns\n" +
            "WHERE shard_id = :shardId\n" +
            "  AND column_id = :columnId\n")
    String getColumnFilename(
            @Bind("shardId") long shardId,
            @Bind("columnId") long columnId);

    @SqlQuery("SELECT filename\n" +
            "FROM columns\n" +
            "WHERE shard_id = :shardId\n")
    List<String> getShardFiles(@Bind("shardId") long shardId);

    @SqlQuery("SELECT COUNT(*) > 0\n" +
            "FROM columns\n" +
            "WHERE shard_id = :shardId")
    boolean shardExists(@Bind("shardId") long shardId);

    @SqlUpdate("DELETE FROM columns\n" +
            "WHERE shard_id = :shardId")
    void dropShard(@Bind("shardId") long shardId);
}
