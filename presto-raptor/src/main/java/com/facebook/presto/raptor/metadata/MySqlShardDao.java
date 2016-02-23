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
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.util.UuidUtil.UuidArgumentFactory;
import com.facebook.presto.raptor.util.UuidUtil.UuidMapperFactory;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterArgumentFactory;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapperFactory;

@RegisterArgumentFactory(UuidArgumentFactory.class)
@RegisterMapperFactory(UuidMapperFactory.class)
public interface MySqlShardDao
        extends ShardDao
{
    @Override
    @SqlUpdate("DELETE x\n" +
            "FROM shard_nodes x\n" +
            "JOIN shards USING (shard_id)\n" +
            "WHERE table_id = :tableId")
    void dropShardNodes(@Bind("tableId") long tableId);

    @Override
    @SqlUpdate("DELETE x\n" +
            "FROM created_shards x\n" +
            "JOIN tmp_created_shards USING (shard_uuid)")
    void deleteOldCreatedShards();

    @Override
    @SqlUpdate("DELETE x\n" +
            "FROM created_shard_nodes x\n" +
            "JOIN tmp_created_shard_nodes USING (shard_uuid, node_id)")
    void deleteOldCreatedShardNodes();
}
