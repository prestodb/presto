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
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterArgumentFactory;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapperFactory;

import java.sql.Timestamp;
import java.util.UUID;

@RegisterArgumentFactory(UuidArgumentFactory.class)
@RegisterMapperFactory(UuidMapperFactory.class)
public interface PostgreSqlShardDao
        extends ShardDao
{
    @Override
    @SqlUpdate("DELETE FROM shard_nodes WHERE shard_id IN\n" +
            "  (SELECT shard_id FROM shards WHERE table_id = :tableId)")
    void dropShardNodes(@Bind("tableId") long tableId);

    @Override
    @SqlBatch("INSERT INTO deleted_shards (shard_uuid, delete_time)\n" +
            "VALUES (:shardUuid, CURRENT_TIMESTAMP)\n" +
            "ON CONFLICT DO NOTHING")
    void insertDeletedShards(@Bind("shardUuid") Iterable<UUID> shardUuids);

    @SqlUpdate("DELETE FROM transactions WHERE transaction_id IN (\n" +
            "  SELECT transaction_id FROM transactions\n" +
            "  WHERE end_time < :maxEndTime\n" +
            "  AND successful IN (TRUE, FALSE)\n" +
            "  AND transaction_id NOT IN (SELECT transaction_id FROM created_shards)\n" +
            "  ORDER BY end_time, transaction_id\n" +
            "  LIMIT " + CLEANUP_TRANSACTIONS_BATCH_SIZE + ")")
    int deleteOldCompletedTransactions(@Bind("maxEndTime") Timestamp maxEndTime);
}
