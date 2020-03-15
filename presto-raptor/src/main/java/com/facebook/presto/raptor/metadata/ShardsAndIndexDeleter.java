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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.util.UuidUtil.uuidToBytes;

public class ShardsAndIndexDeleter
        implements AutoCloseable
{
    private final PreparedStatement deleteShardStatement;
    private final PreparedStatement deleteShardEmptyDeltaStatement;
    private final PreparedStatement deleteIndexStatement;
    private final PreparedStatement deleteIndexEmptyDeltaStatement;

    public ShardsAndIndexDeleter(Connection connection, long tableId)
            throws SQLException
    {
        // DELETE FROM table_name
        // WHERE condition;
        String deleteIndexSql = "" +
                "DELETE FROM " + shardIndexTable(tableId) + " \n" +
                "  WHERE shard_id = ? AND delta_shard_uuid = ?";
        String deleteIndexSqlEmptyDelta = "" +
                "DELETE FROM " + shardIndexTable(tableId) + " \n" +
                "  WHERE shard_id = ? AND delta_shard_uuid IS NULL";
        String deleteShardSql = "" +
                "DELETE FROM shards \n" +
                "  WHERE shard_id = ? AND delta_uuid = ?";
        String deleteShardSqlEmptyDelta = "" +
                "DELETE FROM shards \n" +
                "  WHERE shard_id = ? AND delta_uuid IS NULL";
        this.deleteIndexStatement = connection.prepareStatement(deleteIndexSql);
        this.deleteIndexEmptyDeltaStatement = connection.prepareStatement(deleteIndexSqlEmptyDelta);
        this.deleteShardStatement = connection.prepareStatement(deleteShardSql);
        this.deleteShardEmptyDeltaStatement = connection.prepareStatement(deleteShardSqlEmptyDelta);
    }

    public void delete(Long id, Optional<UUID> deltaUuid)
            throws SQLException
    {
        if (deltaUuid.isPresent()) {
            deleteShardStatement.setLong(1, id);
            deleteShardStatement.setBytes(2, uuidToBytes(deltaUuid.get()));
            deleteShardStatement.addBatch();

            deleteIndexStatement.setLong(1, id);
            deleteIndexStatement.setBytes(2, uuidToBytes(deltaUuid.get()));
            deleteIndexStatement.addBatch();
        }
        else {
            deleteShardEmptyDeltaStatement.setLong(1, id);
            deleteShardEmptyDeltaStatement.addBatch();
            deleteIndexEmptyDeltaStatement.setLong(1, id);
            deleteIndexEmptyDeltaStatement.addBatch();
        }
    }

    public int execute()
            throws SQLException
    {
        int shardsUpdatedCount = 0;
        int indexUpdatedCount = 0;
        shardsUpdatedCount += updatedCount(deleteShardStatement.executeBatch());
        shardsUpdatedCount += updatedCount(deleteShardEmptyDeltaStatement.executeBatch());
        indexUpdatedCount += updatedCount(deleteIndexStatement.executeBatch());
        indexUpdatedCount += updatedCount(deleteIndexEmptyDeltaStatement.executeBatch());

        if (shardsUpdatedCount != indexUpdatedCount) {
            throw DatabaseShardManager.transactionConflict();
        }
        return shardsUpdatedCount;
    }

    @Override
    public void close()
            throws SQLException
    {
        deleteShardStatement.close();
        deleteShardEmptyDeltaStatement.close();
        deleteIndexStatement.close();
        deleteIndexEmptyDeltaStatement.close();
    }

    static int updatedCount(int[] executeBatch)
    {
        return Arrays.stream(executeBatch).sum();
    }
}
