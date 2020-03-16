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

import com.facebook.airlift.log.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.DatabaseShardManager.shardIndexTable;
import static com.facebook.presto.raptor.metadata.ShardsAndIndexDeleter.updatedCount;
import static com.facebook.presto.raptor.util.UuidUtil.uuidToBytes;

public class ShardsAndIndexUpdater
        implements AutoCloseable
{
    private static final Logger log = Logger.get(ShardsAndIndexUpdater.class);

    private final PreparedStatement updateShardEmptyDeltaStatement;
    private final PreparedStatement updateShardStatement;
    private final PreparedStatement updateIndexEmptyDeltaStatement;
    private final PreparedStatement updateIndexStatement;

    public ShardsAndIndexUpdater(Connection connection, long tableId)
            throws SQLException
    {
        // UPDATE table_name
        // SET column1 = value1, column2 = value2, ...
        // WHERE condition;
        String updateIndexSqlEmptyDelta = "" +
                "UPDATE " + shardIndexTable(tableId) + " SET \n" +
                "  delta_shard_uuid = ?\n" +
                "  WHERE shard_id = ? AND delta_shard_uuid IS NULL";
        String updateIndexSql = "" +
                "UPDATE " + shardIndexTable(tableId) + " SET \n" +
                "  delta_shard_uuid = ?\n" +
                "  WHERE shard_id = ? AND delta_shard_uuid = ?";
        String updateShardSqlEmptyDelta = "" +
                "UPDATE shards SET \n" +
                "  delta_uuid = ?\n" +
                "  WHERE shard_id = ? AND delta_uuid IS NULL";
        String updateShardSql = "" +
                "UPDATE shards SET \n" +
                "  delta_uuid = ?\n" +
                "  WHERE shard_id = ? AND delta_uuid = ?";

        this.updateShardEmptyDeltaStatement = connection.prepareStatement(updateShardSqlEmptyDelta);
        this.updateIndexEmptyDeltaStatement = connection.prepareStatement(updateIndexSqlEmptyDelta);
        this.updateShardStatement = connection.prepareStatement(updateShardSql);
        this.updateIndexStatement = connection.prepareStatement(updateIndexSql);
    }

    public void update(long oldId, Optional<UUID> oldUuid, UUID newUuid)
            throws SQLException
    {
        if (oldUuid.isPresent()) {
            updateShardStatement.setBytes(1, uuidToBytes(newUuid));
            updateShardStatement.setLong(2, oldId);
            updateShardStatement.setBytes(3, uuidToBytes(oldUuid.get()));
            updateShardStatement.addBatch();

            updateIndexStatement.setBytes(1, uuidToBytes(newUuid));
            updateIndexStatement.setLong(2, oldId);
            updateIndexStatement.setBytes(3, uuidToBytes(oldUuid.get()));
            updateIndexStatement.addBatch();
        }
        else {
            updateShardEmptyDeltaStatement.setBytes(1, uuidToBytes(newUuid));
            updateShardEmptyDeltaStatement.setLong(2, oldId);
            updateShardEmptyDeltaStatement.addBatch();

            updateIndexEmptyDeltaStatement.setBytes(1, uuidToBytes(newUuid));
            updateIndexEmptyDeltaStatement.setLong(2, oldId);
            updateIndexEmptyDeltaStatement.addBatch();
        }
    }

    public int execute()
            throws SQLException
    {
        int shardsUpdatedCount = 0;
        int indexUpdatedCount = 0;
        shardsUpdatedCount += updatedCount(updateShardStatement.executeBatch());
        shardsUpdatedCount += updatedCount(updateShardEmptyDeltaStatement.executeBatch());
        indexUpdatedCount += updatedCount(updateIndexStatement.executeBatch());
        indexUpdatedCount += updatedCount(updateIndexEmptyDeltaStatement.executeBatch());
        log.info("ShardsAndIndexUpdater shardsUpdatedCount:" + shardsUpdatedCount);
        log.info("ShardsAndIndexUpdater indexUpdatedCount:" + indexUpdatedCount);

        if (shardsUpdatedCount != indexUpdatedCount) {
            throw DatabaseShardManager.transactionConflict();
        }
        return shardsUpdatedCount;
    }

    @Override
    public void close()
            throws SQLException
    {
        updateShardStatement.close();
        updateShardEmptyDeltaStatement.close();
        updateIndexStatement.close();
        updateIndexEmptyDeltaStatement.close();
    }
}
