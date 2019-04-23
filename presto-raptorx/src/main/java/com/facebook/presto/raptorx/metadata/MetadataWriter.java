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

import com.facebook.presto.raptorx.storage.ChunkInfo;

import java.util.Collection;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Consumer;

public interface MetadataWriter
{
    void recover();

    long beginCommit();

    void finishCommit(long commitId, OptionalLong transactionId);

    void createSchema(long commitId, long schemaId, String schemaName);

    void renameSchema(long commitId, long schemaId, String newSchemaName);

    void dropSchema(long commitId, long schemaId);

    void createTable(long commitId, TableInfo table);

    void renameTable(long commitId, long tableId, long schemaId, String tableName);

    void dropTable(long commitId, long tableId);

    void addColumn(long commitId, long tableId, ColumnInfo column);

    void renameColumn(long commitId, long tableId, long columnId, String columnName);

    void dropColumn(long commitId, long tableId, long columnId);

    void createView(long commitId, ViewInfo view);

    void dropView(long commitId, long viewId);

    void insertChunks(long commitId, long tableId, Collection<ChunkInfo> chunks);

    void deleteChunks(long commitId, long tableId, Set<Long> chunkIds);

    void insertWorkerTransaction(long transactionId, long nodeId);

    void updateWorkerTransaction(boolean success, long transactionId, long nodeId);

    void runWorkerTransaction(long tableId, Consumer<ShardWriterDao> writer);

    void blockMaintenance(long tableId);

    void unBlockMaintenance(long tableId);

    boolean isMaintenanceBlocked(long tableId);

    void clearAllMaintenance();
}
