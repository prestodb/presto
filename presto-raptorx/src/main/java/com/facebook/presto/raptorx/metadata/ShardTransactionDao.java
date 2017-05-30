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
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface ShardTransactionDao
{
    @SqlUpdate("INSERT INTO created_chunks (chunk_id, table_id, transaction_id, size, create_time)\n" +
            "VALUES (:chunkId, :tableId, :transactionId, :size, :createTime)")
    void insertCreatedChunk(
            @Bind long chunkId,
            @Bind long tableId,
            @Bind long transactionId,
            @Bind long size,
            @Bind long createTime);
}
