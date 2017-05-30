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

import com.facebook.presto.raptorx.util.Database;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class DatabaseChunkRecorder
        implements ChunkRecorder
{
    private final List<ShardTransactionDao> shardDao;

    @Inject
    public DatabaseChunkRecorder(Database database)
    {
        this.shardDao = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .map(dbi -> dbi.onDemand(ShardTransactionDao.class))
                .collect(toImmutableList());
    }

    @Override
    public void recordCreatedChunk(long transactionId, long tableId, long chunkId, long size)
    {
        shardDao.get(ThreadLocalRandom.current().nextInt(shardDao.size()))
                .insertCreatedChunk(chunkId, tableId, transactionId, size, System.currentTimeMillis());
    }
}
