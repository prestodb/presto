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
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.raptorx.metadata.ShardHashing.tableShard;
import static com.facebook.presto.raptorx.util.DatabaseUtil.createJdbi;
import static com.facebook.presto.raptorx.util.DatabaseUtil.verifyMetadata;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

public class DatabaseChunkManager
        implements ChunkManager
{
    private final NodeIdCache nodeIdCache;
    private final DistributionDao distributionDao;
    private final List<ChunkManagerDao> shardDao;

    @Inject
    public DatabaseChunkManager(NodeIdCache nodeIdCache, Database database)
    {
        this.nodeIdCache = requireNonNull(nodeIdCache, "nodeIdCache is null");

        this.distributionDao = createJdbi(database.getMasterConnection())
                .onDemand(DistributionDao.class);

        this.shardDao = database.getShards().stream()
                .map(shard -> createJdbi(shard.getConnection()))
                .map(dbi -> dbi.onDemand(ChunkManagerDao.class))
                .collect(toImmutableList());
    }

    @Override
    public Set<ChunkFile> getNodeChunks(String nodeIdentifier)
    {
        long nodeId = nodeIdCache.getNodeId(nodeIdentifier);

        Map<Integer, List<TableBucket>> shards = distributionDao.getTableBuckets(nodeId).stream()
                .collect(groupingBy(bucket -> tableShard(bucket.getTableId(), shardDao.size())));

        ImmutableSet.Builder<ChunkFile> chunks = ImmutableSet.builder();

        shards.forEach((i, shardTables) -> {
            ChunkManagerDao dao = shardDao.get(i);

            Map<Long, List<TableBucket>> tables = shardTables.stream()
                    .collect(groupingBy(TableBucket::getTableId));

            tables.forEach((tableId, buckets) ->
                    chunks.addAll(dao.getChunks(tableId, buckets.stream()
                            .map(TableBucket::getBucketNumber)
                            .collect(toSet()))));
        });

        return chunks.build();
    }

    @Override
    public ChunkFile getChunk(long tableId, long chunkId)
    {
        ChunkManagerDao dao = shardDao.get(tableShard(tableId, shardDao.size()));
        ChunkFile chunk = dao.getChunk(tableId, chunkId);
        verifyMetadata(chunk != null, "Chunk ID (%s) for table ID (%s) does not exist", chunkId, tableId);
        return chunk;
    }
}
