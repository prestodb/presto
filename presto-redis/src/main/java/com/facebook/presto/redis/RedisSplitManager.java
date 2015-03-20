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
package com.facebook.presto.redis;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import redis.clients.jedis.Jedis;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Redis specific implementation of {@link ConnectorSplitManager}.
 */
public class RedisSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(RedisSplitManager.class);

    private final String connectorId;
    private final RedisConnectorConfig redisConnectorConfig;
    private final RedisHandleResolver handleResolver;
    private final RedisJedisManager jedisManager;

    private static final long REDIS_MAX_SPLITS = 100;
    private static final long REDIS_STRIDE_SPLITS = 100;

    @Inject
    public RedisSplitManager(@Named("connectorId") String connectorId,
                             RedisConnectorConfig redisConnectorConfig,
                             RedisHandleResolver handleResolver,
                             RedisJedisManager jedisManager)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.redisConnectorConfig = checkNotNull(redisConnectorConfig, "redisConfig is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.jedisManager = checkNotNull(jedisManager, "jedisManager is null");
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
    {
        RedisTableHandle redisTableHandle = handleResolver.convertTableHandle(tableHandle);

        List<HostAddress> nodes = new ArrayList<>(redisConnectorConfig.getNodes());
        Collections.shuffle(nodes);
        // redis connector has only one partition
        List<ConnectorPartition> partitions = ImmutableList.<ConnectorPartition>of(
                new RedisPartition(redisTableHandle.getSchemaName(),
                        redisTableHandle.getTableName(),
                        nodes));
        // redis connector does not do any additional processing/filtering with the TupleDomain, so just return the whole TupleDomain
        return new ConnectorPartitionResult(partitions, tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> partitions)
    {
        RedisTableHandle redisTableHandle = handleResolver.convertTableHandle(tableHandle);
        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();

        for (ConnectorPartition cp : partitions) {
            checkState(cp instanceof RedisPartition, "Found an unknown partition type: %s", cp.getClass().getSimpleName());
            RedisPartition partition = (RedisPartition) cp;

            long numberOfKeys = 1;
            // when Redis keys are provides in a zset, create multiple
            // splits by splitting zset in chunks
            if (redisTableHandle.getKeyDataFormat().equals("zset")) {
                try (Jedis jedis = jedisManager.getJedisPool(partition.getPartitionNodes().get(0)).getResource()) {
                    numberOfKeys = jedis.zcount(redisTableHandle.getKeyName(), "-inf", "+inf");
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }

            long stride = REDIS_STRIDE_SPLITS;

            if (numberOfKeys / stride > REDIS_MAX_SPLITS) {
                stride = numberOfKeys / REDIS_MAX_SPLITS;
            }

            for (long startIndex = 0; startIndex < numberOfKeys; startIndex = startIndex + stride) {
                long endIndex = startIndex + stride - 1;
                if (endIndex >= numberOfKeys) {
                    endIndex = -1;
                }

                RedisSplit split = new RedisSplit(connectorId,
                        partition.getSchemaName(),
                        partition.getTableName(),
                        redisTableHandle.getKeyDataFormat(),
                        redisTableHandle.getValueDataFormat(),
                        redisTableHandle.getKeyName(),
                        startIndex,
                        endIndex,
                        partition.getPartitionNodes());

                builder.add(split);
            }
        }
        return new FixedSplitSource(connectorId, builder.build());
    }
}
