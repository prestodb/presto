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
package io.prestosql.plugin.redis;

import com.google.common.collect.ImmutableList;
import io.prestosql.decoder.DispatchingRowDecoderFactory;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.plugin.redis.RedisHandleResolver.convertSplit;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Factory for Redis specific {@link RecordSet} instances.
 */
public class RedisRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final DispatchingRowDecoderFactory decoderFactory;
    private final RedisJedisManager jedisManager;

    @Inject
    public RedisRecordSetProvider(DispatchingRowDecoderFactory decoderFactory, RedisJedisManager jedisManager)
    {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.jedisManager = requireNonNull(jedisManager, "jedisManager is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        RedisSplit redisSplit = convertSplit(split);

        List<RedisColumnHandle> redisColumns = columns.stream()
                .map(RedisHandleResolver::convertColumnHandle)
                .collect(ImmutableList.toImmutableList());

        RowDecoder keyDecoder = decoderFactory.create(
                redisSplit.getKeyDataFormat(),
                emptyMap(),
                redisColumns.stream()
                        .filter(col -> !col.isInternal())
                        .filter(RedisColumnHandle::isKeyDecoder)
                        .collect(toImmutableSet()));

        RowDecoder valueDecoder = decoderFactory.create(
                redisSplit.getValueDataFormat(),
                emptyMap(),
                redisColumns.stream()
                        .filter(col -> !col.isInternal())
                        .filter(col -> !col.isKeyDecoder())
                        .collect(toImmutableSet()));

        return new RedisRecordSet(redisSplit, jedisManager, redisColumns, keyDecoder, valueDecoder);
    }
}
