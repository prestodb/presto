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

import com.facebook.presto.redis.decoder.RedisFieldDecoder;
import com.facebook.presto.redis.decoder.RedisRowDecoder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Redis specific record set. Returns a cursor for a table which iterates over a Redis values.
 */
public class RedisRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(RedisRecordSet.class);

    private final RedisSplit split;
    private final RedisJedisManager jedisManager;

    private final RedisRowDecoder keyDecoder;
    private final RedisRowDecoder valueDecoder;
    private final Map<RedisColumnHandle, RedisFieldDecoder<?>> keyFieldDecoders;
    private final Map<RedisColumnHandle, RedisFieldDecoder<?>> valueFieldDecoders;

    private final List<RedisColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    RedisRecordSet(RedisSplit split,
                   RedisJedisManager jedisManager,
                   List<RedisColumnHandle> columnHandles,
                   RedisRowDecoder keyDecoder,
                   RedisRowDecoder valueDecoder,
                   Map<RedisColumnHandle, RedisFieldDecoder<?>> keyFieldDecoders,
                   Map<RedisColumnHandle, RedisFieldDecoder<?>> valueFieldDecoders)
    {
        this.split = checkNotNull(split, "split is null");

        this.jedisManager = checkNotNull(jedisManager, "jedisManager is null");

        this.keyDecoder = checkNotNull(keyDecoder, "keyDecoder is null");
        this.valueDecoder = checkNotNull(valueDecoder, "valueDecoder is null");
        this.keyFieldDecoders = checkNotNull(keyFieldDecoders, "keyFieldDecoders is null");
        this.valueFieldDecoders = checkNotNull(valueFieldDecoders, "valueFieldDecoders is null");
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (RedisColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new RedisRecordCursor(keyDecoder, valueDecoder, keyFieldDecoders, valueFieldDecoders, split, columnHandles, jedisManager);
    }
}
