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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.DecodableColumnHandle;
import com.facebook.presto.utils.decoder.RowDecoder;
import com.facebook.presto.utils.decoder.FieldDecoder;
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

    private final RowDecoder keyDecoder;
    private final RowDecoder valueDecoder;
    private final Map<DecodableColumnHandle, FieldDecoder<?>> keyFieldDecoders;
    private final Map<DecodableColumnHandle, FieldDecoder<?>> valueFieldDecoders;

    private final List<DecodableColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    RedisRecordSet(RedisSplit split,
                   RedisJedisManager jedisManager,
                   List<DecodableColumnHandle> columnHandles,
                   RowDecoder keyDecoder,
                   RowDecoder valueDecoder,
                   Map<DecodableColumnHandle, FieldDecoder<?>> keyFieldDecoders,
                   Map<DecodableColumnHandle, FieldDecoder<?>> valueFieldDecoders)
    {
        this.split = checkNotNull(split, "split is null");

        this.jedisManager = checkNotNull(jedisManager, "jedisManager is null");

        this.keyDecoder = checkNotNull(keyDecoder, "keyDecoder is null");
        this.valueDecoder = checkNotNull(valueDecoder, "valueDecoder is null");
        this.keyFieldDecoders = checkNotNull(keyFieldDecoders, "keyFieldDecoders is null");
        this.valueFieldDecoders = checkNotNull(valueFieldDecoders, "valueFieldDecoders is null");
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (DecodableColumnHandle handle : columnHandles) {
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
