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
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Redis specific record set. Returns a cursor for a table which iterates over a Redis values.
 */
public class RedisRecordSet
        implements RecordSet
{
    private final RedisSplit split;
    private final RedisJedisManager jedisManager;

    private final RowDecoder keyDecoder;
    private final RowDecoder valueDecoder;

    private final List<RedisColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    RedisRecordSet(
            RedisSplit split,
            RedisJedisManager jedisManager,
            List<RedisColumnHandle> columnHandles,
            RowDecoder keyDecoder,
            RowDecoder valueDecoder)
    {
        this.split = requireNonNull(split, "split is null");

        this.jedisManager = requireNonNull(jedisManager, "jedisManager is null");

        this.keyDecoder = requireNonNull(keyDecoder, "keyDecoder is null");
        this.valueDecoder = requireNonNull(valueDecoder, "valueDecoder is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        for (DecoderColumnHandle handle : columnHandles) {
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
        return new RedisRecordCursor(keyDecoder, valueDecoder, split, columnHandles, jedisManager);
    }
}
