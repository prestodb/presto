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

import com.facebook.presto.spi.DecodableColumnHandle;
import com.facebook.presto.utils.decoder.DecoderRegistry;
import com.facebook.presto.utils.decoder.FieldDecoder;
import com.facebook.presto.utils.decoder.RowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Factory for Redis specific {@link RecordSet} instances.
 */
public class RedisRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final RedisHandleResolver handleResolver;
    private final RedisJedisManager jedisManager;
    private final DecoderRegistry registry;

    @Inject
    public RedisRecordSetProvider(
            DecoderRegistry registry,
            RedisHandleResolver handleResolver,
            RedisJedisManager jedisManager)
    {
        this.registry = checkNotNull(registry, "registry is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.jedisManager = checkNotNull(jedisManager, "jedisManager is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        RedisSplit redisSplit = handleResolver.convertSplit(split);

        ImmutableList.Builder<DecodableColumnHandle> handleBuilder = ImmutableList.builder();
        ImmutableMap.Builder<DecodableColumnHandle, FieldDecoder<?>> keyFieldDecoderBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<DecodableColumnHandle, FieldDecoder<?>> valueFieldDecoderBuilder = ImmutableMap.builder();

        RowDecoder keyDecoder = registry.getRowDecoder(redisSplit.getKeyDataFormat());
        RowDecoder valueDecoder = registry.getRowDecoder(redisSplit.getValueDataFormat());

        for (ColumnHandle handle : columns) {
            RedisColumnHandle columnHandle = handleResolver.convertColumnHandle(handle);
            handleBuilder.add(columnHandle);

            if (!columnHandle.isInternal()) {
                if (columnHandle.isKeyDecoder()) {
                    FieldDecoder<?> fieldDecoder = registry.getFieldDecoder(
                            redisSplit.getKeyDataFormat(),
                            columnHandle.getType().getJavaType(),
                            columnHandle.getDataFormat());

                    keyFieldDecoderBuilder.put(columnHandle, fieldDecoder);
                }
                else {
                    FieldDecoder<?> fieldDecoder = registry.getFieldDecoder(
                            redisSplit.getValueDataFormat(),
                            columnHandle.getType().getJavaType(),
                            columnHandle.getDataFormat());

                    valueFieldDecoderBuilder.put(columnHandle, fieldDecoder);
                }
            }
        }

        ImmutableList<DecodableColumnHandle> handles = handleBuilder.build();
        ImmutableMap<DecodableColumnHandle, FieldDecoder<?>> keyFieldDecoders = keyFieldDecoderBuilder.build();
        ImmutableMap<DecodableColumnHandle, FieldDecoder<?>> valueFieldDecoders = valueFieldDecoderBuilder.build();

        return new RedisRecordSet(redisSplit, jedisManager, handles, keyDecoder, valueDecoder, keyFieldDecoders, valueFieldDecoders);
    }
}
