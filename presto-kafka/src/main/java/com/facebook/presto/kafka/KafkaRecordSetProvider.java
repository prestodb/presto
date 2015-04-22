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
package com.facebook.presto.kafka;

import com.facebook.presto.kafka.decoder.KafkaDecoderRegistry;
import com.facebook.presto.kafka.decoder.KafkaFieldDecoder;
import com.facebook.presto.kafka.decoder.KafkaRowDecoder;
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
 * Factory for Kafka specific {@link RecordSet} instances.
 */
public class KafkaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final KafkaHandleResolver handleResolver;
    private final KafkaSimpleConsumerManager consumerManager;
    private final KafkaDecoderRegistry registry;

    @Inject
    public KafkaRecordSetProvider(
            KafkaDecoderRegistry registry,
            KafkaHandleResolver handleResolver,
            KafkaSimpleConsumerManager consumerManager)
    {
        this.registry = checkNotNull(registry, "registry is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");
        this.consumerManager = checkNotNull(consumerManager, "consumerManager is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        KafkaSplit kafkaSplit = handleResolver.convertSplit(split);

        ImmutableList.Builder<KafkaColumnHandle> handleBuilder = ImmutableList.builder();
        ImmutableMap.Builder<KafkaColumnHandle, KafkaFieldDecoder<?>> keyFieldDecoderBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<KafkaColumnHandle, KafkaFieldDecoder<?>> messageFieldDecoderBuilder = ImmutableMap.builder();

        KafkaRowDecoder keyDecoder = registry.getRowDecoder(kafkaSplit.getKeyDataFormat());
        KafkaRowDecoder messageDecoder = registry.getRowDecoder(kafkaSplit.getMessageDataFormat());

        for (ColumnHandle handle : columns) {
            KafkaColumnHandle columnHandle = handleResolver.convertColumnHandle(handle);
            handleBuilder.add(columnHandle);

            if (!columnHandle.isInternal()) {
                if (columnHandle.isKeyDecoder()) {
                    KafkaFieldDecoder<?> fieldDecoder = registry.getFieldDecoder(
                            kafkaSplit.getKeyDataFormat(),
                            columnHandle.getType().getJavaType(),
                            columnHandle.getDataFormat());

                    keyFieldDecoderBuilder.put(columnHandle, fieldDecoder);
                }
                else {
                    KafkaFieldDecoder<?> fieldDecoder = registry.getFieldDecoder(
                            kafkaSplit.getMessageDataFormat(),
                            columnHandle.getType().getJavaType(),
                            columnHandle.getDataFormat());

                    messageFieldDecoderBuilder.put(columnHandle, fieldDecoder);
                }
            }
        }

        ImmutableList<KafkaColumnHandle> handles = handleBuilder.build();
        ImmutableMap<KafkaColumnHandle, KafkaFieldDecoder<?>> keyFieldDecoders = keyFieldDecoderBuilder.build();
        ImmutableMap<KafkaColumnHandle, KafkaFieldDecoder<?>> messageFieldDecoders = messageFieldDecoderBuilder.build();

        return new KafkaRecordSet(kafkaSplit, consumerManager, handles, keyDecoder, messageDecoder, keyFieldDecoders, messageFieldDecoders);
    }
}
