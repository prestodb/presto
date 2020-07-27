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

import com.facebook.presto.decoder.DispatchingRowDecoderFactory;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.kafka.KafkaHandleResolver.convertSplit;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Factory for Kafka specific {@link RecordSet} instances.
 */
public class KafkaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private DispatchingRowDecoderFactory decoderFactory;
    private final KafkaConsumerManager consumerManager;
    private final KafkaConnectorConfig config;

    @Inject
    public KafkaRecordSetProvider(DispatchingRowDecoderFactory decoderFactory, KafkaConsumerManager consumerManager, KafkaConnectorConfig config)
    {
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        KafkaSplit kafkaSplit = convertSplit(split);

        List<KafkaColumnHandle> kafkaColumns = columns.stream()
                .map(KafkaHandleResolver::convertColumnHandle)
                .collect(ImmutableList.toImmutableList());

        RowDecoder keyDecoder = decoderFactory.create(
                kafkaSplit.getKeyDataFormat(),
                getDecoderParameters(kafkaSplit.getKeyDataSchemaContents()),
                kafkaColumns.stream()
                        .filter(col -> !col.isInternal())
                        .filter(KafkaColumnHandle::isKeyCodec)
                        .collect(toImmutableSet()));

        RowDecoder messageDecoder = decoderFactory.create(
                kafkaSplit.getMessageDataFormat(),
                getDecoderParameters(kafkaSplit.getMessageDataSchemaContents()),
                kafkaColumns.stream()
                        .filter(col -> !col.isInternal())
                        .filter(col -> !col.isKeyCodec())
                        .collect(toImmutableSet()));

        return new KafkaRecordSet(kafkaSplit, consumerManager, kafkaColumns, keyDecoder, messageDecoder);
    }

    private Map<String, String> getDecoderParameters(Optional<String> dataSchema)
    {
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        dataSchema.ifPresent(schema -> parameters.put("dataSchema", schema));
        return parameters.build();
    }
}
