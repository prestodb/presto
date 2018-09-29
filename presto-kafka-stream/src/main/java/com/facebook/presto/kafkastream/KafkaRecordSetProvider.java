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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KafkaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(KafkaRecordSetProvider.class);
    private final String connectorId;
    private KafkaMetadata kafkaMetadata;
    private KafkaMetadataClient kafkaMetadataClient;
    private KafkaKStreamConsumerManager kafkaKStreamConsumerManager;

    @Inject
    public KafkaRecordSetProvider(KafkaConnectorId connectorId,
            KafkaMetadata kafkaMetadata,
            KafkaMetadataClient restMetadataClient,
            KafkaKStreamConsumerManager kafkaKStreamConsumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.kafkaMetadata = requireNonNull(kafkaMetadata, "restMetadata is null");
        this.kafkaMetadataClient =
                requireNonNull(restMetadataClient, "restMetadata is null");
        this.kafkaKStreamConsumerManager = requireNonNull(kafkaKStreamConsumerManager,
                "kafkaConsumerManager can't be null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        log.debug("Inside getRecordSet ");
        requireNonNull(split, "partitionChunk is null");
        KafkaSplit restSplit = (KafkaSplit) split;

        ImmutableList.Builder<KafkaColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((KafkaColumnHandle) handle);
        }

        String schemaName = ((KafkaSplit) split).getSchemaName();
        String tableName = ((KafkaSplit) split).getTableName();
        ConnectorTableMetadata tableMetadata =
                kafkaMetadata.getTableMetadata(new SchemaTableName(
                        schemaName,
                        tableName));
        return new KafkaRecordSet(
                restSplit,
                handles.build(),
                tableMetadata.getColumns(),
                session,
                kafkaKStreamConsumerManager.getConsumer(
                        kafkaMetadataClient.getTable(schemaName, tableName)));
    }
}
