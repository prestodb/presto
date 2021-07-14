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

import com.facebook.presto.kafka.encoder.DispatchingRowEncoderFactory;
import com.facebook.presto.kafka.encoder.EncoderColumnHandle;
import com.facebook.presto.kafka.encoder.RowEncoder;
import com.facebook.presto.kafka.server.KafkaClusterMetadataSupplier;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final DispatchingRowEncoderFactory encoderFactory;
    private final PlainTextKafkaProducerFactory producerFactory;
    private final KafkaClusterMetadataSupplier kafkaClusterMetadataSupplier;

    @Inject
    public KafkaPageSinkProvider(DispatchingRowEncoderFactory encoderFactory, PlainTextKafkaProducerFactory producerFactory, KafkaClusterMetadataSupplier kafkaClusterMetadataSupplier)
    {
        this.encoderFactory = requireNonNull(encoderFactory, "encoderFactory is null");
        this.producerFactory = requireNonNull(producerFactory, "producerFactory is null");
        this.kafkaClusterMetadataSupplier = requireNonNull(kafkaClusterMetadataSupplier, "kafkaClusterMetadataSupplier is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, PageSinkContext pageSinkContext)
    {
        throw new UnsupportedOperationException("Table creation is not supported by the kafka connector");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, PageSinkContext pageSinkContext)
    {
        requireNonNull(insertTableHandle, "tableHandle is null");
        KafkaTableHandle handle = (KafkaTableHandle) insertTableHandle;

        ImmutableList.Builder<EncoderColumnHandle> keyColumns = ImmutableList.builder();
        ImmutableList.Builder<EncoderColumnHandle> messageColumns = ImmutableList.builder();
        handle.getColumns().forEach(col -> {
            if (col.isInternal()) {
                throw new IllegalArgumentException(format("unexpected internal column '%s'", col.getName()));
            }
            if (col.isKeyCodec()) {
                keyColumns.add(col);
            }
            else {
                messageColumns.add(col);
            }
        });

        RowEncoder keyEncoder = encoderFactory.create(
                session,
                handle.getKeyDataFormat(),
                getDataSchema(handle.getKeyDataSchemaLocation()),
                keyColumns.build());

        RowEncoder messageEncoder = encoderFactory.create(
                session,
                handle.getMessageDataFormat(),
                getDataSchema(handle.getMessageDataSchemaLocation()),
                messageColumns.build());

        return new KafkaPageSink(
                handle.getSchemaName(),
                handle.getTopicName(),
                handle.getColumns(),
                keyEncoder,
                messageEncoder,
                producerFactory,
                kafkaClusterMetadataSupplier);
    }

    private Optional<String> getDataSchema(Optional<String> dataSchemaLocation)
    {
        return dataSchemaLocation.map(location -> {
            try {
                return new String(Files.readAllBytes(Paths.get(location)));
            }
            catch (IOException e) {
                throw new PrestoException(KAFKA_SCHEMA_ERROR, format("Unable to read data schema at '%s'", dataSchemaLocation.get()), e);
            }
        });
    }
}
