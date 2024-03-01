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

import com.facebook.presto.common.Page;
import com.facebook.presto.kafka.encoder.RowEncoder;
import com.facebook.presto.kafka.server.KafkaClusterMetadataSupplier;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_PRODUCER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KafkaPageSink
        implements ConnectorPageSink
{
    private final String topicName;
    private final List<KafkaColumnHandle> columns;
    private final RowEncoder keyEncoder;
    private final RowEncoder messageEncoder;
    private final KafkaProducer<byte[], byte[]> producer;
    private final ErrorCountingCallback errorCounter;

    public KafkaPageSink(
            String schemaName,
            String topicName,
            List<KafkaColumnHandle> columns,
            RowEncoder keyEncoder,
            RowEncoder messageEncoder,
            PlainTextKafkaProducerFactory producerFactory,
            KafkaClusterMetadataSupplier supplier)
    {
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.columns = requireNonNull(ImmutableList.copyOf(columns), "columns is null");
        this.keyEncoder = requireNonNull(keyEncoder, "keyEncoder is null");
        this.messageEncoder = requireNonNull(messageEncoder, "messageEncoder is null");
        requireNonNull(producerFactory, "producerFactory is null");
        List<HostAddress> nodes = supplier.getNodes(schemaName);
        this.producer = producerFactory.create(nodes);
        this.errorCounter = new ErrorCountingCallback();
    }

    private static class ErrorCountingCallback
            implements Callback
    {
        private final AtomicLong errorCounter;

        public ErrorCountingCallback()
        {
            this.errorCounter = new AtomicLong(0);
        }

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e)
        {
            if (e != null) {
                errorCounter.incrementAndGet();
            }
        }

        public long getErrorCount()
        {
            return errorCounter.get();
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                if (columns.get(channel).isKeyCodec()) {
                    keyEncoder.appendColumnValue(page.getBlock(channel), position);
                }
                else {
                    messageEncoder.appendColumnValue(page.getBlock(channel), position);
                }
            }
            producer.send(new ProducerRecord<>(topicName, keyEncoder.toByteArray(), messageEncoder.toByteArray()), errorCounter);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        producer.flush();
        producer.close();
        try {
            keyEncoder.close();
            messageEncoder.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to close row encoders", e);
        }

        if (errorCounter.getErrorCount() > 0) {
            throw new PrestoException(KAFKA_PRODUCER_ERROR, format("%d producer record('s) failed to send", errorCounter.getErrorCount()));
        }
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        producer.close();
    }
}
