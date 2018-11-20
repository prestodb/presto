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

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
<<<<<<< HEAD
=======
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
>>>>>>> c6134c443d3475349b87b821fada78a04197b74b
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
<<<<<<< HEAD
import io.airlift.slice.Slices;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
=======
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
>>>>>>> c6134c443d3475349b87b821fada78a04197b74b

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

<<<<<<< HEAD
=======
import static com.facebook.presto.decoder.FieldValueProviders.booleanValueProvider;
import static com.facebook.presto.decoder.FieldValueProviders.bytesValueProvider;
import static com.facebook.presto.decoder.FieldValueProviders.longValueProvider;
import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
>>>>>>> c6134c443d3475349b87b821fada78a04197b74b
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific record set. Returns a cursor for a topic which iterates over a Kafka partition segment.
 */
public class KafkaRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(KafkaRecordSet.class);

    private static final int KAFKA_READ_BUFFER_SIZE = 100_000;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final KafkaSplit split;
    private final KafkaConsumerManager consumerManager;

    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;

    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    KafkaRecordSet(KafkaSplit split,
<<<<<<< HEAD
                   KafkaConsumerManager consumerManager,
                   List<DecoderColumnHandle> columnHandles,
                   RowDecoder keyDecoder,
                   RowDecoder messageDecoder,
                   Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders,
                   Map<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoders)
=======
            KafkaSimpleConsumerManager consumerManager,
            List<KafkaColumnHandle> columnHandles,
            RowDecoder keyDecoder,
            RowDecoder messageDecoder)
>>>>>>> c6134c443d3475349b87b821fada78a04197b74b
    {
        this.split = requireNonNull(split, "split is null");

        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        this.keyDecoder = requireNonNull(keyDecoder, "rowDecoder is null");
        this.messageDecoder = requireNonNull(messageDecoder, "rowDecoder is null");

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
        return new KafkaRecordCursor();
    }

    public class KafkaRecordCursor
            implements RecordCursor
    {
        private long totalBytes;
        private long totalMessages;
        private long cursorOffset = split.getStart();
        private final AtomicBoolean reported = new AtomicBoolean();
        private Iterator<ConsumerRecord<byte[], byte[]>> consumerRecordIterator;

        private final FieldValueProvider[] currentRowValues = new FieldValueProvider[columnHandles.size()];

        KafkaRecordCursor()
        {
        }

        @Override
        public long getCompletedBytes()
        {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (true) {
                if (cursorOffset >= split.getEnd()) {
                    return endOfData(); // Split end is exclusive.
                }
                // Create a fetch request
                openFetchRequest();

                while (consumerRecordIterator.hasNext()) {
                    ConsumerRecord<byte[], byte[]> messageAndOffset = consumerRecordIterator.next();
                    long messageOffset = messageAndOffset.offset();
                    if (messageOffset >= split.getEnd()) {
                        return endOfData(); // Past our split end. Bail.
                    }

                    if (messageOffset >= cursorOffset) {
                        return nextRow(messageAndOffset);
                    }
                }
                consumerRecordIterator = null;
            }
        }

        private boolean endOfData()
        {
            if (!reported.getAndSet(true)) {
                log.debug("Found a total of %d messages with %d bytes (%d messages expected). Last Offset: %d (%d, %d)",
                        totalMessages, totalBytes, split.getEnd() - split.getStart(), cursorOffset, split.getStart(),
                        split.getEnd());
            }
            return false;
        }

        private boolean nextRow(ConsumerRecord<byte[], byte[]> consumerRecord)
        {
            cursorOffset = consumerRecord.offset() + 1;
            totalBytes += consumerRecord.serializedValueSize();
            totalMessages++;

            byte[] keyData = EMPTY_BYTE_ARRAY;
            byte[] messageData = EMPTY_BYTE_ARRAY;

            if (consumerRecord.key() != null) {
                ByteBuffer key = ByteBuffer.wrap(consumerRecord.key());
                keyData = new byte[key.remaining()];
                key.get(keyData);
            }

            if (consumerRecord.value() != null) {
                ByteBuffer message = ByteBuffer.wrap(consumerRecord.value());
                messageData = new byte[message.remaining()];
                message.get(messageData);
            }

<<<<<<< HEAD
            Set<FieldValueProvider> fieldValueProviders = new HashSet<>();

            fieldValueProviders.addAll(globalInternalFieldValueProviders);
            fieldValueProviders.add(KafkaInternalFieldDescription.SEGMENT_COUNT_FIELD.forLongValue(totalMessages));
            fieldValueProviders.add(KafkaInternalFieldDescription.PARTITION_OFFSET_FIELD.forLongValue(consumerRecord.offset()));
            fieldValueProviders.add(KafkaInternalFieldDescription.MESSAGE_FIELD.forByteValue(messageData));
            fieldValueProviders.add(KafkaInternalFieldDescription.MESSAGE_LENGTH_FIELD.forLongValue(messageData.length));
            fieldValueProviders.add(KafkaInternalFieldDescription.KEY_FIELD.forByteValue(keyData));
            fieldValueProviders.add(KafkaInternalFieldDescription.KEY_LENGTH_FIELD.forLongValue(keyData.length));
            fieldValueProviders.add(KafkaInternalFieldDescription.KEY_CORRUPT_FIELD.forBooleanValue(keyDecoder.decodeRow(keyData, null, fieldValueProviders, columnHandles, keyFieldDecoders)));
            fieldValueProviders.add(KafkaInternalFieldDescription.MESSAGE_CORRUPT_FIELD.forBooleanValue(messageDecoder.decodeRow(messageData, null, fieldValueProviders, columnHandles, messageFieldDecoders)));
            fieldValueProviders.add(KafkaInternalFieldDescription.MESSAGE_TIMESTAMP_FIELD.forLongValue(consumerRecord.timestamp()));
            fieldValueProviders.add(KafkaInternalFieldDescription.MESSAGE_TIMESTAMP_TYPE_FIELD.forLongValue(consumerRecord.timestampType().id));
=======
            Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(keyData, null);
            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = messageDecoder.decodeRow(messageData, null);

            for (DecoderColumnHandle columnHandle : columnHandles) {
                if (columnHandle.isInternal()) {
                    KafkaInternalFieldDescription fieldDescription = KafkaInternalFieldDescription.forColumnName(columnHandle.getName());
                    switch (fieldDescription) {
                        case SEGMENT_COUNT_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(totalMessages));
                            break;
                        case PARTITION_OFFSET_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(messageAndOffset.offset()));
                            break;
                        case MESSAGE_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(messageData));
                            break;
                        case MESSAGE_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(messageData.length));
                            break;
                        case KEY_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(keyData));
                            break;
                        case KEY_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(keyData.length));
                            break;
                        case KEY_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedKey.isPresent()));
                            break;
                        case MESSAGE_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedValue.isPresent()));
                            break;
                        case PARTITION_ID_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(split.getPartitionId()));
                            break;
                        case SEGMENT_START_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(split.getStart()));
                            break;
                        case SEGMENT_END_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(split.getEnd()));
                            break;
                        default:
                            throw new IllegalArgumentException("unknown internal field " + fieldDescription);
                    }
                }
            }
>>>>>>> c6134c443d3475349b87b821fada78a04197b74b

            decodedKey.ifPresent(currentRowValuesMap::putAll);
            decodedValue.ifPresent(currentRowValuesMap::putAll);

            for (int i = 0; i < columnHandles.size(); i++) {
                ColumnHandle columnHandle = columnHandles.get(i);
                currentRowValues[i] = currentRowValuesMap.get(columnHandle);
            }

            return true; // Advanced successfully.
        }

        @Override
        public boolean getBoolean(int field)
        {
            return getFieldValueProvider(field, boolean.class).getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            return getFieldValueProvider(field, long.class).getLong();
        }

        @Override
        public double getDouble(int field)
        {
            return getFieldValueProvider(field, double.class).getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            return getFieldValueProvider(field, Slice.class).getSlice();
        }

        @Override
        public Object getObject(int field)
        {
            return getFieldValueProvider(field, Block.class).getBlock();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return currentRowValues[field] == null || currentRowValues[field].isNull();
        }

        private FieldValueProvider getFieldValueProvider(int field, Class<?> expectedType)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            checkFieldType(field, expectedType);
            return currentRowValues[field];
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        @Override
        public void close()
        {
        }

        private void openFetchRequest()
        {
<<<<<<< HEAD
            if (consumerRecordIterator != null) {
                return;
            }
            ImmutableList.Builder<ConsumerRecord<byte[], byte[]>> consumerRecordsBuilder = ImmutableList.builder();
            try (KafkaConsumer<byte[], byte[]> consumer = consumerManager.getConsumer(ImmutableSet.of(split.getLeader()))) {
                TopicPartition topicPartition = new TopicPartition(split.getTopicName(), split.getPartitionId());
                consumer.assign(ImmutableList.of(topicPartition));
                long nextOffset = split.getStart();
                consumer.seek(topicPartition, split.getStart());
                while (nextOffset <= split.getEnd() - 1) {
                    ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(100);
                    consumerRecordsBuilder.addAll(consumerRecords.records(topicPartition));
                    nextOffset = consumer.position(topicPartition);
                }
=======
            try {
                if (messageAndOffsetIterator == null) {
                    log.debug("Fetching %d bytes from offset %d (%d - %d). %d messages read so far", KAFKA_READ_BUFFER_SIZE, cursorOffset, split.getStart(), split.getEnd(), totalMessages);
                    FetchRequest req = new FetchRequestBuilder()
                            .clientId("presto-worker-" + Thread.currentThread().getName())
                            .addFetch(split.getTopicName(), split.getPartitionId(), cursorOffset, KAFKA_READ_BUFFER_SIZE)
                            .build();

                    // TODO - this should look at the actual node this is running on and prefer
                    // that copy if running locally. - look into NodeInfo
                    SimpleConsumer consumer = consumerManager.getConsumer(split.getLeader());

                    FetchResponse fetchResponse = consumer.fetch(req);
                    if (fetchResponse.hasError()) {
                        short errorCode = fetchResponse.errorCode(split.getTopicName(), split.getPartitionId());
                        log.warn("Fetch response has error: %d", errorCode);
                        throw new RuntimeException("could not fetch data from Kafka, error code is '" + errorCode + "'");
                    }

                    messageAndOffsetIterator = fetchResponse.messageSet(split.getTopicName(), split.getPartitionId()).iterator();
                }
            }
            catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
                if (e instanceof PrestoException) {
                    throw e;
                }
                throw new PrestoException(
                        KAFKA_SPLIT_ERROR,
                        format(
                                "Cannot read data from topic '%s', partition '%s', startOffset %s, endOffset %s, leader %s ",
                                split.getTopicName(),
                                split.getPartitionId(),
                                split.getStart(),
                                split.getEnd(),
                                split.getLeader()),
                        e);
>>>>>>> c6134c443d3475349b87b821fada78a04197b74b
            }
            consumerRecordIterator = consumerRecordsBuilder.build().iterator();
        }
    }
}
