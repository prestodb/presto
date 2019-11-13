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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.decoder.FieldValueProviders.booleanValueProvider;
import static com.facebook.presto.decoder.FieldValueProviders.bytesValueProvider;
import static com.facebook.presto.decoder.FieldValueProviders.longValueProvider;
import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
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

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final KafkaSplit split;
    private final KafkaConsumerManager consumerManager;

    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;

    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    KafkaRecordSet(KafkaSplit split,
                   KafkaConsumerManager consumerManager,
            List<KafkaColumnHandle> columnHandles,
            RowDecoder keyDecoder,
            RowDecoder messageDecoder)
    {
        this.split = requireNonNull(split, "split is null");

        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        this.keyDecoder = requireNonNull(keyDecoder, "rowDecoder is null");
        this.messageDecoder = requireNonNull(messageDecoder, "rowDecoder is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (DecoderColumnHandle handle : this.columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
        long threadId = Thread.currentThread().getId();
        KafkaThreadIdentifier consumerId = new KafkaThreadIdentifier(Integer.toString(split.getPartitionId()), threadId, split.getLeader());
        KafkaConsumer consumer = consumerManager.createConsumer(consumerId);
        TopicPartition tp = new TopicPartition(split.getTopicName(), split.getPartitionId());
        consumer.assign(ImmutableList.of(tp));
        setOffsetRange(consumer, split);
        consumer.close();
    }

    private static void setOffsetRange(KafkaConsumer<Long, String> consumer, KafkaSplit split)
    {
        if (split.getStartTs() > split.getEndTs()) {
            throw new IllegalArgumentException(String.format("Invalid Kafka Offset start/end pair: %s - %s", split.getStartTs(), split.getEndTs()));
        }

        TopicPartition topicPartition = new TopicPartition(split.getTopicName(), split.getPartitionId());
        consumer.assign(ImmutableList.of(topicPartition));

        long beginningOffset =
                (split.getStartTs() == 0) ?
                        consumer.beginningOffsets(ImmutableList.of(topicPartition)).values().iterator().next() :
                        findOffsetsByTimestamp(consumer, topicPartition, split.getStartTs());
        long endOffset =
                (split.getEndTs() == 0) ?
                        consumer.endOffsets(ImmutableList.of(topicPartition)).values().iterator().next() :
                        findOffsetsByTimestamp(consumer, topicPartition, split.getEndTs());

        split.setStart(beginningOffset);
        split.setEnd(endOffset);
    }

    private static long findOffsetsByTimestamp(KafkaConsumer<Long, String> consumer, TopicPartition tp, long timestamp)
    {
        try {
            Map<TopicPartition, OffsetAndTimestamp> tpOffsets = consumer.offsetsForTimes(ImmutableMap.of(tp, timestamp));
            if (tpOffsets == null || tpOffsets.values().size() == 0) {
                return 0;
            }
            OffsetAndTimestamp offsetAndTimestamp = new ArrayList<>(tpOffsets.values()).get(0);
            return (offsetAndTimestamp == null) ? 0 : offsetAndTimestamp.offset();
        }
        catch (Exception e) {
            log.error(e, String.format("Failed to find offset by timestamp: %d for partition %d", timestamp, tp.partition()));
            throw new RuntimeException(e);
        }
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
        private Iterator<ConsumerRecord<ByteBuffer, ByteBuffer>> messageAndOffsetIterator;
        private final AtomicBoolean reported = new AtomicBoolean();
        private KafkaConsumer consumer;
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
                    return endOfData();
                }
                // Create a fetch request
                openFetchRequest();

                while (messageAndOffsetIterator.hasNext()) {
                    ConsumerRecord<ByteBuffer, ByteBuffer> currentMessageAndOffset = messageAndOffsetIterator.next();
                    long messageOffset = currentMessageAndOffset.offset();

                    if (messageOffset >= split.getEnd()) {
                        return endOfData();
                    }

                    if (messageOffset >= cursorOffset) {
                        return nextRow(currentMessageAndOffset);
                    }
                }
                messageAndOffsetIterator = null;
            }
        }

        private boolean endOfData()
        {
            if (!reported.getAndSet(true)) {
                log.debug("Found a total of %d messages with %d bytes (%d messages expected). Last Offset: %d (%d, %d)",
                        totalMessages, totalBytes, split.getEnd() - split.getStart(),
                        cursorOffset, split.getStart(), split.getEnd());
            }
            return false;
        }

        private boolean nextRow(ConsumerRecord<ByteBuffer, ByteBuffer> messageAndOffset)
        {
            cursorOffset = messageAndOffset.offset() + 1; // Cursor now points to the next message.
            totalBytes += messageAndOffset.serializedValueSize();
            totalMessages++;

            byte[] keyData = EMPTY_BYTE_ARRAY;
            byte[] messageData = EMPTY_BYTE_ARRAY;
            ByteBuffer key = messageAndOffset.key();
            if (key != null) {
                keyData = new byte[key.remaining()];
                key.get(keyData);
            }

            ByteBuffer message = messageAndOffset.value();
            if (message != null) {
                messageData = new byte[message.remaining()];
                message.get(messageData);
            }

            long timeStamp = messageAndOffset.timestamp();
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
                        case OFFSET_TIMESTAMP_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(timeStamp));
                            break;
                        default:
                            throw new IllegalArgumentException("unknown internal field " + fieldDescription);
                    }
                }
            }

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
            log.debug("Close consumer if exist: %d, partition %d", Thread.currentThread().getId(), split.getPartitionId());
            if (this.consumer != null) {
                this.consumer.close();
            }
        }

        private void openFetchRequest()
        {
            try {
                if (messageAndOffsetIterator == null) {
                    long threadId = Thread.currentThread().getId();

                    if (this.consumer == null) {
                        KafkaThreadIdentifier consumerId = new KafkaThreadIdentifier(Integer.toString(split.getPartitionId()), threadId, split.getLeader());
                        this.consumer = consumerManager.createConsumer(consumerId);
                    }

                    TopicPartition tp = new TopicPartition(split.getTopicName(), split.getPartitionId());
                    this.consumer.assign(ImmutableList.of(tp));
                    this.consumer.seek(tp, cursorOffset);
                    ConsumerRecords<ByteBuffer, ByteBuffer> records = this.consumer.poll(3000);
                    this.consumer.commitSync();
                    messageAndOffsetIterator = records.records(tp).iterator();
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
            }
        }
    }
}
