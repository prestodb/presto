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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.decoder.FieldValueProviders.booleanValueProvider;
import static com.facebook.presto.decoder.FieldValueProviders.bytesValueProvider;
import static com.facebook.presto.decoder.FieldValueProviders.longValueProvider;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific record set. Returns a cursor for a topic which iterates over a Kafka partition for a specific range by startOffset and endOffset of messages.
 */
public class KafkaRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(KafkaRecordSet.class);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final int POLL_TIMEOUT = 500;

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
        private final TopicPartition topicPartition;
        private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
        private Iterator<ConsumerRecord<byte[], byte[]>> records = emptyIterator();
        private long completedBytes;

        private final FieldValueProvider[] currentRowValues = new FieldValueProvider[columnHandles.size()];

        KafkaRecordCursor()
        {
            topicPartition = new TopicPartition(split.getTopicName(), split.getPartitionId());
            kafkaConsumer = consumerManager.createConsumer(Thread.currentThread().getName(), split.getLeader());
            kafkaConsumer.assign(ImmutableList.of(topicPartition));
            kafkaConsumer.seek(topicPartition, split.getMessagesRange().getBegin());
        }

        @Override
        public long getCompletedBytes()
        {
            return completedBytes;
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
            if (!records.hasNext()) {
                if (kafkaConsumer.position(topicPartition) >= split.getMessagesRange().getEnd()) {
                    return false;
                }
                records = kafkaConsumer.poll(POLL_TIMEOUT).iterator();
                return advanceNextPosition();
            }

            return nextRow(records.next());
        }

        private boolean nextRow(ConsumerRecord<byte[], byte[]> message)
        {
            requireNonNull(message, "message is null");

            if (message.offset() >= split.getMessagesRange().getEnd()) {
                return false;
            }

            completedBytes += max(message.serializedKeySize(), 0) + max(message.serializedValueSize(), 0);

            byte[] keyData = EMPTY_BYTE_ARRAY;
            if (message.key() != null) {
                keyData = message.key();
            }

            byte[] messageData = EMPTY_BYTE_ARRAY;
            if (message.value() != null) {
                messageData = message.value();
            }
            long timeStamp = message.timestamp();

            Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(keyData, null);
            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = messageDecoder.decodeRow(messageData, null);

            for (DecoderColumnHandle columnHandle : columnHandles) {
                if (columnHandle.isInternal()) {
                    KafkaInternalFieldDescription fieldDescription = KafkaInternalFieldDescription.forColumnName(columnHandle.getName());
                    switch (fieldDescription) {
                        case PARTITION_OFFSET_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(message.offset()));
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
                        case OFFSET_TIMESTAMP_FIELD:
                            timeStamp = TimeUnit.SECONDS.toMillis(timeStamp);
                            currentRowValuesMap.put(columnHandle, longValueProvider(timeStamp));
                            break;
                        case KEY_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedKey.isPresent()));
                            break;
                        case MESSAGE_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedValue.isPresent()));
                            break;
                        case PARTITION_ID_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(message.partition()));
                            break;
                        default:
                            throw new IllegalArgumentException("unknown internal field " + columnHandle.getName());
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
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
        }
    }
}
