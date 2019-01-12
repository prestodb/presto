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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.decoder.FieldValueProviders.booleanValueProvider;
import static io.prestosql.decoder.FieldValueProviders.bytesValueProvider;
import static io.prestosql.decoder.FieldValueProviders.longValueProvider;
import static io.prestosql.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
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
    private final KafkaSimpleConsumerManager consumerManager;

    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;

    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    KafkaRecordSet(KafkaSplit split,
            KafkaSimpleConsumerManager consumerManager,
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
        private long totalBytes;
        private long totalMessages;
        private long cursorOffset = split.getStart();
        private Iterator<MessageAndOffset> messageAndOffsetIterator;
        private final AtomicBoolean reported = new AtomicBoolean();

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

                while (messageAndOffsetIterator.hasNext()) {
                    MessageAndOffset currentMessageAndOffset = messageAndOffsetIterator.next();
                    long messageOffset = currentMessageAndOffset.offset();

                    if (messageOffset >= split.getEnd()) {
                        return endOfData(); // Past our split end. Bail.
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

        private boolean nextRow(MessageAndOffset messageAndOffset)
        {
            cursorOffset = messageAndOffset.offset() + 1; // Cursor now points to the next message.
            totalBytes += messageAndOffset.message().payloadSize();
            totalMessages++;

            byte[] keyData = EMPTY_BYTE_ARRAY;
            byte[] messageData = EMPTY_BYTE_ARRAY;
            ByteBuffer key = messageAndOffset.message().key();
            if (key != null) {
                keyData = new byte[key.remaining()];
                key.get(keyData);
            }

            ByteBuffer message = messageAndOffset.message().payload();
            if (message != null) {
                messageData = new byte[message.remaining()];
                message.get(messageData);
            }

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
            }
        }
    }
}
