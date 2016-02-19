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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Describes an internal (managed by the connector) field which is added to each table row. The definition itself makes the row
 * show up in the tables (the columns are hidden by default, so they must be explicitly selected) but unless the field is hooked in using the
 * forBooleanValue/forLongValue/forBytesValue methods and the resulting FieldValueProvider is then passed into the appropriate row decoder, the fields
 * will be null. Most values are assigned in the {@link com.facebook.presto.kafka.KafkaRecordSet}.
 */
public class KafkaInternalFieldDescription
{
    /**
     * <tt>_partition_id</tt> - Kafka partition id.
     */
    public static final KafkaInternalFieldDescription PARTITION_ID_FIELD = new KafkaInternalFieldDescription("_partition_id", BigintType.BIGINT, "Partition Id");

    /**
     * <tt>_partition_offset</tt> - The current offset of the message in the partition.
     */
    public static final KafkaInternalFieldDescription PARTITION_OFFSET_FIELD = new KafkaInternalFieldDescription("_partition_offset", BigintType.BIGINT, "Offset for the message within the partition");

    /**
     * <tt>_segment_start</tt> - Kafka start offset for the segment which contains the current message. This is per-partition.
     */
    public static final KafkaInternalFieldDescription SEGMENT_START_FIELD = new KafkaInternalFieldDescription("_segment_start", BigintType.BIGINT, "Segment start offset");

    /**
     * <tt>_segment_end</tt> - Kafka end offset for the segment which contains the current message. This is per-partition. The end offset is the first offset that is *not* in the segment.
     */
    public static final KafkaInternalFieldDescription SEGMENT_END_FIELD = new KafkaInternalFieldDescription("_segment_end", BigintType.BIGINT, "Segment end offset");

    /**
     * <tt>_segment_count</tt> - Running count of messages in a segment.
     */
    public static final KafkaInternalFieldDescription SEGMENT_COUNT_FIELD = new KafkaInternalFieldDescription("_segment_count", BigintType.BIGINT, "Running message count per segment");

    /**
     * <tt>_message_corrupt</tt> - True if the row converter could not read the a message. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    public static final KafkaInternalFieldDescription MESSAGE_CORRUPT_FIELD = new KafkaInternalFieldDescription("_message_corrupt", BooleanType.BOOLEAN, "Message data is corrupt");

    /**
     * <tt>_message</tt> - Represents the full topic as a text column. Format is UTF-8 which may be wrong for some topics. TODO: make charset configurable.
     */
    public static final KafkaInternalFieldDescription MESSAGE_FIELD = new KafkaInternalFieldDescription("_message", VarcharType.VARCHAR, "Message text");

    /**
     * <tt>_message_length</tt> - length in bytes of the message.
     */
    public static final KafkaInternalFieldDescription MESSAGE_LENGTH_FIELD = new KafkaInternalFieldDescription("_message_length", BigintType.BIGINT, "Total number of message bytes");

    /**
     * <tt>_key_corrupt</tt> - True if the row converter could not read the a key. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    public static final KafkaInternalFieldDescription KEY_CORRUPT_FIELD = new KafkaInternalFieldDescription("_key_corrupt", BooleanType.BOOLEAN, "Key data is corrupt");

    /**
     * <tt>_key</tt> - Represents the key as a text column. Format is UTF-8 which may be wrong for topics. TODO: make charset configurable.
     */
    public static final KafkaInternalFieldDescription KEY_FIELD = new KafkaInternalFieldDescription("_key", VarcharType.VARCHAR, "Key text");

    /**
     * <tt>_key_length</tt> - length in bytes of the key.
     */
    public static final KafkaInternalFieldDescription KEY_LENGTH_FIELD = new KafkaInternalFieldDescription("_key_length", BigintType.BIGINT, "Total number of key bytes");

    public static Set<KafkaInternalFieldDescription> getInternalFields()
    {
        return ImmutableSet.of(PARTITION_ID_FIELD, PARTITION_OFFSET_FIELD,
                SEGMENT_START_FIELD, SEGMENT_END_FIELD, SEGMENT_COUNT_FIELD,
                KEY_FIELD, KEY_CORRUPT_FIELD, KEY_LENGTH_FIELD,
                MESSAGE_FIELD, MESSAGE_CORRUPT_FIELD, MESSAGE_LENGTH_FIELD);
    }

    private final String name;
    private final Type type;
    private final String comment;

    KafkaInternalFieldDescription(
            String name,
            Type type,
            String comment)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    KafkaColumnHandle getColumnHandle(String connectorId, int index, boolean hidden)
    {
        return new KafkaColumnHandle(connectorId,
                index,
                getName(),
                getType(),
                null,
                null,
                null,
                false,
                hidden,
                true);
    }

    ColumnMetadata getColumnMetadata(boolean hidden)
    {
        return new ColumnMetadata(name, type, comment, hidden);
    }

    public FieldValueProvider forBooleanValue(boolean value)
    {
        return new BooleanKafkaFieldValueProvider(value);
    }

    public FieldValueProvider forLongValue(long value)
    {
        return new LongKafkaFieldValueProvider(value);
    }

    public FieldValueProvider forByteValue(byte[] value)
    {
        return new BytesKafkaFieldValueProvider(value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        KafkaInternalFieldDescription other = (KafkaInternalFieldDescription) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .toString();
    }

    public class BooleanKafkaFieldValueProvider
            extends FieldValueProvider
    {
        private final boolean value;

        private BooleanKafkaFieldValueProvider(boolean value)
        {
            this.value = value;
        }

        @Override
        public boolean accept(DecoderColumnHandle columnHandle)
        {
            return columnHandle.getName().equals(name);
        }

        @Override
        public boolean getBoolean()
        {
            return value;
        }

        @Override
        public boolean isNull()
        {
            return false;
        }
    }

    public class LongKafkaFieldValueProvider
            extends FieldValueProvider
    {
        private final long value;

        private LongKafkaFieldValueProvider(long value)
        {
            this.value = value;
        }

        @Override
        public boolean accept(DecoderColumnHandle columnHandle)
        {
            return columnHandle.getName().equals(name);
        }

        @Override
        public long getLong()
        {
            return value;
        }

        @Override
        public boolean isNull()
        {
            return false;
        }
    }

    public class BytesKafkaFieldValueProvider
            extends FieldValueProvider
    {
        private final byte[] value;

        private BytesKafkaFieldValueProvider(byte[] value)
        {
            this.value = value;
        }

        @Override
        public boolean accept(DecoderColumnHandle columnHandle)
        {
            return columnHandle.getName().equals(name);
        }

        @Override
        public Slice getSlice()
        {
            return isNull() ? Slices.EMPTY_SLICE : Slices.wrappedBuffer(value);
        }

        @Override
        public boolean isNull()
        {
            return value == null || value.length == 0;
        }
    }
}
