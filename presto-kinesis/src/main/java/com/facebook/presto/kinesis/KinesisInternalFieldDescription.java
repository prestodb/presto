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
package com.facebook.presto.kinesis;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Set;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

public class KinesisInternalFieldDescription
{
    public static final KinesisInternalFieldDescription SHARD_ID_FIELD = new KinesisInternalFieldDescription("_shard_id", VarcharType.VARCHAR, "Shard Id");

    public static final KinesisInternalFieldDescription SHARD_SEQUENCE_ID_FIELD = new KinesisInternalFieldDescription("_shard_sequence_id", VarcharType.VARCHAR, "sequence id of message within the shard");

    public static final KinesisInternalFieldDescription SEGMENT_START_FIELD = new KinesisInternalFieldDescription("_segment_start", VarcharType.VARCHAR, "Segment start sequence id");

    public static final KinesisInternalFieldDescription SEGMENT_END_FIELD = new KinesisInternalFieldDescription("_segment_end", VarcharType.VARCHAR, "Segment end sequence id");

    public static final KinesisInternalFieldDescription SEGMENT_COUNT_FIELD = new KinesisInternalFieldDescription("_segment_count", BigintType.BIGINT, "Running message count per segment");

    public static final KinesisInternalFieldDescription MESSAGE_VALID_FIELD = new KinesisInternalFieldDescription("_message_valid", BooleanType.BOOLEAN, "Message data is valid");

    public static final KinesisInternalFieldDescription MESSAGE_FIELD = new KinesisInternalFieldDescription("_message", VarcharType.VARCHAR, "Message text");

    public static final KinesisInternalFieldDescription MESSAGE_LENGTH_FIELD = new KinesisInternalFieldDescription("_message_length", BigintType.BIGINT, "Total number of message bytes");

    public static final KinesisInternalFieldDescription PARTITION_KEY_FIELD = new KinesisInternalFieldDescription("_partition_key", VarcharType.VARCHAR, "Key text");

    public static Set<KinesisInternalFieldDescription> getInternalFields()
    {
        return ImmutableSet.of(SHARD_ID_FIELD, SHARD_SEQUENCE_ID_FIELD,
                SEGMENT_START_FIELD, SEGMENT_END_FIELD, SEGMENT_COUNT_FIELD,
                PARTITION_KEY_FIELD, MESSAGE_FIELD, MESSAGE_VALID_FIELD, MESSAGE_LENGTH_FIELD);
    }

    private final String name;
    private final Type type;
    private final String comment;

    KinesisInternalFieldDescription(
                String name,
                Type type,
                String comment)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = checkNotNull(type, "type is null");
        this.comment = checkNotNull(comment, "comment is null");
    }

    public String getName()
    {
        return name;
    }

    public Type getType()
    {
        return type;
    }

    KinesisColumnHandle getColumnHandle(String connectorId, int index, boolean hidden)
    {
        return new KinesisColumnHandle(connectorId,
                index,
                getName(),
                getType(),
                null,
                null,
                null,
                hidden,
                true);
    }

    ColumnMetadata getColumnMetadata(boolean hidden)
    {
        return new ColumnMetadata(name, type, false, comment, hidden);
    }

    public KinesisFieldValueProvider forBooleanValue(boolean value)
    {
        return new BooleanKinesisFieldValueProvider(value);
    }

    public KinesisFieldValueProvider forLongValue(long value)
    {
        return new LongKinesisFieldValueProvider(value);
    }

    public KinesisFieldValueProvider forByteValue(byte[] value)
    {
        return new BytesKinesisFieldValueProvider(value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, type);
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

        KinesisInternalFieldDescription other = (KinesisInternalFieldDescription) obj;
        return Objects.equal(this.name, other.name) &&
                Objects.equal(this.type, other.type);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .toString();
    }

    public class BooleanKinesisFieldValueProvider
            extends KinesisFieldValueProvider
    {
        private final boolean value;

        private BooleanKinesisFieldValueProvider(boolean value)
        {
            this.value = value;
        }

        @Override
        public boolean accept(KinesisColumnHandle columnHandle)
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

    public class LongKinesisFieldValueProvider
            extends KinesisFieldValueProvider
    {
        private final long value;

        private LongKinesisFieldValueProvider(long value)
        {
            this.value = value;
        }

        @Override
        public boolean accept(KinesisColumnHandle columnHandle)
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

    public class BytesKinesisFieldValueProvider
            extends KinesisFieldValueProvider
    {
        private final byte[] value;
        private BytesKinesisFieldValueProvider(byte[] value)
        {
            this.value = value;
        }

        @Override
        public boolean accept(KinesisColumnHandle columnHandle)
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
