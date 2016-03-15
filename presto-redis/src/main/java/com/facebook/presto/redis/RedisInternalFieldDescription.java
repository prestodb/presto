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
package com.facebook.presto.redis;

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
 * will be null. Most values are assigned in the {@link RedisRecordSet}.
 */
public class RedisInternalFieldDescription
{
    /**
     * <tt>_key</tt> - Represents the key as a text column.
     */
    public static final RedisInternalFieldDescription KEY_FIELD = new RedisInternalFieldDescription("_key", VarcharType.VARCHAR, "Key text");

    /**
     * <tt>_value</tt> - Represents the value as a text column. Format is UTF-8
     */
    public static final RedisInternalFieldDescription VALUE_FIELD = new RedisInternalFieldDescription("_value", VarcharType.VARCHAR, "Value text");

    /**
     * <tt>_value_corrupt</tt> - True if the row converter could not read the value. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    public static final RedisInternalFieldDescription VALUE_CORRUPT_FIELD = new RedisInternalFieldDescription("_value_corrupt", BooleanType.BOOLEAN, "Value data is corrupt");

    /**
     * <tt>_value_length</tt> - length in bytes of the value.
     */
    public static final RedisInternalFieldDescription VALUE_LENGTH_FIELD = new RedisInternalFieldDescription("_value_length", BigintType.BIGINT, "Total number of value bytes");

    /**
     * <tt>_key_corrupt</tt> - True if the row converter could not read the key. May be null if the row converter does not set a value (e.g. the dummy row converter does not).
     */
    public static final RedisInternalFieldDescription KEY_CORRUPT_FIELD = new RedisInternalFieldDescription("_key_corrupt", BooleanType.BOOLEAN, "Key data is corrupt");

    /**
     * <tt>_key_length</tt> - length in bytes of the key.
     */
    public static final RedisInternalFieldDescription KEY_LENGTH_FIELD = new RedisInternalFieldDescription("_key_length", BigintType.BIGINT, "Total number of key bytes");

    public static Set<RedisInternalFieldDescription> getInternalFields()
    {
        return ImmutableSet.of(KEY_FIELD, VALUE_FIELD,
                KEY_LENGTH_FIELD, VALUE_LENGTH_FIELD,
                KEY_CORRUPT_FIELD, VALUE_CORRUPT_FIELD);
    }

    private final String name;
    private final Type type;
    private final String comment;

    RedisInternalFieldDescription(
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

    RedisColumnHandle getColumnHandle(String connectorId, int index, boolean hidden)
    {
        return new RedisColumnHandle(connectorId,
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
        return new BooleanRedisFieldValueProvider(value);
    }

    public FieldValueProvider forLongValue(long value)
    {
        return new LongRedisFieldValueProvider(value);
    }

    public FieldValueProvider forByteValue(byte[] value)
    {
        return new BytesRedisFieldValueProvider(value);
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

        RedisInternalFieldDescription other = (RedisInternalFieldDescription) obj;
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

    public class BooleanRedisFieldValueProvider
            extends FieldValueProvider
    {
        private final boolean value;

        private BooleanRedisFieldValueProvider(boolean value)
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

    public class LongRedisFieldValueProvider
            extends FieldValueProvider
    {
        private final long value;

        private LongRedisFieldValueProvider(long value)
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

    public class BytesRedisFieldValueProvider
            extends FieldValueProvider
    {
        private final byte[] value;

        private BytesRedisFieldValueProvider(byte[] value)
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
