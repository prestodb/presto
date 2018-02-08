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
package com.facebook.presto.decoder.raw;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Varchars;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Decoder for raw (direct byte) rows. All field decoders map bytes directly to Presto columns.
 */
public class RawRowDecoder
        implements RowDecoder
{
    public static final String NAME = "raw";

    private final Set<DecoderColumnHandle> columnHandles;

    public RawRowDecoder(Set<DecoderColumnHandle> columnHandles)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        checkArgument(columnHandles.stream().noneMatch(DecoderColumnHandle::isInternal), "unexpected internal column");
        this.columnHandles = ImmutableSet.copyOf(columnHandles);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(
            byte[] data,
            Map<String, String> dataMap)
    {
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new HashMap<>();
        for (DecoderColumnHandle columnHandle : columnHandles) {
            decodedRow.put(columnHandle, decodeField(data, columnHandle));
        }
        return Optional.of(decodedRow);
    }

    private enum FieldType
    {
        BYTE(Byte.SIZE),
        SHORT(Short.SIZE),
        INT(Integer.SIZE),
        LONG(Long.SIZE),
        FLOAT(Float.SIZE),
        DOUBLE(Double.SIZE);

        private final int size;

        FieldType(int bitSize)
        {
            this.size = bitSize / 8;
        }

        public int getSize()
        {
            return size;
        }

        static FieldType forString(String value)
        {
            if (value != null) {
                for (FieldType fieldType : values()) {
                    if (value.toUpperCase(Locale.ENGLISH).equals(fieldType.name())) {
                        return fieldType;
                    }
                }
            }

            return null;
        }
    }

    public FieldValueProvider decodeField(byte[] value, DecoderColumnHandle columnHandle)
    {
        requireNonNull(value, "value is null");
        requireNonNull(columnHandle, "columnHandle is null");

        String mapping = columnHandle.getMapping();
        FieldType fieldType = columnHandle.getDataFormat() == null ? FieldType.BYTE : FieldType.forString(columnHandle.getDataFormat());

        int start = 0;
        int end = value.length;

        if (mapping != null) {
            List<String> fields = ImmutableList.copyOf(Splitter.on(':').limit(2).split(mapping));
            if (!fields.isEmpty()) {
                start = Integer.parseInt(fields.get(0));
                checkState(start >= 0 && start < value.length, "Found start %s, but only 0..%s is legal", start, value.length);
                if (fields.size() > 1) {
                    end = Integer.parseInt(fields.get(1));
                    checkState(end > 0 && end <= value.length, "Found end %s, but only 1..%s is legal", end, value.length);
                }
            }
        }

        checkState(start <= end, "Found start %s and end %s. start must be smaller than end", start, end);

        return new RawValueProvider(ByteBuffer.wrap(value, start, end - start), columnHandle, fieldType);
    }

    private static class RawValueProvider
            extends FieldValueProvider
    {
        protected final ByteBuffer value;
        protected final DecoderColumnHandle columnHandle;
        protected final FieldType fieldType;
        protected final int size;

        public RawValueProvider(ByteBuffer value, DecoderColumnHandle columnHandle, FieldType fieldType)
        {
            this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
            this.fieldType = requireNonNull(fieldType, "fieldType is null");
            this.size = value.limit() - value.position();
            // check size for non-null fields
            if (size > 0) {
                checkState(size >= fieldType.getSize(), "minimum byte size is %s, found %s,", fieldType.getSize(), size);
            }
            this.value = value;
        }

        @Override
        public final boolean isNull()
        {
            return size == 0;
        }

        @Override
        public boolean getBoolean()
        {
            if (isNull()) {
                return false;
            }
            switch (fieldType) {
                case BYTE:
                    return value.get() != 0;
                case SHORT:
                    return value.getShort() != 0;
                case INT:
                    return value.getInt() != 0;
                case LONG:
                    return value.getLong() != 0;
                default:
                    throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("conversion %s to boolean not supported", fieldType));
            }
        }

        @Override
        public long getLong()
        {
            if (isNull()) {
                return 0L;
            }
            switch (fieldType) {
                case BYTE:
                    return value.get();
                case SHORT:
                    return value.getShort();
                case INT:
                    return value.getInt();
                case LONG:
                    return value.getLong();
                default:
                    throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("conversion %s to long not supported", fieldType));
            }
        }

        @Override
        public double getDouble()
        {
            if (isNull()) {
                return 0.0d;
            }
            switch (fieldType) {
                case FLOAT:
                    return value.getFloat();
                case DOUBLE:
                    return value.getDouble();
                default:
                    throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("conversion %s to double not supported", fieldType));
            }
        }

        @Override
        public Slice getSlice()
        {
            if (isNull()) {
                return Slices.EMPTY_SLICE;
            }

            if (fieldType == FieldType.BYTE) {
                Slice slice = Slices.wrappedBuffer(value.slice());
                if (Varchars.isVarcharType(columnHandle.getType())) {
                    slice = Varchars.truncateToLength(slice, columnHandle.getType());
                }
                return slice;
            }

            throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("conversion %s to Slice not supported", fieldType));
        }
    }
}
