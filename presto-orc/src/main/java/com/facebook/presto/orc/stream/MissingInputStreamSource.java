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
package com.facebook.presto.orc.stream;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public class MissingInputStreamSource<S extends ValueInputStream<?>>
        implements InputStreamSource<S>
{
    private static final MissingInputStreamSource<BooleanInputStream> BOOLEAN_MISSING_STREAM_SOURCE = new MissingInputStreamSource<>(BooleanInputStream.class);
    private static final MissingInputStreamSource<ByteInputStream> BYTE_MISSING_STREAM_SOURCE = new MissingInputStreamSource<>(ByteInputStream.class);
    private static final MissingInputStreamSource<ByteArrayInputStream> BYTE_ARRAY_MISSING_STREAM_SOURCE = new MissingInputStreamSource<>(ByteArrayInputStream.class);
    private static final MissingInputStreamSource<DecimalInputStream> DECIMAL_MISSING_STREAM_SOURCE = new MissingInputStreamSource<>(DecimalInputStream.class);
    private static final MissingInputStreamSource<DoubleInputStream> DOUBLE_MISSING_STREAM_SOURCE = new MissingInputStreamSource<>(DoubleInputStream.class);
    private static final MissingInputStreamSource<FloatInputStream> FLOAT_MISSING_STREAM_SOURCE = new MissingInputStreamSource<>(FloatInputStream.class);
    private static final MissingInputStreamSource<LongInputStream> LONG_MISSING_STREAM_SOURCE = new MissingInputStreamSource<>(LongInputStream.class);
    private static final MissingInputStreamSource<RowGroupDictionaryLengthInputStream> ROW_GROUP_DICTIONARY_LENGTH_MISSING_STREAM_SOURCE = new MissingInputStreamSource<>(RowGroupDictionaryLengthInputStream.class);

    private final Class<S> streamType;

    private MissingInputStreamSource(Class<S> streamType)
    {
        this.streamType = requireNonNull(streamType, "streamType is null");
    }

    public static InputStreamSource<BooleanInputStream> getBooleanMissingStreamSource()
    {
        return BOOLEAN_MISSING_STREAM_SOURCE;
    }

    public static InputStreamSource<ByteInputStream> getByteMissingStreamSource()
    {
        return BYTE_MISSING_STREAM_SOURCE;
    }

    public static MissingInputStreamSource<ByteArrayInputStream> getByteArrayMissingStreamSource()
    {
        return BYTE_ARRAY_MISSING_STREAM_SOURCE;
    }

    public static MissingInputStreamSource<DecimalInputStream> getDecimalMissingStreamSource()
    {
        return DECIMAL_MISSING_STREAM_SOURCE;
    }

    public static MissingInputStreamSource<DoubleInputStream> getDoubleMissingStreamSource()
    {
        return DOUBLE_MISSING_STREAM_SOURCE;
    }

    public static MissingInputStreamSource<FloatInputStream> getFloatMissingStreamSource()
    {
        return FLOAT_MISSING_STREAM_SOURCE;
    }

    public static InputStreamSource<LongInputStream> getLongMissingStreamSource()
    {
        return LONG_MISSING_STREAM_SOURCE;
    }

    public static MissingInputStreamSource<RowGroupDictionaryLengthInputStream> getRowGroupDictionaryLengthMissingStreamSource()
    {
        return ROW_GROUP_DICTIONARY_LENGTH_MISSING_STREAM_SOURCE;
    }

    public static <S extends ValueInputStream<?>> InputStreamSource<S> missingStreamSource(Class<S> streamType)
    {
        if (BOOLEAN_MISSING_STREAM_SOURCE.streamType.equals(streamType)) {
            return (InputStreamSource<S>) BOOLEAN_MISSING_STREAM_SOURCE;
        }
        if (LONG_MISSING_STREAM_SOURCE.streamType.equals(streamType)) {
            return (InputStreamSource<S>) LONG_MISSING_STREAM_SOURCE;
        }
        if (BYTE_MISSING_STREAM_SOURCE.streamType.equals(streamType)) {
            return (InputStreamSource<S>) BYTE_MISSING_STREAM_SOURCE;
        }
        if (BYTE_ARRAY_MISSING_STREAM_SOURCE.streamType.equals(streamType)) {
            return (InputStreamSource<S>) BYTE_ARRAY_MISSING_STREAM_SOURCE;
        }
        if (DECIMAL_MISSING_STREAM_SOURCE.streamType.equals(streamType)) {
            return (InputStreamSource<S>) DECIMAL_MISSING_STREAM_SOURCE;
        }
        if (DOUBLE_MISSING_STREAM_SOURCE.streamType.equals(streamType)) {
            return (InputStreamSource<S>) DOUBLE_MISSING_STREAM_SOURCE;
        }
        if (FLOAT_MISSING_STREAM_SOURCE.streamType.equals(streamType)) {
            return (InputStreamSource<S>) FLOAT_MISSING_STREAM_SOURCE;
        }
        if (ROW_GROUP_DICTIONARY_LENGTH_MISSING_STREAM_SOURCE.streamType.equals(streamType)) {
            return (InputStreamSource<S>) ROW_GROUP_DICTIONARY_LENGTH_MISSING_STREAM_SOURCE;
        }
        return new MissingInputStreamSource<>(streamType);
    }

    @Override
    public Class<S> getStreamType()
    {
        return streamType;
    }

    @Nullable
    @Override
    public S openStream()
    {
        return null;
    }
}
