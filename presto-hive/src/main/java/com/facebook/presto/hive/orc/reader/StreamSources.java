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
package com.facebook.presto.hive.orc.reader;

import com.facebook.presto.hive.orc.StreamId;
import com.facebook.presto.hive.orc.metadata.ColumnEncoding;
import com.facebook.presto.hive.orc.metadata.CompressionKind;
import com.facebook.presto.hive.orc.metadata.Type;
import com.facebook.presto.hive.orc.metadata.Type.Kind;
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.stream.ByteArrayStreamSource;
import com.facebook.presto.hive.orc.stream.ByteStreamSource;
import com.facebook.presto.hive.orc.stream.DoubleStreamSource;
import com.facebook.presto.hive.orc.stream.FloatStreamSource;
import com.facebook.presto.hive.orc.stream.LongStreamSource;
import com.facebook.presto.hive.orc.stream.OrcByteSource;
import com.facebook.presto.hive.orc.stream.StreamSource;
import com.facebook.presto.hive.orc.stream.StrideDictionaryLengthStreamSource;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteSource;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.Kind.DICTIONARY;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.Kind.DICTIONARY_V2;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.Kind.DWRF_DIRECT;
import static com.facebook.presto.hive.orc.metadata.CompressionKind.UNCOMPRESSED;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DICTIONARY_DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.IN_DICTIONARY;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.LENGTH;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.PRESENT;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.SECONDARY;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.STRIDE_DICTIONARY;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.STRIDE_DICTIONARY_LENGTH;
import static com.facebook.presto.hive.orc.metadata.Type.Kind.INT;

public final class StreamSources
{
    private StreamSources()
    {
    }

    public static StreamSource<?> createStreamSource(
            StreamId streamId,
            Slice slice,
            Kind type,
            ColumnEncoding.Kind encoding,
            boolean usesVInt,
            CompressionKind compressionKind,
            List<Integer> offsetPositions,
            int bufferSize)
    {
        // create byte source with initial offset into uncompressed stream
        int inputStreamInitialOffset = 0;
        if (!offsetPositions.isEmpty() & compressionKind != UNCOMPRESSED) {
            inputStreamInitialOffset = Ints.checkedCast(offsetPositions.get(0));
            offsetPositions = offsetPositions.subList(1, offsetPositions.size());
        }
        ByteSource byteSource = new OrcByteSource(slice, compressionKind, bufferSize, inputStreamInitialOffset);

        if (streamId.getKind() == PRESENT) {
            return new BooleanStreamSource(byteSource, getBooleanStreamStartOffset(offsetPositions));
        }

        if (streamId.getKind() == DICTIONARY_DATA) {
            switch (type) {
                case SHORT:
                case INT:
                case LONG:
                    return new LongStreamSource(byteSource, DWRF_DIRECT, INT, true, usesVInt, getPosition(offsetPositions, 0));
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                    return new ByteArrayStreamSource(byteSource);
            }
        }

        // dictionary length and data streams are unsigned int streams
        if ((encoding == DICTIONARY || encoding == DICTIONARY_V2) && (streamId.getKind() == LENGTH || streamId.getKind() == DATA)) {
            return new LongStreamSource(byteSource, encoding, INT, false, usesVInt, getPosition(offsetPositions, 0));
        }

        if (streamId.getKind() == DATA) {
            switch (type) {
                case BOOLEAN:
                    return new BooleanStreamSource(byteSource, getBooleanStreamStartOffset(offsetPositions));
                case BYTE:
                    return new ByteStreamSource(byteSource, Ints.checkedCast(getPosition(offsetPositions, 0)));
                case SHORT:
                case INT:
                case LONG:
                    return new LongStreamSource(byteSource, encoding, type, true, usesVInt, getPosition(offsetPositions, 0));
                case FLOAT:
                    return new FloatStreamSource(byteSource, getPosition(offsetPositions, 0));
                case DOUBLE:
                    return new DoubleStreamSource(byteSource, getPosition(offsetPositions, 0));
                case DATE:
                    return new LongStreamSource(byteSource, encoding, type, true, usesVInt, getPosition(offsetPositions, 0));
                case STRING:
                case BINARY:
                    return new ByteArrayStreamSource(byteSource);
                case TIMESTAMP:
                    return new LongStreamSource(byteSource, encoding, type, true, usesVInt, getPosition(offsetPositions, 0));
            }
        }

        // length stream of a direct encoded string or binary column
        if (streamId.getKind() == LENGTH) {
            switch (type) {
                case STRING:
                case BINARY:
                case MAP:
                case LIST:
                    return new LongStreamSource(byteSource, encoding, type, false, usesVInt, getPosition(offsetPositions, 0));
            }
        }

        // length stream of a the stride dictionary
        if (streamId.getKind() == STRIDE_DICTIONARY_LENGTH) {
            switch (type) {
                case STRING:
                case BINARY:
                    return new StrideDictionaryLengthStreamSource(byteSource, encoding, false, getPosition(offsetPositions, 0), getPosition(offsetPositions, 1));
            }
        }

        // stride dictionary
        if (streamId.getKind() == STRIDE_DICTIONARY) {
            switch (type) {
                case STRING:
                case BINARY:
                    return new ByteArrayStreamSource(byteSource);
            }
        }

        // stride dictionary
        if (streamId.getKind() == IN_DICTIONARY) {
            return new BooleanStreamSource(byteSource, getBooleanStreamStartOffset(offsetPositions));
        }

        // length (nanos) of a timestamp column
        if (type == Type.Kind.TIMESTAMP && streamId.getKind() == SECONDARY) {
            return new LongStreamSource(byteSource, encoding, type, false, usesVInt, getPosition(offsetPositions, 0));
        }

        throw new IllegalArgumentException("Unsupported column type " + type + " for stream " + streamId);
    }

    public static int getBooleanStreamStartOffset(List<Integer> offsetPositions)
    {
        return Ints.checkedCast(offsetPositions.get(0) * 8 + offsetPositions.get(1));
    }

    @SuppressWarnings("ConstantConditions")
    public static int getPosition(List<Integer> offsetPositions, int position)
    {
        return Iterables.get(offsetPositions, position, 0);
    }
}
