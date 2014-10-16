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
package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.StreamId;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.stream.BooleanStreamSource;
import com.facebook.presto.orc.stream.ByteArrayStreamSource;
import com.facebook.presto.orc.stream.ByteStreamSource;
import com.facebook.presto.orc.stream.DoubleStreamSource;
import com.facebook.presto.orc.stream.FloatStreamSource;
import com.facebook.presto.orc.stream.LongStreamSource;
import com.facebook.presto.orc.stream.OrcByteSource;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.orc.stream.RowGroupDictionaryLengthStreamSource;
import com.facebook.presto.orc.stream.StreamSource;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

import java.util.List;

import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_DIRECT;
import static com.facebook.presto.orc.metadata.CompressionKind.UNCOMPRESSED;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.INT;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.TIMESTAMP;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY_LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;

public final class StreamSources
{
    private StreamSources()
    {
    }

    public static StreamSource<?> createStreamSource(
            StreamId streamId,
            OrcInputStream inputStream,
            OrcTypeKind type,
            ColumnEncodingKind encoding,
            boolean usesVInt,
            CompressionKind compressionKind,
            List<Integer> offsetPositions)
    {
        // create byte source with initial offset into uncompressed stream
        int compressedBlockOffset = 0;
        if (!offsetPositions.isEmpty()) {
            compressedBlockOffset = Ints.checkedCast(offsetPositions.get(0));
            offsetPositions = offsetPositions.subList(1, offsetPositions.size());
        }

        int decompressedOffset;
        if (!offsetPositions.isEmpty() & compressionKind != UNCOMPRESSED) {
            decompressedOffset = Ints.checkedCast(offsetPositions.get(0));
            offsetPositions = offsetPositions.subList(1, offsetPositions.size());
        }
        else {
            decompressedOffset = compressedBlockOffset;
            compressedBlockOffset = 0;
        }
        OrcByteSource byteSource = new OrcByteSource(inputStream, compressedBlockOffset, decompressedOffset);

        if (streamId.getStreamKind() == PRESENT) {
            return new BooleanStreamSource(byteSource, getBooleanStreamStartOffset(offsetPositions));
        }

        if (streamId.getStreamKind() == DICTIONARY_DATA) {
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
        if ((encoding == DICTIONARY || encoding == DICTIONARY_V2) && (streamId.getStreamKind() == LENGTH || streamId.getStreamKind() == DATA)) {
            return new LongStreamSource(byteSource, encoding, INT, false, usesVInt, getPosition(offsetPositions, 0));
        }

        if (streamId.getStreamKind() == DATA) {
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
        if (streamId.getStreamKind() == LENGTH) {
            switch (type) {
                case STRING:
                case BINARY:
                case MAP:
                case LIST:
                    return new LongStreamSource(byteSource, encoding, type, false, usesVInt, getPosition(offsetPositions, 0));
            }
        }

        // length stream of a the row group dictionary
        if (streamId.getStreamKind() == ROW_GROUP_DICTIONARY_LENGTH) {
            switch (type) {
                case STRING:
                case BINARY:
                    return new RowGroupDictionaryLengthStreamSource(byteSource, encoding, false, getPosition(offsetPositions, 0), getPosition(offsetPositions, 1));
            }
        }

        // row group dictionary
        if (streamId.getStreamKind() == ROW_GROUP_DICTIONARY) {
            switch (type) {
                case STRING:
                case BINARY:
                    return new ByteArrayStreamSource(byteSource);
            }
        }

        // row group dictionary
        if (streamId.getStreamKind() == IN_DICTIONARY) {
            return new BooleanStreamSource(byteSource, getBooleanStreamStartOffset(offsetPositions));
        }

        // length (nanos) of a timestamp column
        if (type == TIMESTAMP && streamId.getStreamKind() == SECONDARY) {
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
