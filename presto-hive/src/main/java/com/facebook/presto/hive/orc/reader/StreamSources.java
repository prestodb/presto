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

import com.facebook.presto.hive.orc.stream.OrcByteSource;
import com.facebook.presto.hive.orc.StreamId;
import com.facebook.presto.hive.orc.stream.BooleanStreamSource;
import com.facebook.presto.hive.orc.stream.ByteArrayStreamSource;
import com.facebook.presto.hive.orc.stream.ByteStreamSource;
import com.facebook.presto.hive.orc.stream.DoubleStreamSource;
import com.facebook.presto.hive.orc.stream.FloatStreamSource;
import com.facebook.presto.hive.orc.stream.LongStreamSource;
import com.facebook.presto.hive.orc.stream.StreamSource;
import com.google.common.io.ByteSource;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;

import java.util.List;

import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DICTIONARY;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DICTIONARY_V2;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DIRECT;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DIRECT_V2;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind.NONE;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DICTIONARY_DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.LENGTH;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.SECONDARY;

public final class StreamSources
{
    private StreamSources()
    {
    }

    public static StreamSource<?> createStreamSource(
            StreamId streamId,
            Slice slice,
            Type.Kind type,
            Kind encoding,
            CompressionKind compressionKind,
            List<Integer> offsetPositions,
            int bufferSize)
    {
        // create byte source with initial offset into uncompressed stream
        int inputStreamInitialOffset = 0;
        if (!offsetPositions.isEmpty() & compressionKind != NONE) {
            inputStreamInitialOffset = Ints.checkedCast(offsetPositions.get(0));
            offsetPositions = offsetPositions.subList(1, offsetPositions.size());
        }
        ByteSource byteSource = new OrcByteSource(slice, compressionKind, bufferSize, inputStreamInitialOffset);

        if (streamId.getKind() == PRESENT) {
            return new BooleanStreamSource(byteSource, Ints.checkedCast(offsetPositions.get(0) * 8 + offsetPositions.get(1)));
        }

        if (streamId.getKind() == DICTIONARY_DATA) {
            return new ByteArrayStreamSource(byteSource);
        }

        if (streamId.getKind() == LENGTH && (encoding == DICTIONARY || encoding == DICTIONARY_V2)) {
            return new LongStreamSource(byteSource, encoding, false, 0);
        }

        if (streamId.getKind() == DATA) {
            switch (type) {
                case BOOLEAN:
                    return new BooleanStreamSource(byteSource, Ints.checkedCast(offsetPositions.get(0) * 8 + offsetPositions.get(1)));
                case BYTE:
                    return new ByteStreamSource(byteSource, Ints.checkedCast(offsetPositions.get(0)));
                case SHORT:
                case INT:
                case LONG:
                    return new LongStreamSource(byteSource, encoding, true, Ints.checkedCast(offsetPositions.get(0)));
                case FLOAT:
                    return new FloatStreamSource(byteSource, 0);
                case DOUBLE:
                    return new DoubleStreamSource(byteSource, 0);
                case DATE:
                    return new LongStreamSource(byteSource, encoding, true, Ints.checkedCast(offsetPositions.get(0)));
                case STRING:
                case BINARY:
                    if (encoding == DIRECT || encoding == DIRECT_V2) {
                        return new ByteArrayStreamSource(byteSource);
                    }
                    else if (encoding == DICTIONARY || encoding == DICTIONARY_V2) {
                        return new LongStreamSource(byteSource, encoding, false, Ints.checkedCast(offsetPositions.get(0)));
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported encoding " + encoding);
                    }
                case TIMESTAMP:
                    return new LongStreamSource(byteSource, encoding, true, Ints.checkedCast(offsetPositions.get(0)));
            }
        }

        // length stream of a direct encoded string or binary column
        if (streamId.getKind() == LENGTH) {
            switch (type) {
                case STRING:
                case BINARY:
                case MAP:
                case LIST:
                    return new LongStreamSource(byteSource, encoding, false, Ints.checkedCast(offsetPositions.get(0)));
            }
        }

        // length (nanos) of a timestamp column
        if (type == Type.Kind.TIMESTAMP && streamId.getKind() == SECONDARY) {
            return new LongStreamSource(byteSource, encoding, false, Ints.checkedCast(offsetPositions.get(0)));
        }

        throw new IllegalArgumentException("Unsupported column type " + type + " for stream " + streamId);
    }
}
