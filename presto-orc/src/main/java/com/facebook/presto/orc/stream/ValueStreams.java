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

import com.facebook.presto.orc.StreamId;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;

import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_DIRECT;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.DECIMAL;
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
import static java.lang.String.format;

public final class ValueStreams
{
    private ValueStreams()
    {
    }

    public static ValueStream<?> createValueStreams(
            StreamId streamId,
            OrcInputStream inputStream,
            OrcTypeKind type,
            ColumnEncodingKind encoding,
            boolean usesVInt)
    {
        if (streamId.getStreamKind() == PRESENT) {
            return new BooleanStream(inputStream);
        }

        // dictionary length and data streams are unsigned int streams
        if ((encoding == DICTIONARY || encoding == DICTIONARY_V2) && (streamId.getStreamKind() == LENGTH || streamId.getStreamKind() == DATA)) {
            return createLongStream(inputStream, encoding, INT, false, usesVInt);
        }

        if (streamId.getStreamKind() == DATA) {
            switch (type) {
                case BOOLEAN:
                    return new BooleanStream(inputStream);
                case BYTE:
                    return new ByteStream(inputStream);
                case SHORT:
                case INT:
                case LONG:
                case DATE:
                    return createLongStream(inputStream, encoding, type, true, usesVInt);
                case FLOAT:
                    return new FloatStream(inputStream);
                case DOUBLE:
                    return new DoubleStream(inputStream);
                case STRING:
                case VARCHAR:
                case BINARY:
                    return new ByteArrayStream(inputStream);
                case TIMESTAMP:
                    return createLongStream(inputStream, encoding, type, true, usesVInt);
                case DECIMAL:
                    return new DecimalStream(inputStream);
            }
        }

        // length stream of a direct encoded string or binary column
        if (streamId.getStreamKind() == LENGTH) {
            switch (type) {
                case STRING:
                case VARCHAR:
                case BINARY:
                case MAP:
                case LIST:
                    return createLongStream(inputStream, encoding, type, false, usesVInt);
            }
        }

        // length stream of a the row group dictionary
        if (streamId.getStreamKind() == ROW_GROUP_DICTIONARY_LENGTH) {
            switch (type) {
                case STRING:
                case VARCHAR:
                case BINARY:
                    return new RowGroupDictionaryLengthStream(inputStream, false);
            }
        }

        // row group dictionary
        if (streamId.getStreamKind() == ROW_GROUP_DICTIONARY) {
            switch (type) {
                case STRING:
                case VARCHAR:
                case BINARY:
                    return new ByteArrayStream(inputStream);
            }
        }

        // row group dictionary
        if (streamId.getStreamKind() == IN_DICTIONARY) {
            return new BooleanStream(inputStream);
        }

        // length (nanos) of a timestamp column
        if (type == TIMESTAMP && streamId.getStreamKind() == SECONDARY) {
            return createLongStream(inputStream, encoding, type, false, usesVInt);
        }

        // scale of a decimal column
        if (type == DECIMAL && streamId.getStreamKind() == SECONDARY) {
            // specification (https://orc.apache.org/docs/encodings.html) says scale stream is unsigned,
            // however Hive writer stores scale as signed integer (org.apache.hadoop.hive.ql.io.orc.WriterImpl.DecimalTreeWriter)
            // BUG link: https://issues.apache.org/jira/browse/HIVE-13229
            return createLongStream(inputStream, encoding, type, true, usesVInt);
        }

        if (streamId.getStreamKind() == DICTIONARY_DATA) {
            switch (type) {
                case SHORT:
                case INT:
                case LONG:
                    return createLongStream(inputStream, DWRF_DIRECT, INT, true, usesVInt);
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                    return new ByteArrayStream(inputStream);
            }
        }

        throw new IllegalArgumentException(format("Unsupported column type %s for stream %s with encoding %s", type, streamId, encoding));
    }

    private static ValueStream<?> createLongStream(
            OrcInputStream inputStream,
            ColumnEncodingKind encoding,
            OrcTypeKind type,
            boolean signed,
            boolean usesVInt)
    {
        if (encoding == DIRECT_V2 || encoding == DICTIONARY_V2) {
            return new LongStreamV2(inputStream, signed, false);
        }
        else if (encoding == DIRECT || encoding == DICTIONARY) {
            return new LongStreamV1(inputStream, signed);
        }
        else if (encoding == DWRF_DIRECT) {
            return new LongStreamDwrf(inputStream, type, signed, usesVInt);
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding for long stream: " + encoding);
        }
    }
}
