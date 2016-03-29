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
package com.facebook.presto.orc.checkpoint;

import com.facebook.presto.orc.StreamId;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.metadata.RowGroupIndex;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_DIRECT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_GROUP_DICTIONARY_LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.google.common.base.Predicates.equalTo;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Checkpoints
{
    private Checkpoints()
    {
    }

    public static Map<StreamId, StreamCheckpoint> getStreamCheckpoints(
            Set<Integer> columns,
            List<OrcType> columnTypes,
            CompressionKind compressionKind,
            int rowGroupId,
            List<ColumnEncoding> columnEncodings,
            Map<StreamId, Stream> streams,
            Map<Integer, List<RowGroupIndex>> columnIndexes)
            throws InvalidCheckpointException
    {
        ImmutableSetMultimap.Builder<Integer, StreamKind> streamKindsBuilder = ImmutableSetMultimap.builder();
        for (Stream stream : streams.values()) {
            streamKindsBuilder.put(stream.getColumn(), stream.getStreamKind());
        }
        SetMultimap<Integer, StreamKind> streamKinds = streamKindsBuilder.build();

        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();
        for (int column : columns) {
            List<Integer> positionsList = columnIndexes.get(column).get(rowGroupId).getPositions();

            ColumnEncodingKind columnEncoding = columnEncodings.get(column).getColumnEncodingKind();
            OrcTypeKind columnType = columnTypes.get(column).getOrcTypeKind();
            Set<StreamKind> availableStreams = streamKinds.get(column);

            ColumnPositionsList columnPositionsList = new ColumnPositionsList(column, columnType, positionsList);
            switch (columnType) {
                case BOOLEAN:
                    checkpoints.putAll(getBooleanColumnCheckpoints(column, compressionKind, availableStreams, columnPositionsList));
                    break;
                case BYTE:
                    checkpoints.putAll(getByteColumnCheckpoints(column, compressionKind, availableStreams, columnPositionsList));
                    break;
                case SHORT:
                case INT:
                case LONG:
                case DATE:
                    checkpoints.putAll(getLongColumnCheckpoints(column, columnEncoding, compressionKind, availableStreams, columnPositionsList));
                    break;
                case FLOAT:
                    checkpoints.putAll(getFloatColumnCheckpoints(column, compressionKind, availableStreams, columnPositionsList));
                    break;
                case DOUBLE:
                    checkpoints.putAll(getDoubleColumnCheckpoints(column, compressionKind, availableStreams, columnPositionsList));
                    break;
                case TIMESTAMP:
                    checkpoints.putAll(getTimestampColumnCheckpoints(column, columnEncoding, compressionKind, availableStreams, columnPositionsList));
                    break;
                case BINARY:
                case STRING:
                case VARCHAR:
                    checkpoints.putAll(getSliceColumnCheckpoints(column, columnEncoding, compressionKind, availableStreams, columnPositionsList));
                    break;
                case LIST:
                case MAP:
                    checkpoints.putAll(getListOrMapColumnCheckpoints(column, columnEncoding, compressionKind, availableStreams, columnPositionsList));
                    break;
                case STRUCT:
                    checkpoints.putAll(getStructColumnCheckpoints(column, compressionKind, availableStreams, columnPositionsList));
                    break;
                case DECIMAL:
                    checkpoints.putAll(getDecimalColumnCheckpoints(column, columnEncoding, compressionKind, availableStreams, columnPositionsList));
                    break;
                case CHAR:
                case UNION:
                    throw new IllegalArgumentException("Unsupported column type " + columnType);
            }

            // The DWRF code is not meticulous in the handling of checkpoints.  It appears that for the first row group
            // it will write checkpoints for all streams, but in other cases it will write only the streams that exist.
            // We detect this case by checking that all offsets in the initial position list are zero, and if so, we
            // clear the extra offsets
            if (columnPositionsList.hasNextPosition() && !Iterables.all(positionsList, equalTo(0))) {
                throw new InvalidCheckpointException(format("Column %s, of type %s, contains %s offset positions, but only %s positions were consumed",
                        column,
                        columnType,
                        positionsList.size(),
                        columnPositionsList.getIndex()));
            }
        }
        return checkpoints.build();
    }

    public static StreamCheckpoint getDictionaryStreamCheckpoint(StreamId streamId, OrcTypeKind columnType, ColumnEncodingKind columnEncoding)
    {
        if (streamId.getStreamKind() == DICTIONARY_DATA) {
            switch (columnType) {
                case SHORT:
                case INT:
                case LONG:
                    return new LongStreamDwrfCheckpoint(createInputStreamCheckpoint(0, 0));
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                    return new ByteArrayStreamCheckpoint(createInputStreamCheckpoint(0, 0));
            }
        }

        // dictionary length and data streams are unsigned long streams
        if (streamId.getStreamKind() == LENGTH || streamId.getStreamKind() == DATA) {
            if (columnEncoding == DICTIONARY_V2) {
                return new LongStreamV2Checkpoint(0, createInputStreamCheckpoint(0, 0));
            }
            else if (columnEncoding == DICTIONARY) {
                return new LongStreamV1Checkpoint(0, createInputStreamCheckpoint(0, 0));
            }
        }
        throw new IllegalArgumentException("Unsupported column type " + columnType + " for dictionary stream " + streamId);
    }

    private static Map<StreamId, StreamCheckpoint> getBooleanColumnCheckpoints(
            int column,
            CompressionKind compressionKind,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getByteColumnCheckpoints(
            int column,
            CompressionKind compressionKind,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), new ByteStreamCheckpoint(compressionKind, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getLongColumnCheckpoints(
            int column,
            ColumnEncodingKind encoding,
            CompressionKind compressionKind,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        if (availableStreams.contains(IN_DICTIONARY)) {
            checkpoints.put(new StreamId(column, IN_DICTIONARY), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), createLongStreamCheckpoint(encoding, compressionKind, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getFloatColumnCheckpoints(
            int column,
            CompressionKind compressionKind,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), new FloatStreamCheckpoint(compressionKind, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getDoubleColumnCheckpoints(
            int column,
            CompressionKind compressionKind,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), new DoubleStreamCheckpoint(compressionKind, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getTimestampColumnCheckpoints(
            int column,
            ColumnEncodingKind encoding,
            CompressionKind compressionKind,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), createLongStreamCheckpoint(encoding, compressionKind, positionsList));
        }

        if (availableStreams.contains(SECONDARY)) {
            checkpoints.put(new StreamId(column, SECONDARY), createLongStreamCheckpoint(encoding, compressionKind, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getSliceColumnCheckpoints(
            int column,
            ColumnEncodingKind encoding,
            CompressionKind compressionKind,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        if (encoding == DIRECT || encoding == DIRECT_V2) {
            if (availableStreams.contains(DATA)) {
                checkpoints.put(new StreamId(column, DATA), new ByteArrayStreamCheckpoint(compressionKind, positionsList));
            }

            if (availableStreams.contains(LENGTH)) {
                checkpoints.put(new StreamId(column, LENGTH), createLongStreamCheckpoint(encoding, compressionKind, positionsList));
            }
        }
        else if (encoding == DICTIONARY || encoding == DICTIONARY_V2) {
            // DWRF has rules inconsistent with the ORC style
            if (availableStreams.contains(IN_DICTIONARY)) {
                if (availableStreams.contains(ROW_GROUP_DICTIONARY)) {
                    checkpoints.put(new StreamId(column, ROW_GROUP_DICTIONARY), new ByteArrayStreamCheckpoint(compressionKind, positionsList));
                }

                checkpoints.put(new StreamId(column, ROW_GROUP_DICTIONARY_LENGTH), new RowGroupDictionaryLengthStreamCheckpoint(compressionKind, positionsList));

                if (availableStreams.contains(DATA)) {
                    checkpoints.put(new StreamId(column, DATA), createLongStreamCheckpoint(encoding, compressionKind, positionsList));
                }

                checkpoints.put(new StreamId(column, IN_DICTIONARY), new BooleanStreamCheckpoint(compressionKind, positionsList));
            }
            else {
                if (availableStreams.contains(DATA)) {
                    checkpoints.put(new StreamId(column, DATA), createLongStreamCheckpoint(encoding, compressionKind, positionsList));
                }
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding for slice column: " + encoding);
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getListOrMapColumnCheckpoints(
            int column,
            ColumnEncodingKind encoding,
            CompressionKind compressionKind,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        if (availableStreams.contains(LENGTH)) {
            checkpoints.put(new StreamId(column, LENGTH), createLongStreamCheckpoint(encoding, compressionKind, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getStructColumnCheckpoints(
            int column,
            CompressionKind compressionKind,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getDecimalColumnCheckpoints(
            int column,
            ColumnEncodingKind encoding,
            CompressionKind compressionKind,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressionKind, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), new DecimalStreamCheckpoint(compressionKind, positionsList));
        }

        if (availableStreams.contains(SECONDARY)) {
            checkpoints.put(new StreamId(column, SECONDARY), createLongStreamCheckpoint(encoding, compressionKind, positionsList));
        }

        return checkpoints.build();
    }

    private static StreamCheckpoint createLongStreamCheckpoint(ColumnEncodingKind encoding, CompressionKind compressionKind, ColumnPositionsList positionsList)
    {
        if (encoding == DIRECT_V2 || encoding == DICTIONARY_V2) {
            return new LongStreamV2Checkpoint(compressionKind, positionsList);
        }

        if (encoding == DIRECT || encoding == DICTIONARY) {
            return new LongStreamV1Checkpoint(compressionKind, positionsList);
        }

        if (encoding == DWRF_DIRECT) {
            return new LongStreamDwrfCheckpoint(compressionKind, positionsList);
        }

        throw new IllegalArgumentException("Unsupported encoding for long stream: " + encoding);
    }

    public static class ColumnPositionsList
    {
        private final int column;
        private final OrcTypeKind columnType;
        private final List<Integer> positionsList;
        private int index;

        private ColumnPositionsList(int column, OrcTypeKind columnType, List<Integer> positionsList)
        {
            this.column = column;
            this.columnType = requireNonNull(columnType, "columnType is null");
            this.positionsList = ImmutableList.copyOf(requireNonNull(positionsList, "positionsList is null"));
        }

        public int getIndex()
        {
            return index;
        }

        public boolean hasNextPosition()
        {
            return index < positionsList.size();
        }

        public int nextPosition()
        {
            if (!hasNextPosition()) {
                throw new InvalidCheckpointException("Not enough positions for column %s, of type %s, checkpoints",
                        column,
                        columnType);
            }

            return positionsList.get(index++);
        }
    }
}
