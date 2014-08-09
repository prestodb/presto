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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.orc.stream.OrcInputStream;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.StripeFooter;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.orc.stream.OrcInputStream.BLOCK_HEADER_SIZE;
import static com.facebook.presto.hive.orc.StreamLayout.diskRangeGetter;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DICTIONARY;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DICTIONARY_V2;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DIRECT;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind.DIRECT_V2;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind.NONE;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.DICTIONARY_DATA;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.LENGTH;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.PRESENT;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.ROW_INDEX;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.Stream.Kind.SECONDARY;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.StripeInformation;

public class StripeReader
{
    private static final int BYTE_STREAM_POSITIONS = 1;
    private static final int RUN_LENGTH_INT_POSITIONS = BYTE_STREAM_POSITIONS + 1;
    private static final int RUN_LENGTH_BYTE_POSITIONS = BYTE_STREAM_POSITIONS + 1;
    private static final int BITFIELD_POSITIONS = RUN_LENGTH_BYTE_POSITIONS + 1;

    // for uncompressed streams, what is the most overlap with the following set
    // of rows (long vint literal group).
    private static final int WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;

    private final FSDataInputStream file;
    private final CompressionKind compressionKind;
    private final List<Type> types;
    private final int bufferSize;
    private final boolean[] includedStreams;
    private final long rowIndexStride;
    private final Map<HiveColumnHandle, Integer> columnHandleStreamIndex;
    private final TupleDomain<HiveColumnHandle> tupleDomain;

    public StripeReader(FSDataInputStream file,
            CompressionKind compressionKind,
            List<Type> types,
            int bufferSize,
            boolean[] includedStreams,
            long rowIndexStride,
            Map<HiveColumnHandle, Integer> columnHandleStreamIndex,
            TupleDomain<HiveColumnHandle> tupleDomain)
    {
        this.file = file;
        this.compressionKind = compressionKind;
        this.types = types;
        this.bufferSize = bufferSize;
        this.includedStreams = includedStreams;
        this.rowIndexStride = rowIndexStride;
        this.columnHandleStreamIndex = columnHandleStreamIndex;
        this.tupleDomain = tupleDomain;
    }

    public Stripe readStripe(StripeInformation stripe)
            throws IOException
    {
        StripeLayout stripeLayout = readStripeMetadata(stripe);
        if (stripeLayout == null) {
            return null;
        }

        // read the stripe data
        ImmutableList.Builder<StreamLayout> streamLayouts = ImmutableList.builder();
        streamLayouts.addAll(stripeLayout.getDictionaryStreamLayouts());
        for (RowGroupLayout rowGroupLayout : stripeLayout.getRowGroupLayouts()) {
            streamLayouts.addAll(rowGroupLayout.getStreamLayouts());
        }
        List<StripeSlice> stripeSlices = readDiskRanges(stripe.getOffset(), streamLayouts.build());

        return stripeLayout.createStripe(stripeSlices, bufferSize);
    }

    public StripeLayout readStripeMetadata(StripeInformation stripe)
            throws IOException
    {
        // read the stripe footer
        StripeFooter stripeFooter = readStripeFooter(stripe);

        // read the row indexes
        RowIndex[] indexes = readRowIndex(stripe, stripeFooter);

        // select the row groups matching the tuple domain
        boolean[] selectedRowGroups = selectRowGroups(stripe, indexes);

        // if all row groups are skipped, return null
        if (!Booleans.contains(selectedRowGroups, true)) {
            return null;
        }

        // determine the dictionary stream locations
        List<StreamLayout> dictionaryStreamLayouts = getDictionaryStreams(stripeFooter.getStreamsList(), stripeFooter.getColumnsList());

        // determine the locations of the row groups
        List<RowGroupLayout> rowGroupLayouts = getRowGroupRanges(
                stripe.getNumberOfRows(),
                stripeFooter.getStreamsList(),
                indexes,
                selectedRowGroups,
                stripeFooter.getColumnsList());

        // merge row groups
        rowGroupLayouts = RowGroupLayout.mergeAdjacentRowGroups(rowGroupLayouts);

        return new StripeLayout(stripe.getNumberOfRows(), stripeFooter.getColumnsList(), dictionaryStreamLayouts, rowGroupLayouts);
    }

    private StripeFooter readStripeFooter(StripeInformation stripe)
            throws IOException
    {
        long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
        int tailLength = Ints.checkedCast(stripe.getFooterLength());

        // read the footer
        byte[] tailBuf = new byte[tailLength];
        file.readFully(offset, tailBuf);
        InputStream inputStream = new OrcInputStream(Slices.wrappedBuffer(tailBuf).getInput(), compressionKind, bufferSize);
        return StripeFooter.parseFrom(inputStream);
    }

    private RowIndex[] readRowIndex(StripeInformation stripe, StripeFooter stripeFooter)
            throws IOException
    {
        RowIndex[] indexes = new RowIndex[types.size()];

        long offset = stripe.getOffset();
        for (Stream stream : stripeFooter.getStreamsList()) {
            if (includedStreams[stream.getColumn()] && stream.getKind() == ROW_INDEX) {
                byte[] buffer = new byte[(int) stream.getLength()];
                file.readFully(offset, buffer);
                indexes[stream.getColumn()] = RowIndex.parseFrom(new OrcInputStream(Slices.wrappedBuffer(buffer).getInput(), compressionKind, bufferSize));
            }
            offset += stream.getLength();
        }
        return indexes;
    }

    private boolean[] selectRowGroups(StripeInformation stripe, RowIndex[] indexes)
            throws IOException
    {
        long rowsInStripe = stripe.getNumberOfRows();
        int groupsInStripe = (int) ((rowsInStripe + rowIndexStride - 1) / rowIndexStride);

        boolean[] selectedRowGroups = new boolean[groupsInStripe];
        long rows = rowsInStripe;
        for (int rowGroup = 0; rowGroup < selectedRowGroups.length; ++rowGroup) {
            long rowsInStride = Math.min(rows, rowIndexStride);
            TupleDomain<HiveColumnHandle> rowGroupTupleDomain = OrcDomainExtractor.extractDomain(columnHandleStreamIndex, indexes, rowGroup, rowsInStride);
            selectedRowGroups[rowGroup] = tupleDomain.overlaps(rowGroupTupleDomain);
            rows -= rowsInStride;
        }
        return selectedRowGroups;
    }

    private List<StreamLayout> getDictionaryStreams(List<Stream> streams, List<ColumnEncoding> encodings)
    {
        // determine the range of every stream
        List<DiskRange> streamDiskRanges = new ArrayList<>();
        long stripeOffset = 0;
        for (Stream stream : streams) {
            long streamLength = stream.getLength();
            streamDiskRanges.add(new DiskRange(stripeOffset, stripeOffset + streamLength));
            stripeOffset += streamLength;
        }

        // Determine stream ranges for dictionary data
        ImmutableList.Builder<StreamLayout> streamLayouts = ImmutableList.builder();
        for (int i = 0; i < streams.size(); i++) {
            Stream stream = streams.get(i);
            Stream.Kind streamKind = stream.getKind();
            int column = stream.getColumn();

            // only streams for included columns, in the data area, that are not dictionaries
            ColumnEncoding encoding = encodings.get(column);
            if (!includedStreams[column] || isIndexStream(streamKind) || !isDictionary(streamKind, encoding)) {
                continue;
            }

            DiskRange streamDiskRange = streamDiskRanges.get(i);
            StreamLayout streamLayout = new StreamLayout(new StreamId(stream),
                    0,
                    types.get(column).getKind(),
                    encoding.getKind(),
                    compressionKind,
                    streamDiskRange,
                    ImmutableList.<Integer>of());
            streamLayouts.add(streamLayout);
        }

        return streamLayouts.build();
    }

    private List<RowGroupLayout> getRowGroupRanges(
            long rowsInStripe,
            List<Stream> streams,
            RowIndex[] indexes,
            boolean[] selectedRowGroups,
            List<ColumnEncoding> encodings)
    {
        // determine the range of every stream
        long stripeOffset = 0;
        List<DiskRange> streamDiskRanges = new ArrayList<>();
        for (Stream stream : streams) {
            long streamLength = stream.getLength();
            streamDiskRanges.add(new DiskRange(stripeOffset, stripeOffset + streamLength));
            stripeOffset += streamLength;
        }

        boolean[] hasNull = new boolean[types.size()];
        for (Stream stream : streams) {
            if (stream.getKind() == PRESENT) {
                hasNull[stream.getColumn()] = true;
            }
        }

        // Determine stream ranges for selected row groups
        ImmutableList.Builder<RowGroupLayout> rowGroupLayouts = ImmutableList.builder();

        for (int groupId = 0; groupId < selectedRowGroups.length; ++groupId) {
            if (!selectedRowGroups[groupId]) {
                continue;
            }

            ImmutableList.Builder<StreamLayout> streamLayouts = ImmutableList.builder();
            for (int i = 0; i < streams.size(); i++) {
                Stream stream = streams.get(i);
                int column = stream.getColumn();
                Stream.Kind streamKind = stream.getKind();

                // only streams for included columns, in the data area, that are not dictionaries
                if (!includedStreams[column] || isIndexStream(streamKind) || isDictionary(streamKind, encodings.get(column))) {
                    continue;
                }

                StreamId streamId = new StreamId(stream);
                ColumnEncoding.Kind encoding = encodings.get(column).getKind();

                DiskRange diskRange = getRowGroupStreamDiskRange(indexes[column], streamDiskRanges.get(i), hasNull[column], groupId, streamId, encoding);

                List<Long> offsetPositions = getOffsetPositions(streamId, encoding, hasNull[column], indexes[column].getEntry(groupId).getPositionsList());
                StreamLayout streamLayout = new StreamLayout(
                        streamId,
                        groupId,
                        types.get(column).getKind(),
                        encoding,
                        compressionKind,
                        diskRange,
                        checkedCastToInteger(offsetPositions.subList(1, offsetPositions.size())));
                streamLayouts.add(streamLayout);
            }

            long rowsInGroup = Math.min(rowsInStripe - (groupId * rowIndexStride), rowIndexStride);
            rowGroupLayouts.add(new RowGroupLayout(groupId, rowsInGroup, streamLayouts.build()));
        }

        return rowGroupLayouts.build();
    }

    private DiskRange getRowGroupStreamDiskRange(RowIndex index, DiskRange streamDiskRange, boolean hasNulls, int groupId, StreamId streamId, ColumnEncoding.Kind encoding)
    {
        long start = streamDiskRange.getOffset() + getOffsetPositions(streamId, encoding, hasNulls, index.getEntry(groupId).getPositionsList()).get(0);

        long end;
        if (groupId == index.getEntryCount() - 1) {
            end = streamDiskRange.getEnd();
        }
        else {
            end = streamDiskRange.getOffset() + getOffsetPositions(streamId, encoding, hasNulls, index.getEntry(groupId + 1).getPositionsList()).get(0);

            // for an inner group, we need to add some "slop" to the length
            // since the last value may be in a compressed block or encoded sequence
            // shared with the next row group
            if (compressionKind != NONE) {
                // add 2 buffers to safely accommodate the next compression block.
                end += 2 * (BLOCK_HEADER_SIZE + bufferSize);
            }
            else {
                // add worst case size of an encoded value sequence
                end += WORST_UNCOMPRESSED_SLOP;
            }

            end = Math.min(streamDiskRange.getEnd(), end);
        }

        return new DiskRange(start, end);
    }

    private List<StripeSlice> readDiskRanges(
            long base,
            ImmutableList<StreamLayout> streamLayouts)
            throws IOException
    {
        List<DiskRange> diskRanges = DiskRange.mergeAdjacentDiskRanges(ImmutableList.copyOf(Iterables.transform(streamLayouts, diskRangeGetter())));

        ArrayList<StripeSlice> result = new ArrayList<>(streamLayouts.size());
        for (DiskRange diskRange : diskRanges) {
            long offset = diskRange.getOffset();
            int length = (int) (diskRange.getEnd() - offset);

            byte[] buffer = new byte[length];
            long readPosition = base + offset;
            file.readFully(readPosition, buffer);
            result.add(new StripeSlice(Slices.wrappedBuffer(buffer), offset));
        }
        return result;
    }

    private List<Long> getOffsetPositions(
            StreamId streamId,
            ColumnEncoding.Kind columnEncoding,
            boolean hasNulls,
            List<Long> positionsList)
    {
        Type.Kind type = types.get(streamId.getColumn()).getKind();
        int compressionOffsets = compressionKind != NONE ? 1 : 0;

        // if this is the present stream the offset is in position 1
        List<Long> offsetPositions = positionsList;
        if (streamId.getKind() == PRESENT) {
            return offsetPositions.subList(0, BITFIELD_POSITIONS + compressionOffsets);
        }

        // if this column has nulls, remove offset used by the PRESENT stream
        if (hasNulls) {
            offsetPositions = offsetPositions.subList(BITFIELD_POSITIONS + compressionOffsets, offsetPositions.size());
        }

        if (streamId.getKind() == DATA) {
            switch (type) {
                case BOOLEAN:
                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case DATE:
                case STRUCT:
                case MAP:
                case LIST:
                case UNION:
                    // these types should not have any trailing offsets
                    return offsetPositions;
                case STRING:
                case BINARY:
                    if (columnEncoding == DIRECT || columnEncoding == DIRECT_V2) {
                        return offsetPositions.subList(0, BYTE_STREAM_POSITIONS + compressionOffsets);
                    }
                    else if (columnEncoding == DICTIONARY || columnEncoding == DICTIONARY_V2) {
                        return offsetPositions.subList(0, RUN_LENGTH_INT_POSITIONS + compressionOffsets);
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported encoding " + columnEncoding);
                    }
                case TIMESTAMP:
                    return offsetPositions.subList(0, RUN_LENGTH_INT_POSITIONS + compressionOffsets);
                default:
                    throw new IllegalArgumentException("Unknown type " + type);
            }
        }

        if (streamId.getKind() == LENGTH) {
            switch (type) {
                case STRING:
                case BINARY:
                    int start = BYTE_STREAM_POSITIONS + compressionOffsets;
                    int end = start + RUN_LENGTH_INT_POSITIONS + compressionOffsets;
                    return offsetPositions.subList(start, end);
                case MAP:
                case LIST:
                    return offsetPositions;
            }
        }

        // length (nanos) of a timestamp column
        if (streamId.getKind() == SECONDARY && type == Type.Kind.TIMESTAMP) {
            int start = RUN_LENGTH_INT_POSITIONS + compressionOffsets;
            int end = start + RUN_LENGTH_INT_POSITIONS + compressionOffsets;
            return offsetPositions.subList(start, end);
        }

        throw new IllegalArgumentException("Unsupported column type " + type + " for stream " + streamId);
    }

    private static boolean isIndexStream(Kind streamKind)
    {
        return streamKind == ROW_INDEX || streamKind == Kind.DICTIONARY_COUNT;
    }

    private static boolean isDictionary(Stream.Kind kind, ColumnEncoding encoding)
    {
        ColumnEncoding.Kind encodingKind = encoding.getKind();
        return kind == DICTIONARY_DATA || (kind == LENGTH && (encodingKind == DICTIONARY || encodingKind == DICTIONARY_V2));
    }

    public static ImmutableList<Integer> checkedCastToInteger(List<Long> offsetPositions)
    {
        return ImmutableList.copyOf(Iterables.transform(offsetPositions, new Function<Long, Integer>()
        {
            @Nullable
            @Override
            public Integer apply(Long input)
            {
                return Ints.checkedCast(input);
            }
        }));
    }
}
