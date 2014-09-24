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
import com.facebook.presto.hive.orc.metadata.ColumnEncoding;
import com.facebook.presto.hive.orc.metadata.CompressionKind;
import com.facebook.presto.hive.orc.metadata.MetadataReader;
import com.facebook.presto.hive.orc.metadata.RowGroupIndex;
import com.facebook.presto.hive.orc.metadata.Stream;
import com.facebook.presto.hive.orc.metadata.Stream.Kind;
import com.facebook.presto.hive.orc.metadata.StripeFooter;
import com.facebook.presto.hive.orc.metadata.StripeInformation;
import com.facebook.presto.hive.orc.metadata.Type;
import com.facebook.presto.hive.orc.stream.OrcInputStream;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.hive.orc.StripeSlice.getSlice;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.Kind.DICTIONARY;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.Kind.DICTIONARY_V2;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.Kind.DIRECT;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.Kind.DIRECT_V2;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.Kind.DWRF_DIRECT;
import static com.facebook.presto.hive.orc.metadata.CompressionKind.UNCOMPRESSED;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DICTIONARY_COUNT;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.DICTIONARY_DATA;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.IN_DICTIONARY;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.LENGTH;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.PRESENT;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.ROW_INDEX;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.SECONDARY;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.STRIDE_DICTIONARY;
import static com.facebook.presto.hive.orc.metadata.Stream.Kind.STRIDE_DICTIONARY_LENGTH;
import static com.facebook.presto.hive.orc.stream.OrcInputStream.BLOCK_HEADER_SIZE;

public class StripeReader
{
    private static final int BYTE_STREAM_POSITIONS = 1;
    private static final int INT_POSITIONS = BYTE_STREAM_POSITIONS + 1;
    private static final int STRIDE_DICTIONARY_LENGTH_POSITIONS = INT_POSITIONS + 1;
    private static final int RUN_LENGTH_BYTE_POSITIONS = BYTE_STREAM_POSITIONS + 1;
    private static final int BITFIELD_POSITIONS = RUN_LENGTH_BYTE_POSITIONS + 1;

    // for uncompressed streams, what is the most overlap with the following set
    // of rows (long vint literal group).
    private static final int WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;

    private final OrcDataSource orcDataSource;
    private final CompressionKind compressionKind;
    private final List<Type> types;
    private final int bufferSize;
    private final boolean[] includedStreams;
    private final long rowIndexStride;
    private final Map<HiveColumnHandle, Integer> columnHandleStreamIndex;
    private final TupleDomain<HiveColumnHandle> tupleDomain;
    private final MetadataReader metadataReader;
    private final TypeManager typeManager;

    public StripeReader(OrcDataSource orcDataSource,
            CompressionKind compressionKind,
            List<Type> types,
            int bufferSize,
            boolean[] includedStreams,
            long rowIndexStride,
            Map<HiveColumnHandle, Integer> columnHandleStreamIndex,
            TupleDomain<HiveColumnHandle> tupleDomain,
            MetadataReader metadataReader,
            TypeManager typeManager)
    {
        this.orcDataSource = orcDataSource;
        this.compressionKind = compressionKind;
        this.types = types;
        this.bufferSize = bufferSize;
        this.includedStreams = includedStreams;
        this.rowIndexStride = rowIndexStride;
        this.columnHandleStreamIndex = columnHandleStreamIndex;
        this.tupleDomain = tupleDomain;
        this.metadataReader = metadataReader;
        this.typeManager = typeManager;
    }

    public Stripe readStripe(StripeInformation stripe)
            throws IOException
    {
        // read the stripe footer
        StripeFooter stripeFooter = readStripeFooter(stripe);

        // read all included streams
        List<StripeSlice> stripeSlices = readDiskRanges(stripe.getOffset(), stripeFooter.getStreams());

        // read the column indexes
        List<List<RowGroupIndex>> columnIndexes = readColumnIndexes(stripeFooter.getStreams(), stripeSlices);

        // select the row groups matching the tuple domain
        boolean[] selectedRowGroups = selectRowGroups(stripe, columnIndexes);

        // if all row groups are skipped, return null
        if (!Booleans.contains(selectedRowGroups, true)) {
            return null;
        }

        // determine the dictionary stream locations
        List<StreamLayout> dictionaryStreamLayouts = getDictionaryStreams(stripeFooter.getStreams(), stripeFooter.getColumnEncodings());

        // determine the locations of the row groups
        List<RowGroupLayout> rowGroupLayouts = getRowGroupRanges(
                stripe.getNumberOfRows(),
                stripeFooter.getStreams(),
                columnIndexes,
                selectedRowGroups,
                stripeFooter.getColumnEncodings());

        StripeLayout stripeLayout = new StripeLayout(stripe.getNumberOfRows(), stripeFooter.getColumnEncodings(), dictionaryStreamLayouts, rowGroupLayouts);

        return stripeLayout.createStripe(stripeSlices, bufferSize);
    }

    public StripeFooter readStripeFooter(StripeInformation stripe)
            throws IOException
    {
        long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
        int tailLength = Ints.checkedCast(stripe.getFooterLength());

        // read the footer
        byte[] tailBuf = new byte[tailLength];
        orcDataSource.readFully(offset, tailBuf);
        InputStream inputStream = new OrcInputStream(Slices.wrappedBuffer(tailBuf).getInput(), compressionKind, bufferSize);
        return metadataReader.readStripeFooter(types, inputStream);
    }

    private List<List<RowGroupIndex>> readColumnIndexes(List<Stream> streams, List<StripeSlice> stripeSlices)
            throws IOException
    {
        List<List<RowGroupIndex>> indexes = new ArrayList<>();
        indexes.addAll(Collections.<List<RowGroupIndex>>nCopies(types.size(), null));

        long offset = 0;
        for (Stream stream : streams) {
            int column = stream.getColumn();
            if (includedStreams[column] && stream.getKind() == ROW_INDEX) {
                Slice slice = getSlice(new DiskRange(offset, stream.getLength()), stripeSlices);
                indexes.set(column, metadataReader.readRowIndexes(new OrcInputStream(slice.getInput(), compressionKind, bufferSize)));
            }
            offset += stream.getLength();
        }
        return indexes;
    }

    private boolean[] selectRowGroups(StripeInformation stripe, List<List<RowGroupIndex>> columnIndexes)
            throws IOException
    {
        long rowsInStripe = stripe.getNumberOfRows();
        int groupsInStripe = (int) ((rowsInStripe + rowIndexStride - 1) / rowIndexStride);

        boolean[] selectedRowGroups = new boolean[groupsInStripe];
        long rows = rowsInStripe;
        for (int rowGroup = 0; rowGroup < selectedRowGroups.length; ++rowGroup) {
            long rowsInStride = Math.min(rows, rowIndexStride);
            TupleDomain<HiveColumnHandle> rowGroupTupleDomain = OrcDomainExtractor.extractDomain(typeManager, columnHandleStreamIndex, columnIndexes, rowGroup, rowsInStride);
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
            int streamLength = stream.getLength();
            streamDiskRanges.add(new DiskRange(stripeOffset, streamLength));
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
            StreamLayout streamLayout = new StreamLayout(
                    new StreamId(stream),
                    0,
                    types.get(column).getKind(),
                    encoding.getKind(),
                    stream.isUseVInts(),
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
            List<List<RowGroupIndex>> columnIndexes,
            boolean[] selectedRowGroups,
            List<ColumnEncoding> encodings)
    {
        // determine the range of every stream
        long stripeOffset = 0;
        List<DiskRange> streamDiskRanges = new ArrayList<>();
        for (Stream stream : streams) {
            int streamLength = stream.getLength();
            streamDiskRanges.add(new DiskRange(stripeOffset, streamLength));
            stripeOffset += streamLength;
        }

        boolean[] hasNull = new boolean[types.size()];
        for (Stream stream : streams) {
            if (stream.getKind() == PRESENT) {
                hasNull[stream.getColumn()] = true;
            }
        }

        ImmutableSetMultimap.Builder<Integer, Stream.Kind> streamKindsBuilder = ImmutableSetMultimap.builder();
        for (Stream stream : streams) {
            if (stream.getLength() > 0) {
                streamKindsBuilder.put(stream.getColumn(), stream.getKind());
            }
        }
        SetMultimap<Integer, Kind> streamKinds = streamKindsBuilder.build();

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
                if (!includedStreams[column] ||
                        stream.getLength() == 0 ||
                        isIndexStream(streamKind) ||
                        isDictionary(streamKind, encodings.get(column))) {
                    continue;
                }

                StreamId streamId = new StreamId(stream);
                ColumnEncoding.Kind encoding = encodings.get(column).getKind();
                Set<Kind> availableStreams = streamKinds.get(column);

                DiskRange diskRange = getRowGroupStreamDiskRange(columnIndexes.get(column), streamDiskRanges.get(i), groupId, streamId, encoding, availableStreams);

                List<Long> offsetPositions = getOffsetPositions(streamId, encoding, availableStreams, columnIndexes.get(column).get(groupId).getPositions());
                StreamLayout streamLayout = new StreamLayout(
                        streamId,
                        groupId,
                        types.get(column).getKind(),
                        encoding,
                        stream.isUseVInts(),
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

    private DiskRange getRowGroupStreamDiskRange(
            List<RowGroupIndex> indexes,
            DiskRange streamDiskRange,
            int groupId,
            StreamId streamId,
            ColumnEncoding.Kind encoding,
            Set<Kind> availableStreams)
    {
        long start = streamDiskRange.getOffset() + getOffsetPositions(streamId, encoding, availableStreams, indexes.get(groupId).getPositions()).get(0);

        long end;
        if (groupId == indexes.size() - 1) {
            end = streamDiskRange.getEnd();
        }
        else {
            end = streamDiskRange.getOffset() + getOffsetPositions(streamId, encoding, availableStreams, indexes.get(groupId + 1).getPositions()).get(0);

            // for an inner group, we need to add some "slop" to the length
            // since the last value may be in a compressed block or encoded sequence
            // shared with the next row group
            if (compressionKind != UNCOMPRESSED) {
                // add 2 buffers to safely accommodate the next compression block.
                end += 2 * (BLOCK_HEADER_SIZE + bufferSize);
            }
            else {
                // add worst case size of an encoded value sequence
                end += WORST_UNCOMPRESSED_SLOP;
            }

            end = Math.min(streamDiskRange.getEnd(), end);
        }

        return new DiskRange(start, Ints.checkedCast(end - start));
    }

    private List<StripeSlice> readDiskRanges(long base, List<Stream> streams)
            throws IOException
    {
        long stripeOffset = 0;
        List<DiskRange> diskRanges = new ArrayList<>();
        for (Stream stream : streams) {
            int column = stream.getColumn();
            int streamLength = stream.getLength();
            if (includedStreams[column]) {
                diskRanges.add(new DiskRange(stripeOffset, streamLength));
            }
            stripeOffset += streamLength;
        }

        return orcDataSource.readFully(base, diskRanges);
    }

    private List<Long> getOffsetPositions(
            StreamId streamId,
            ColumnEncoding.Kind columnEncoding,
            Set<Kind> availableStreams,
            List<Long> positionsList)
    {
        Type.Kind type = types.get(streamId.getColumn()).getKind();
        int compressionOffsets = compressionKind != UNCOMPRESSED ? 1 : 0;

        // if this is the present stream the offset is in position 1
        List<Long> offsetPositions = positionsList;
        if (streamId.getKind() == PRESENT) {
            return offsetPositions.subList(0, BITFIELD_POSITIONS + compressionOffsets);
        }

        // If there is a present stream, remove offset used by the bit field stream
        if (availableStreams.contains(PRESENT)) {
            offsetPositions = offsetPositions.subList(BITFIELD_POSITIONS + compressionOffsets, offsetPositions.size());
        }

        if (streamId.getKind() == DATA) {
            switch (type) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG: {
                    // start after the in-dictionary stream if present
                    int start;
                    if (columnEncoding == DICTIONARY && availableStreams.contains(IN_DICTIONARY)) {
                        start = BITFIELD_POSITIONS + compressionOffsets;
                    }
                    else {
                        start = 0;
                    }
                    int end;
                    if (columnEncoding == DWRF_DIRECT) {
                        end = start + BYTE_STREAM_POSITIONS + compressionOffsets;
                    }
                    else {
                        end = start + INT_POSITIONS + compressionOffsets;
                    }
                    return offsetPositions.subList(start, end);
                }
                case BOOLEAN:
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
                    if (columnEncoding == DIRECT || columnEncoding == DIRECT_V2 || columnEncoding == DWRF_DIRECT) {
                        return offsetPositions.subList(0, BYTE_STREAM_POSITIONS + compressionOffsets);
                    }
                    else if (columnEncoding == DICTIONARY || columnEncoding == DICTIONARY_V2) {
                        // if there is a stride dictionary, start after dictionary data and dictionary lengths
                        int start;
                        if (availableStreams.contains(STRIDE_DICTIONARY)) {
                            start = (BYTE_STREAM_POSITIONS + compressionOffsets) +
                                    (STRIDE_DICTIONARY_LENGTH_POSITIONS + compressionOffsets);
                        }
                        else {
                            start = 0;
                        }
                        int end = start + INT_POSITIONS + compressionOffsets;
                        return offsetPositions.subList(start, end);
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported encoding " + columnEncoding);
                    }
                case TIMESTAMP:
                    return offsetPositions.subList(0, INT_POSITIONS + compressionOffsets);
                default:
                    throw new IllegalArgumentException("Unknown type " + type);
            }
        }

        if (streamId.getKind() == LENGTH) {
            switch (type) {
                case STRING:
                case BINARY:
                    if (columnEncoding == DIRECT || columnEncoding == DIRECT_V2 || columnEncoding == DWRF_DIRECT) {
                        int start = BYTE_STREAM_POSITIONS + compressionOffsets;
                        int end = start + INT_POSITIONS + compressionOffsets;
                        return offsetPositions.subList(start, end);
                    }
                    else if (columnEncoding == DICTIONARY || columnEncoding == DICTIONARY_V2) {
                        int start = 0;
                        // if there is a data stream, then the we need to skip the byte stream positions consumed
                        if (availableStreams.contains(DICTIONARY_DATA)) {
                            start = BYTE_STREAM_POSITIONS + compressionOffsets;
                        }
                        int end = start + INT_POSITIONS + compressionOffsets;
                        return offsetPositions.subList(start, end);
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported encoding " + columnEncoding);
                    }
                case MAP:
                case LIST:
                    return offsetPositions;
            }
        }

        // length (nanos) of a timestamp column
        if (streamId.getKind() == SECONDARY && type == Type.Kind.TIMESTAMP) {
            int start = INT_POSITIONS + compressionOffsets;
            int end = start + INT_POSITIONS + compressionOffsets;
            return offsetPositions.subList(start, end);
        }

        if (streamId.getKind() == STRIDE_DICTIONARY) {
            switch (type) {
                case STRING:
                case BINARY:
                    // stride dictionary is first
                    int start = 0;
                    int end = start + BYTE_STREAM_POSITIONS + compressionOffsets;
                    return offsetPositions.subList(start, end);
            }
        }

        if (streamId.getKind() == STRIDE_DICTIONARY_LENGTH) {
            switch (type) {
                case STRING:
                case BINARY:
                    // start after stride dictionary
                    int start = BYTE_STREAM_POSITIONS + compressionOffsets;
                    int end = start + STRIDE_DICTIONARY_LENGTH_POSITIONS + compressionOffsets;
                    return offsetPositions.subList(start, end);
            }
        }

        if (streamId.getKind() == IN_DICTIONARY) {
            switch (type) {
                case BYTE:
                case SHORT:
                case INT:
                case LONG: {
                    // in dictionary is first for integer streams
                    int start = 0;
                    int end = start + BITFIELD_POSITIONS + compressionOffsets;
                    return offsetPositions.subList(start, end);
                }
                case STRING:
                case BINARY: {
                    // start stride dictionary, stride dictionary lengths and dictionary keys (data)
                    int start = (BYTE_STREAM_POSITIONS + compressionOffsets) +
                            (STRIDE_DICTIONARY_LENGTH_POSITIONS + compressionOffsets) +
                            (INT_POSITIONS + compressionOffsets);
                    int end = start + BITFIELD_POSITIONS + compressionOffsets;
                    return offsetPositions.subList(start, end);
                }
            }
        }

        throw new IllegalArgumentException("Unsupported column type " + type + " for stream " + streamId);
    }

    private static boolean isIndexStream(Stream.Kind streamKind)
    {
        return streamKind == ROW_INDEX || streamKind == DICTIONARY_COUNT;
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
