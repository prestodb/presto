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
package com.facebook.presto.orc.metadata;

import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.statistics.BinaryStatistics;
import com.facebook.presto.orc.metadata.statistics.BooleanStatistics;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DoubleStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.CodedInputStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.CompressionKind.LZ4;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.STATIC_METADATA;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.byteStringToSlice;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.maxStringTruncateToValidRange;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.minStringTruncateToValidRange;
import static com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion.ORC_HIVE_8732;
import static com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion.ORIGINAL;
import static com.facebook.presto.orc.metadata.statistics.BinaryStatistics.BINARY_VALUE_BYTES_OVERHEAD;
import static com.facebook.presto.orc.metadata.statistics.BooleanStatistics.BOOLEAN_VALUE_BYTES;
import static com.facebook.presto.orc.metadata.statistics.DoubleStatistics.DOUBLE_VALUE_BYTES;
import static com.facebook.presto.orc.metadata.statistics.IntegerStatistics.INTEGER_VALUE_BYTES;
import static com.facebook.presto.orc.metadata.statistics.StringStatistics.STRING_VALUE_BYTES_OVERHEAD;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;

public class DwrfMetadataReader
        implements MetadataReader
{
    @Override
    public PostScript readPostScript(byte[] data, int offset, int length)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(data, offset, length);
        DwrfProto.PostScript postScript = DwrfProto.PostScript.parseFrom(input);

        HiveWriterVersion writerVersion = postScript.hasWriterVersion() && postScript.getWriterVersion() > 0 ? ORC_HIVE_8732 : ORIGINAL;

        return new PostScript(
                ImmutableList.of(),
                postScript.getFooterLength(),
                0,
                toCompression(postScript.getCompression()),
                postScript.getCompressionBlockSize(),
                writerVersion);
    }

    @Override
    public Metadata readMetadata(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
    {
        return new Metadata(ImmutableList.of());
    }

    @Override
    public Footer readFooter(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        DwrfProto.Footer footer = DwrfProto.Footer.parseFrom(input);

        // todo enable file stats when DWRF team verifies that the stats are correct
        // List<ColumnStatistics> fileStats = toColumnStatistics(hiveWriterVersion, footer.getStatisticsList(), false);
        List<ColumnStatistics> fileStats = ImmutableList.of();

        return new Footer(
                footer.getNumberOfRows(),
                footer.getRowIndexStride(),
                toStripeInformation(footer.getStripesList()),
                toType(footer.getTypesList()),
                fileStats,
                toUserMetadata(footer.getMetadataList()));
    }

    private static List<StripeInformation> toStripeInformation(List<DwrfProto.StripeInformation> types)
    {
        return ImmutableList.copyOf(Iterables.transform(types, DwrfMetadataReader::toStripeInformation));
    }

    private static StripeInformation toStripeInformation(DwrfProto.StripeInformation stripeInformation)
    {
        return new StripeInformation(
                toIntExact(stripeInformation.getNumberOfRows()),
                stripeInformation.getOffset(),
                stripeInformation.getIndexLength(),
                stripeInformation.getDataLength(),
                stripeInformation.getFooterLength());
    }

    @Override
    public StripeFooter readStripeFooter(List<OrcType> types, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        DwrfProto.StripeFooter stripeFooter = DwrfProto.StripeFooter.parseFrom(input);
        return new StripeFooter(toStream(stripeFooter.getStreamsList()), toColumnEncoding(types, stripeFooter.getColumnsList()));
    }

    private static Stream toStream(DwrfProto.Stream stream)
    {
        return new Stream(stream.getColumn(), toStreamKind(stream.getKind()), toIntExact(stream.getLength()), stream.getUseVInts(), stream.getSequence());
    }

    private static List<Stream> toStream(List<DwrfProto.Stream> streams)
    {
        return ImmutableList.copyOf(Iterables.transform(streams, DwrfMetadataReader::toStream));
    }

    private static DwrfSequenceEncoding toSequenceEncoding(OrcType type, DwrfProto.ColumnEncoding columnEncoding)
    {
        return new DwrfSequenceEncoding(
                columnEncoding.getKey(),
                new ColumnEncoding(
                        toColumnEncodingKind(type.getOrcTypeKind(), columnEncoding.getKind()),
                        columnEncoding.getDictionarySize()));
    }

    private static Optional<List<DwrfSequenceEncoding>> toAdditionalSequenceEncodings(List<DwrfProto.ColumnEncoding> columnEncodings, OrcType type)
    {
        if (columnEncodings.size() == 1) {
            return Optional.empty();
        }

        ImmutableList.Builder<DwrfSequenceEncoding> additionalSequenceEncodings = ImmutableList.builder();

        for (int i = 1; i < columnEncodings.size(); i++) {
            additionalSequenceEncodings.add(toSequenceEncoding(type, columnEncodings.get(i)));
        }

        return Optional.of(additionalSequenceEncodings.build());
    }

    private static List<ColumnEncoding> toColumnEncoding(List<OrcType> types, List<DwrfProto.ColumnEncoding> columnEncodings)
    {
        Map<Integer, List<DwrfProto.ColumnEncoding>> groupedColumnEncodings = new HashMap<>(columnEncodings.size());

        for (int i = 0; i < columnEncodings.size(); i++) {
            DwrfProto.ColumnEncoding columnEncoding = columnEncodings.get(i);
            int column = columnEncoding.getColumn();

            // DWRF prior to version 6.0.8 doesn't set the value of column, infer it from the index
            if (!columnEncoding.hasColumn()) {
                column = i;
            }

            groupedColumnEncodings.computeIfAbsent(column, key -> new ArrayList<>()).add(columnEncoding);
        }

        ImmutableList.Builder<ColumnEncoding> resultBuilder = ImmutableList.builder();

        for (Map.Entry<Integer, List<DwrfProto.ColumnEncoding>> entry : groupedColumnEncodings.entrySet()) {
            OrcType type = types.get(entry.getKey());

            DwrfProto.ColumnEncoding columnEncoding = entry.getValue().get(0);
            resultBuilder.add(
                    new ColumnEncoding(
                            toColumnEncodingKind(type.getOrcTypeKind(), columnEncoding.getKind()),
                            columnEncoding.getDictionarySize(),
                            toAdditionalSequenceEncodings(entry.getValue(), type)));
        }

        return resultBuilder.build();
    }

    @Override
    public List<RowGroupIndex> readRowIndexes(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        DwrfProto.RowIndex rowIndex = DwrfProto.RowIndex.parseFrom(input);
        return ImmutableList.copyOf(Iterables.transform(rowIndex.getEntryList(), rowIndexEntry -> toRowGroupIndex(hiveWriterVersion, rowIndexEntry)));
    }

    @Override
    public List<HiveBloomFilter> readBloomFilterIndexes(InputStream inputStream)
    {
        // DWRF does not have bloom filters
        return ImmutableList.of();
    }

    private static RowGroupIndex toRowGroupIndex(HiveWriterVersion hiveWriterVersion, DwrfProto.RowIndexEntry rowIndexEntry)
    {
        List<Long> positionsList = rowIndexEntry.getPositionsList();
        ImmutableList.Builder<Integer> positions = ImmutableList.builder();
        for (int index = 0; index < positionsList.size(); index++) {
            long longPosition = positionsList.get(index);
            int intPosition = (int) longPosition;

            checkState(intPosition == longPosition, "Expected checkpoint position %s, to be an integer", index);

            positions.add(intPosition);
        }
        return new RowGroupIndex(positions.build(), toColumnStatistics(hiveWriterVersion, rowIndexEntry.getStatistics(), true));
    }

    private static List<ColumnStatistics> toColumnStatistics(HiveWriterVersion hiveWriterVersion, List<DwrfProto.ColumnStatistics> columnStatistics, boolean isRowGroup)
    {
        if (columnStatistics == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(Iterables.transform(columnStatistics, statistics -> toColumnStatistics(hiveWriterVersion, statistics, isRowGroup)));
    }

    private Map<String, Slice> toUserMetadata(List<DwrfProto.UserMetadataItem> metadataList)
    {
        ImmutableMap.Builder<String, Slice> mapBuilder = ImmutableMap.builder();
        for (DwrfProto.UserMetadataItem item : metadataList) {
            // skip static metadata added by the writer framework
            if (!STATIC_METADATA.containsKey(item.getName())) {
                mapBuilder.put(item.getName(), byteStringToSlice(item.getValue()));
            }
        }
        return mapBuilder.build();
    }

    private static ColumnStatistics toColumnStatistics(HiveWriterVersion hiveWriterVersion, DwrfProto.ColumnStatistics statistics, boolean isRowGroup)
    {
        long minAverageValueBytes;
        if (statistics.hasBucketStatistics()) {
            minAverageValueBytes = BOOLEAN_VALUE_BYTES;
        }
        else if (statistics.hasIntStatistics()) {
            minAverageValueBytes = INTEGER_VALUE_BYTES;
        }
        else if (statistics.hasDoubleStatistics()) {
            minAverageValueBytes = DOUBLE_VALUE_BYTES;
        }
        else if (statistics.hasStringStatistics()) {
            minAverageValueBytes = STRING_VALUE_BYTES_OVERHEAD;
            if (statistics.hasNumberOfValues() && statistics.getNumberOfValues() > 0) {
                minAverageValueBytes += statistics.getStringStatistics().getSum() / statistics.getNumberOfValues();
            }
        }
        else if (statistics.hasBinaryStatistics()) {
            // offset and value length
            minAverageValueBytes = BINARY_VALUE_BYTES_OVERHEAD;
            if (statistics.hasNumberOfValues() && statistics.getNumberOfValues() > 0) {
                minAverageValueBytes += statistics.getBinaryStatistics().getSum() / statistics.getNumberOfValues();
            }
        }
        else {
            minAverageValueBytes = 0;
        }

        return new ColumnStatistics(
                statistics.getNumberOfValues(),
                minAverageValueBytes,
                statistics.hasBucketStatistics() ? toBooleanStatistics(statistics.getBucketStatistics()) : null,
                statistics.hasIntStatistics() ? toIntegerStatistics(statistics.getIntStatistics()) : null,
                statistics.hasDoubleStatistics() ? toDoubleStatistics(statistics.getDoubleStatistics()) : null,
                statistics.hasStringStatistics() ? toStringStatistics(hiveWriterVersion, statistics.getStringStatistics(), isRowGroup) : null,
                null,
                null,
                statistics.hasBinaryStatistics() ? toBinaryStatistics(statistics.getBinaryStatistics()) : null,
                null);
    }

    private static BooleanStatistics toBooleanStatistics(DwrfProto.BucketStatistics bucketStatistics)
    {
        if (bucketStatistics.getCountCount() == 0) {
            return null;
        }

        return new BooleanStatistics(bucketStatistics.getCount(0));
    }

    private static IntegerStatistics toIntegerStatistics(DwrfProto.IntegerStatistics integerStatistics)
    {
        return new IntegerStatistics(
                integerStatistics.hasMinimum() ? integerStatistics.getMinimum() : null,
                integerStatistics.hasMaximum() ? integerStatistics.getMaximum() : null,
                integerStatistics.hasSum() ? integerStatistics.getSum() : null);
    }

    private static DoubleStatistics toDoubleStatistics(DwrfProto.DoubleStatistics doubleStatistics)
    {
        // if either min, max, or sum is NaN, ignore the stat
        if ((doubleStatistics.hasMinimum() && Double.isNaN(doubleStatistics.getMinimum())) ||
                (doubleStatistics.hasMaximum() && Double.isNaN(doubleStatistics.getMaximum())) ||
                (doubleStatistics.hasSum() && Double.isNaN(doubleStatistics.getSum()))) {
            return null;
        }

        return new DoubleStatistics(
                doubleStatistics.hasMinimum() ? doubleStatistics.getMinimum() : null,
                doubleStatistics.hasMaximum() ? doubleStatistics.getMaximum() : null);
    }

    @VisibleForTesting
    static StringStatistics toStringStatistics(HiveWriterVersion hiveWriterVersion, DwrfProto.StringStatistics stringStatistics, boolean isRowGroup)
    {
        if (hiveWriterVersion == ORIGINAL && !isRowGroup) {
            return null;
        }

        Slice maximum = stringStatistics.hasMaximum() ? maxStringTruncateToValidRange(byteStringToSlice(stringStatistics.getMaximumBytes()), hiveWriterVersion) : null;
        Slice minimum = stringStatistics.hasMinimum() ? minStringTruncateToValidRange(byteStringToSlice(stringStatistics.getMinimumBytes()), hiveWriterVersion) : null;
        long sum = stringStatistics.hasSum() ? stringStatistics.getSum() : 0;

        return new StringStatistics(minimum, maximum, sum);
    }

    private static BinaryStatistics toBinaryStatistics(DwrfProto.BinaryStatistics binaryStatistics)
    {
        if (!binaryStatistics.hasSum()) {
            return null;
        }

        return new BinaryStatistics(binaryStatistics.getSum());
    }

    private static OrcType toType(DwrfProto.Type type)
    {
        return new OrcType(toTypeKind(type.getKind()), type.getSubtypesList(), type.getFieldNamesList(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static List<OrcType> toType(List<DwrfProto.Type> types)
    {
        return ImmutableList.copyOf(Iterables.transform(types, DwrfMetadataReader::toType));
    }

    private static OrcTypeKind toTypeKind(DwrfProto.Type.Kind kind)
    {
        switch (kind) {
            case BOOLEAN:
                return OrcTypeKind.BOOLEAN;
            case BYTE:
                return OrcTypeKind.BYTE;
            case SHORT:
                return OrcTypeKind.SHORT;
            case INT:
                return OrcTypeKind.INT;
            case LONG:
                return OrcTypeKind.LONG;
            case FLOAT:
                return OrcTypeKind.FLOAT;
            case DOUBLE:
                return OrcTypeKind.DOUBLE;
            case STRING:
                return OrcTypeKind.STRING;
            case BINARY:
                return OrcTypeKind.BINARY;
            case TIMESTAMP:
                return OrcTypeKind.TIMESTAMP;
            case LIST:
                return OrcTypeKind.LIST;
            case MAP:
                return OrcTypeKind.MAP;
            case STRUCT:
                return OrcTypeKind.STRUCT;
            case UNION:
                return OrcTypeKind.UNION;
            default:
                throw new IllegalArgumentException(kind + " data type not implemented yet");
        }
    }

    private static StreamKind toStreamKind(DwrfProto.Stream.Kind kind)
    {
        switch (kind) {
            case PRESENT:
                return StreamKind.PRESENT;
            case DATA:
                return StreamKind.DATA;
            case LENGTH:
                return StreamKind.LENGTH;
            case DICTIONARY_DATA:
                return StreamKind.DICTIONARY_DATA;
            case DICTIONARY_COUNT:
                return StreamKind.DICTIONARY_COUNT;
            case NANO_DATA:
                return StreamKind.SECONDARY;
            case ROW_INDEX:
                return StreamKind.ROW_INDEX;
            case IN_DICTIONARY:
                return StreamKind.IN_DICTIONARY;
            case STRIDE_DICTIONARY:
                return StreamKind.ROW_GROUP_DICTIONARY;
            case STRIDE_DICTIONARY_LENGTH:
                return StreamKind.ROW_GROUP_DICTIONARY_LENGTH;
            case IN_MAP:
                return StreamKind.IN_MAP;
            default:
                throw new IllegalArgumentException(kind + " stream type not implemented yet");
        }
    }

    private static ColumnEncodingKind toColumnEncodingKind(OrcTypeKind type, DwrfProto.ColumnEncoding.Kind kind)
    {
        switch (kind) {
            case DIRECT:
                if (type == OrcTypeKind.SHORT || type == OrcTypeKind.INT || type == OrcTypeKind.LONG) {
                    return ColumnEncodingKind.DWRF_DIRECT;
                }
                else {
                    return ColumnEncodingKind.DIRECT;
                }
            case DICTIONARY:
                return ColumnEncodingKind.DICTIONARY;
            case MAP_FLAT:
                return ColumnEncodingKind.DWRF_MAP_FLAT;
            default:
                throw new IllegalArgumentException(kind + " stream encoding not implemented yet");
        }
    }

    private static CompressionKind toCompression(DwrfProto.CompressionKind compression)
    {
        switch (compression) {
            case NONE:
                return NONE;
            case ZLIB:
                return ZLIB;
            case SNAPPY:
                return SNAPPY;
            case LZ4:
                return LZ4;
            case ZSTD:
                return ZSTD;
            default:
                throw new IllegalArgumentException(compression + " compression not implemented yet");
        }
    }
}
