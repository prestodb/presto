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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcDecompressor;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.metadata.OrcType.OrcTypeKind;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.statistics.BinaryStatistics;
import com.facebook.presto.orc.metadata.statistics.BooleanStatistics;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateStatistics;
import com.facebook.presto.orc.metadata.statistics.DecimalStatistics;
import com.facebook.presto.orc.metadata.statistics.DoubleStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.proto.OrcProto;
import com.facebook.presto.orc.proto.OrcProto.RowIndexEntry;
import com.facebook.presto.orc.protobuf.ByteString;
import com.facebook.presto.orc.protobuf.CodedInputStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.management.ThreadMXBean;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.metadata.CompressionKind.LZ4;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion.ORC_HIVE_8732;
import static com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion.ORIGINAL;
import static com.facebook.presto.orc.metadata.statistics.ColumnStatistics.createColumnStatistics;
import static com.facebook.presto.orc.metadata.statistics.ShortDecimalStatisticsBuilder.SHORT_DECIMAL_VALUE_BYTES;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.tryGetCodePointAt;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.Character.MIN_SUPPLEMENTARY_CODE_POINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OrcMetadataReader
        implements MetadataReader
{
    private static final int REPLACEMENT_CHARACTER_CODE_POINT = 0xFFFD;
    private static final int PROTOBUF_MESSAGE_MAX_LIMIT = toIntExact(new DataSize(1, GIGABYTE).toBytes());
    private static final ThreadMXBean THREAD_MX_BEAN = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private final RuntimeStats runtimeStats;

    public OrcMetadataReader(RuntimeStats runtimeStats)
    {
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
    }

    @Override
    public PostScript readPostScript(byte[] data, int offset, int length)
            throws IOException
    {
        long cpuStart = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        CodedInputStream input = CodedInputStream.newInstance(data, offset, length);
        OrcProto.PostScript postScript = OrcProto.PostScript.parseFrom(input);
        runtimeStats.addMetricValue("OrcReadPostScriptTimeNanos", THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStart);

        return new PostScript(
                postScript.getVersionList(),
                postScript.getFooterLength(),
                postScript.getMetadataLength(),
                toCompression(postScript.getCompression()),
                postScript.getCompressionBlockSize(),
                toHiveWriterVersion(postScript.getWriterVersion()),
                OptionalInt.empty(),
                Optional.empty());
    }

    private static HiveWriterVersion toHiveWriterVersion(int writerVersion)
    {
        if (writerVersion >= 1) {
            return ORC_HIVE_8732;
        }
        return ORIGINAL;
    }

    @Override
    public Metadata readMetadata(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        long cpuStart = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        input.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
        OrcProto.Metadata metadata = OrcProto.Metadata.parseFrom(input);
        runtimeStats.addMetricValue("OrcReadMetadataTimeNanos", THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStart);
        return new Metadata(toStripeStatistics(hiveWriterVersion, metadata.getStripeStatsList()));
    }

    private static List<StripeStatistics> toStripeStatistics(HiveWriterVersion hiveWriterVersion, List<OrcProto.StripeStatistics> types)
    {
        return types.stream()
                .map(stripeStatistics -> toStripeStatistics(hiveWriterVersion, stripeStatistics))
                .collect(toImmutableList());
    }

    private static StripeStatistics toStripeStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.StripeStatistics stripeStatistics)
    {
        return new StripeStatistics(toColumnStatistics(hiveWriterVersion, stripeStatistics.getColStatsList(), false));
    }

    @Override
    public Footer readFooter(HiveWriterVersion hiveWriterVersion,
            InputStream inputStream,
            DwrfEncryptionProvider dwrfEncryptionProvider,
            DwrfKeyProvider dwrfKeyProvider,
            OrcDataSource orcDataSource,
            Optional<OrcDecompressor> decompressor)
            throws IOException
    {
        long cpuStart = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        input.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
        OrcProto.Footer footer = OrcProto.Footer.parseFrom(input);
        runtimeStats.addMetricValue("OrcReadFooterTimeNanos", THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStart);
        return new Footer(
                footer.getNumberOfRows(),
                footer.getRowIndexStride(),
                OptionalLong.empty(),
                toStripeInformation(footer.getStripesList()),
                toType(footer.getTypesList()),
                toColumnStatistics(hiveWriterVersion, footer.getStatisticsList(), false),
                toUserMetadata(footer.getMetadataList()),
                Optional.empty(),
                Optional.empty());
    }

    private static List<StripeInformation> toStripeInformation(List<OrcProto.StripeInformation> types)
    {
        return types.stream()
                .map(OrcMetadataReader::toStripeInformation)
                .collect(toImmutableList());
    }

    private static StripeInformation toStripeInformation(OrcProto.StripeInformation stripeInformation)
    {
        return new StripeInformation(
                stripeInformation.getNumberOfRows(),
                stripeInformation.getOffset(),
                stripeInformation.getIndexLength(),
                stripeInformation.getDataLength(),
                stripeInformation.getFooterLength(),
                OptionalLong.empty(),
                ImmutableList.of());
    }

    @Override
    public StripeFooter readStripeFooter(OrcDataSourceId orcDataSourceId, List<OrcType> types, InputStream inputStream)
            throws IOException
    {
        long cpuStart = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.StripeFooter stripeFooter = OrcProto.StripeFooter.parseFrom(input);
        runtimeStats.addMetricValue("OrcReadStripeFooterTimeNanos", THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStart);
        return new StripeFooter(toStream(stripeFooter.getStreamsList()), toColumnEncoding(stripeFooter.getColumnsList()), ImmutableList.of());
    }

    private static Stream toStream(OrcProto.Stream stream)
    {
        return new Stream(stream.getColumn(), toStreamKind(stream.getKind()), toIntExact(stream.getLength()), true);
    }

    private static List<Stream> toStream(List<OrcProto.Stream> streams)
    {
        return streams.stream()
                .map(OrcMetadataReader::toStream)
                .collect(toImmutableList());
    }

    private static ColumnEncoding toColumnEncoding(OrcProto.ColumnEncoding columnEncoding)
    {
        return new ColumnEncoding(toColumnEncodingKind(columnEncoding.getKind()), columnEncoding.getDictionarySize());
    }

    private static Map<Integer, ColumnEncoding> toColumnEncoding(List<OrcProto.ColumnEncoding> columnEncodings)
    {
        return IntStream.range(0, columnEncodings.size())
                .boxed()
                .collect(toImmutableMap(index -> index, index -> toColumnEncoding(columnEncodings.get(index))));
    }

    @Override
    public List<RowGroupIndex> readRowIndexes(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        long cpuStart = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.RowIndex rowIndex = OrcProto.RowIndex.parseFrom(input);
        runtimeStats.addMetricValue("OrcReadRowIndexesTimeNanos", THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStart);
        return rowIndex.getEntryList().stream()
                .map(rowIndexEntry -> toRowGroupIndex(hiveWriterVersion, rowIndexEntry))
                .collect(toImmutableList());
    }

    @Override
    public List<HiveBloomFilter> readBloomFilterIndexes(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.BloomFilterIndex bloomFilter = OrcProto.BloomFilterIndex.parseFrom(input);
        List<OrcProto.BloomFilter> bloomFilterList = bloomFilter.getBloomFilterList();
        ImmutableList.Builder<HiveBloomFilter> builder = ImmutableList.builder();
        for (OrcProto.BloomFilter orcBloomFilter : bloomFilterList) {
            builder.add(new HiveBloomFilter(orcBloomFilter.getBitsetList(), orcBloomFilter.getBitsetCount() * 64, orcBloomFilter.getNumHashFunctions()));
        }
        return builder.build();
    }

    private static RowGroupIndex toRowGroupIndex(HiveWriterVersion hiveWriterVersion, RowIndexEntry rowIndexEntry)
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

    private static ColumnStatistics toColumnStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.ColumnStatistics statistics, boolean isRowGroup)
    {
        return createColumnStatistics(
                statistics.getNumberOfValues(),
                statistics.hasBucketStatistics() ? toBooleanStatistics(statistics.getBucketStatistics()) : null,
                statistics.hasIntStatistics() ? toIntegerStatistics(statistics.getIntStatistics()) : null,
                statistics.hasDoubleStatistics() ? toDoubleStatistics(statistics.getDoubleStatistics()) : null,
                statistics.hasStringStatistics() ? toStringStatistics(hiveWriterVersion, statistics.getStringStatistics(), isRowGroup) : null,
                statistics.hasDateStatistics() ? toDateStatistics(hiveWriterVersion, statistics.getDateStatistics(), isRowGroup) : null,
                statistics.hasDecimalStatistics() ? toDecimalStatistics(statistics.getDecimalStatistics()) : null,
                statistics.hasBinaryStatistics() ? toBinaryStatistics(statistics.getBinaryStatistics()) : null,
                null);
    }

    private static List<ColumnStatistics> toColumnStatistics(HiveWriterVersion hiveWriterVersion, List<OrcProto.ColumnStatistics> columnStatistics, boolean isRowGroup)
    {
        if (columnStatistics == null) {
            return ImmutableList.of();
        }
        return columnStatistics.stream()
                .map(statistics -> toColumnStatistics(hiveWriterVersion, statistics, isRowGroup))
                .collect(toImmutableList());
    }

    private static Map<String, Slice> toUserMetadata(List<OrcProto.UserMetadataItem> metadataList)
    {
        ImmutableMap.Builder<String, Slice> mapBuilder = ImmutableMap.builder();
        for (OrcProto.UserMetadataItem item : metadataList) {
            mapBuilder.put(item.getName(), byteStringToSlice(item.getValue()));
        }
        return mapBuilder.build();
    }

    private static BooleanStatistics toBooleanStatistics(OrcProto.BucketStatistics bucketStatistics)
    {
        if (bucketStatistics.getCountCount() == 0) {
            return null;
        }

        return new BooleanStatistics(bucketStatistics.getCount(0));
    }

    private static IntegerStatistics toIntegerStatistics(OrcProto.IntegerStatistics integerStatistics)
    {
        return new IntegerStatistics(
                integerStatistics.hasMinimum() ? integerStatistics.getMinimum() : null,
                integerStatistics.hasMaximum() ? integerStatistics.getMaximum() : null,
                integerStatistics.hasSum() ? integerStatistics.getSum() : null);
    }

    private static DoubleStatistics toDoubleStatistics(OrcProto.DoubleStatistics doubleStatistics)
    {
        // TODO remove this when double statistics are changed to correctly deal with NaNs
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

    static StringStatistics toStringStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.StringStatistics stringStatistics, boolean isRowGroup)
    {
        if (hiveWriterVersion == ORIGINAL && !isRowGroup) {
            return null;
        }

        Slice maximum = stringStatistics.hasMaximum() ? maxStringTruncateToValidRange(byteStringToSlice(stringStatistics.getMaximumBytes()), hiveWriterVersion) : null;
        Slice minimum = stringStatistics.hasMinimum() ? minStringTruncateToValidRange(byteStringToSlice(stringStatistics.getMinimumBytes()), hiveWriterVersion) : null;
        long sum = stringStatistics.hasSum() ? stringStatistics.getSum() : 0;
        return new StringStatistics(minimum, maximum, sum);
    }

    private static DecimalStatistics toDecimalStatistics(OrcProto.DecimalStatistics decimalStatistics)
    {
        BigDecimal minimum = decimalStatistics.hasMinimum() ? new BigDecimal(decimalStatistics.getMinimum()) : null;
        BigDecimal maximum = decimalStatistics.hasMaximum() ? new BigDecimal(decimalStatistics.getMaximum()) : null;

        // could be long (16 bytes) or short (8 bytes); use short for estimation
        return new DecimalStatistics(minimum, maximum, SHORT_DECIMAL_VALUE_BYTES);
    }

    private static BinaryStatistics toBinaryStatistics(OrcProto.BinaryStatistics binaryStatistics)
    {
        if (!binaryStatistics.hasSum()) {
            return null;
        }

        return new BinaryStatistics(binaryStatistics.getSum());
    }

    static Slice byteStringToSlice(ByteString value)
    {
        return Slices.wrappedBuffer(value.toByteArray());
    }

    @VisibleForTesting
    public static Slice maxStringTruncateToValidRange(Slice value, HiveWriterVersion version)
    {
        if (value == null) {
            return null;
        }

        if (version != ORIGINAL) {
            return value;
        }

        int index = findStringStatisticTruncationPositionForOriginalOrcWriter(value);
        if (index == value.length()) {
            return value;
        }
        // Append 0xFF so that it is larger than value
        Slice newValue = Slices.copyOf(value, 0, index + 1);
        newValue.setByte(index, 0xFF);
        return newValue;
    }

    @VisibleForTesting
    public static Slice minStringTruncateToValidRange(Slice value, HiveWriterVersion version)
    {
        if (value == null) {
            return null;
        }

        if (version != ORIGINAL) {
            return value;
        }

        int index = findStringStatisticTruncationPositionForOriginalOrcWriter(value);
        if (index == value.length()) {
            return value;
        }
        return Slices.copyOf(value, 0, index);
    }

    @VisibleForTesting
    static int findStringStatisticTruncationPositionForOriginalOrcWriter(Slice utf8)
    {
        int length = utf8.length();

        int position = 0;
        while (position < length) {
            int codePoint = tryGetCodePointAt(utf8, position);

            // stop at invalid sequences
            if (codePoint < 0) {
                break;
            }

            // The original ORC writers round trip string min and max values through java.lang.String which
            // replaces invalid UTF-8 sequences with the unicode replacement character.  This can cause the min value to be
            // greater than expected which can result in data sections being skipped instead of being processed. As a work around,
            // the string stats are truncated at the first replacement character.
            if (codePoint == REPLACEMENT_CHARACTER_CODE_POINT) {
                break;
            }

            // The original writer performs comparisons using java Strings to determine the minimum and maximum
            // values. This results in weird behaviors in the presence of surrogate pairs and special characters.
            //
            // For example, unicode code point 0x1D403 has the following representations:
            // UTF-16: [0xD835, 0xDC03]
            // UTF-8: [0xF0, 0x9D, 0x90, 0x83]
            //
            // while code point 0xFFFD (the replacement character) has the following representations:
            // UTF-16: [0xFFFD]
            // UTF-8: [0xEF, 0xBF, 0xBD]
            //
            // when comparisons between strings containing these characters are done with Java Strings (UTF-16),
            // 0x1D403 < 0xFFFD, but when comparisons are done using raw codepoints or UTF-8, 0x1D403 > 0xFFFD
            //
            // We use the following logic to ensure that we have a wider range of min-max
            // * if a min string has a surrogate character, the min string is truncated
            //   at the first occurrence of the surrogate character (to exclude the surrogate character)
            // * if a max string has a surrogate character, the max string is truncated
            //   at the first occurrence the surrogate character and 0xFF byte is appended to it.
            if (codePoint >= MIN_SUPPLEMENTARY_CODE_POINT) {
                break;
            }

            position += lengthOfCodePoint(codePoint);
        }
        return position;
    }

    private static DateStatistics toDateStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.DateStatistics dateStatistics, boolean isRowGroup)
    {
        if (hiveWriterVersion == ORIGINAL && !isRowGroup) {
            return null;
        }

        return new DateStatistics(
                dateStatistics.hasMinimum() ? dateStatistics.getMinimum() : null,
                dateStatistics.hasMaximum() ? dateStatistics.getMaximum() : null);
    }

    private static OrcType toType(OrcProto.Type type)
    {
        Optional<Integer> length = Optional.empty();
        if (type.getKind() == OrcProto.Type.Kind.VARCHAR || type.getKind() == OrcProto.Type.Kind.CHAR) {
            length = Optional.of(type.getMaximumLength());
        }
        Optional<Integer> precision = Optional.empty();
        Optional<Integer> scale = Optional.empty();
        if (type.getKind() == OrcProto.Type.Kind.DECIMAL) {
            precision = Optional.of(type.getPrecision());
            scale = Optional.of(type.getScale());
        }
        return new OrcType(toTypeKind(type.getKind()), type.getSubtypesList(), type.getFieldNamesList(), length, precision, scale, toMap(type.getAttributesList()));
    }

    private static List<OrcType> toType(List<OrcProto.Type> types)
    {
        return types.stream()
                .map(OrcMetadataReader::toType)
                .collect(toImmutableList());
    }

    private static OrcTypeKind toTypeKind(OrcProto.Type.Kind typeKind)
    {
        switch (typeKind) {
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
            case DECIMAL:
                return OrcTypeKind.DECIMAL;
            case DATE:
                return OrcTypeKind.DATE;
            case VARCHAR:
                return OrcTypeKind.VARCHAR;
            case CHAR:
                return OrcTypeKind.CHAR;
            default:
                throw new IllegalStateException(typeKind + " stream type not implemented yet");
        }
    }

    // This method assumes type attributes have no duplicate key
    private static Map<String, String> toMap(List<OrcProto.StringPair> attributes)
    {
        ImmutableMap.Builder<String, String> results = new ImmutableMap.Builder<>();
        if (attributes != null) {
            for (OrcProto.StringPair attribute : attributes) {
                if (attribute.hasKey() && attribute.hasValue()) {
                    results.put(attribute.getKey(), attribute.getValue());
                }
            }
        }
        return results.build();
    }

    private static StreamKind toStreamKind(OrcProto.Stream.Kind streamKind)
    {
        switch (streamKind) {
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
            case SECONDARY:
                return StreamKind.SECONDARY;
            case ROW_INDEX:
                return StreamKind.ROW_INDEX;
            case BLOOM_FILTER:
                return StreamKind.BLOOM_FILTER;
            case BLOOM_FILTER_UTF8:
                return StreamKind.BLOOM_FILTER_UTF8;
            default:
                throw new IllegalStateException(streamKind + " stream type not implemented yet");
        }
    }

    private static ColumnEncodingKind toColumnEncodingKind(OrcProto.ColumnEncoding.Kind columnEncodingKind)
    {
        switch (columnEncodingKind) {
            case DIRECT:
                return ColumnEncodingKind.DIRECT;
            case DIRECT_V2:
                return ColumnEncodingKind.DIRECT_V2;
            case DICTIONARY:
                return ColumnEncodingKind.DICTIONARY;
            case DICTIONARY_V2:
                return ColumnEncodingKind.DICTIONARY_V2;
            default:
                throw new IllegalStateException(columnEncodingKind + " stream encoding not implemented yet");
        }
    }

    private static CompressionKind toCompression(OrcProto.CompressionKind compression)
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
                throw new IllegalStateException(compression + " compression not implemented yet");
        }
    }
}
