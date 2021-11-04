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
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.EncryptionLibrary;
import com.facebook.presto.orc.OrcCorruptionException;
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
import com.facebook.presto.orc.metadata.statistics.DoubleStatistics;
import com.facebook.presto.orc.metadata.statistics.HiveBloomFilter;
import com.facebook.presto.orc.metadata.statistics.IntegerStatistics;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.ByteString;
import com.facebook.presto.orc.protobuf.CodedInputStream;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.orc.stream.SharedBuffer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.sun.management.ThreadMXBean;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.SortedMap;

import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.NoopOrcLocalMemoryContext.NOOP_ORC_LOCAL_MEMORY_CONTEXT;
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
import static com.facebook.presto.orc.metadata.statistics.ColumnStatistics.createColumnStatistics;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DwrfMetadataReader
        implements MetadataReader
{
    private static final ThreadMXBean THREAD_MX_BEAN = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    private final RuntimeStats runtimeStats;

    public DwrfMetadataReader(RuntimeStats runtimeStats)
    {
        this.runtimeStats = requireNonNull(runtimeStats, "runtimeStats is null");
    }

    @Override
    public PostScript readPostScript(byte[] data, int offset, int length)
            throws IOException
    {
        long cpuStart = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        CodedInputStream input = CodedInputStream.newInstance(data, offset, length);
        DwrfProto.PostScript postScript = DwrfProto.PostScript.parseFrom(input);

        HiveWriterVersion writerVersion = postScript.hasWriterVersion() && postScript.getWriterVersion() > 0 ? ORC_HIVE_8732 : ORIGINAL;

        OptionalInt stripeCacheLength = OptionalInt.empty();
        Optional<DwrfStripeCacheMode> stripeCacheMode = Optional.empty();
        if (postScript.hasCacheSize() && postScript.hasCacheMode()) {
            stripeCacheLength = OptionalInt.of(postScript.getCacheSize());
            stripeCacheMode = Optional.of(toStripeCacheMode(postScript.getCacheMode()));
        }
        runtimeStats.addMetricValue("DwrfReadPostScriptTimeNanos", THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStart);

        return new PostScript(
                ImmutableList.of(),
                postScript.getFooterLength(),
                0,
                toCompression(postScript.getCompression()),
                postScript.getCompressionBlockSize(),
                writerVersion,
                stripeCacheLength,
                stripeCacheMode);
    }

    @Override
    public Metadata readMetadata(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
    {
        return new Metadata(ImmutableList.of());
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
        DwrfProto.Footer footer = DwrfProto.Footer.parseFrom(input);
        List<ColumnStatistics> fileStats = toColumnStatistics(hiveWriterVersion, footer.getStatisticsList(), false);
        List<StripeInformation> fileStripes = toStripeInformation(footer.getStripesList());
        List<OrcType> types = toType(footer.getTypesList());
        Optional<DwrfEncryption> encryption = footer.hasEncryption() ? Optional.of(toEncryption(footer.getEncryption())) : Optional.empty();
        Optional<List<Integer>> stripeCacheOffsets = Optional.of(footer.getStripeCacheOffsetsList());

        if (encryption.isPresent()) {
            Map<Integer, Slice> keys = dwrfKeyProvider.getIntermediateKeys(types);
            EncryptionLibrary encryptionLibrary = dwrfEncryptionProvider.getEncryptionLibrary(encryption.get().getKeyProvider());
            fileStats = decryptAndCombineFileStatistics(hiveWriterVersion, encryption.get(), encryptionLibrary, fileStats, fileStripes, keys, orcDataSource, decompressor);
        }
        runtimeStats.addMetricValue("DwrfReadFooterTimeNanos", THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStart);

        OptionalLong rawSize = footer.hasRawDataSize() ? OptionalLong.of(footer.getRawDataSize()) : OptionalLong.empty();
        return new Footer(
                footer.getNumberOfRows(),
                footer.getRowIndexStride(),
                rawSize,
                fileStripes,
                types,
                fileStats,
                toUserMetadata(footer.getMetadataList()),
                encryption,
                stripeCacheOffsets);
    }

    private List<ColumnStatistics> decryptAndCombineFileStatistics(HiveWriterVersion hiveWriterVersion,
            DwrfEncryption dwrfEncryption,
            EncryptionLibrary encryptionLibrary,
            List<ColumnStatistics> fileStats,
            List<StripeInformation> fileStripes,
            Map<Integer, Slice> nodeToIntermediateKeys,
            OrcDataSource orcDataSource,
            Optional<OrcDecompressor> decompressor)
    {
        requireNonNull(dwrfEncryption, "dwrfEncryption is null");
        requireNonNull(encryptionLibrary, "encryptionLibrary is null");

        if (nodeToIntermediateKeys.isEmpty() || fileStats.isEmpty()) {
            return fileStats;
        }

        ColumnStatistics[] decryptedFileStats = fileStats.toArray(new ColumnStatistics[0]);
        List<EncryptionGroup> encryptionGroups = dwrfEncryption.getEncryptionGroups();
        List<byte[]> stripeKeys = null;
        if (!fileStripes.isEmpty() && !fileStripes.get(0).getKeyMetadata().isEmpty()) {
            stripeKeys = fileStripes.get(0).getKeyMetadata();
            checkState(stripeKeys.size() == encryptionGroups.size(),
                    "Number of keys in the first stripe must be the same as the number of encryption groups");
        }

        // if there is a node that has child nodes then its whole subtree will be encrypted and only the parent
        // node is added to the encryption group
        for (int groupIdx = 0; groupIdx < encryptionGroups.size(); groupIdx++) {
            EncryptionGroup encryptionGroup = encryptionGroups.get(groupIdx);
            DwrfDataEncryptor decryptor = null;
            List<Integer> nodes = encryptionGroup.getNodes();
            for (int i = 0; i < nodes.size(); i++) {
                Integer nodeId = nodes.get(i);

                // do decryption only for those nodes that are requested (part of the projection)
                if (!nodeToIntermediateKeys.containsKey(nodeId)) {
                    continue;
                }

                if (decryptor == null) {
                    // DEK for the FileStats can be stored either in the footer or/and in the first stripe.
                    // The key in the footer takes priority over the key in the first stripe.
                    byte[] encryptedDataKeyWithMeta = null;
                    if (encryptionGroup.getKeyMetadata().isPresent()) {
                        encryptedDataKeyWithMeta = encryptionGroup.getKeyMetadata().get().byteArray();
                    }
                    else if (stripeKeys != null) {
                        encryptedDataKeyWithMeta = stripeKeys.get(groupIdx);
                    }
                    checkState(encryptedDataKeyWithMeta != null, "DEK for %s encryption group is null", groupIdx);

                    // decrypt the DEK which is encrypted using the IEK passed into a record reader
                    byte[] intermediateKey = nodeToIntermediateKeys.get(nodeId).byteArray();
                    byte[] dataKey = encryptionLibrary.decryptKey(intermediateKey, encryptedDataKeyWithMeta, 0, encryptedDataKeyWithMeta.length);
                    decryptor = new DwrfDataEncryptor(dataKey, encryptionLibrary);
                }

                // decrypt the FileStats
                Slice encryptedFileStats = encryptionGroup.getStatistics().get(i);
                try (OrcInputStream inputStream = new OrcInputStream(
                        orcDataSource.getId(),
                        // Memory is not accounted as the buffer is expected to be tiny and will be immediately discarded
                        new SharedBuffer(NOOP_ORC_LOCAL_MEMORY_CONTEXT),
                        new BasicSliceInput(encryptedFileStats),
                        decompressor,
                        Optional.of(decryptor),
                        NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                        encryptedFileStats.length())) {
                    CodedInputStream input = CodedInputStream.newInstance(inputStream);
                    DwrfProto.FileStatistics nodeStats = DwrfProto.FileStatistics.parseFrom(input);

                    // FileStatistics contains ColumnStatistics for the node and all its child nodes (subtree)
                    for (int statsIdx = 0; statsIdx < nodeStats.getStatisticsCount(); statsIdx++) {
                        decryptedFileStats[nodeId + statsIdx] =
                                toColumnStatistics(hiveWriterVersion, nodeStats.getStatistics(statsIdx), false);
                    }
                }
                catch (IOException e) {
                    throw new OrcCorruptionException(e, orcDataSource.getId(), "Failed to read or decrypt FileStatistics for node %s", nodeId);
                }
            }
        }

        return ImmutableList.copyOf(decryptedFileStats);
    }

    private static DwrfEncryption toEncryption(DwrfProto.Encryption encryption)
    {
        KeyProvider keyProvider = toKeyProvider(encryption.getKeyProvider());
        List<EncryptionGroup> encryptionGroups = toEncryptionGroups(encryption.getEncryptionGroupsList());
        return new DwrfEncryption(keyProvider, encryptionGroups);
    }

    private static List<EncryptionGroup> toEncryptionGroups(List<DwrfProto.EncryptionGroup> encryptionGroups)
    {
        ImmutableList.Builder<EncryptionGroup> encryptionGroupBuilder = ImmutableList.builder();
        for (DwrfProto.EncryptionGroup dwrfEncryptionGroup : encryptionGroups) {
            encryptionGroupBuilder.add(new EncryptionGroup(
                    dwrfEncryptionGroup.getNodesList(),
                    dwrfEncryptionGroup.hasKeyMetadata() ? Optional.of(byteStringToSlice(dwrfEncryptionGroup.getKeyMetadata())) : Optional.empty(),
                    dwrfEncryptionGroup.getStatisticsList().stream()
                            .map(OrcMetadataReader::byteStringToSlice)
                            .collect(toImmutableList())));
        }
        return encryptionGroupBuilder.build();
    }

    private static KeyProvider toKeyProvider(DwrfProto.Encryption.KeyProvider keyProvider)
    {
        switch (keyProvider) {
            case CRYPTO_SERVICE:
                return KeyProvider.CRYPTO_SERVICE;
            default:
                return KeyProvider.UNKNOWN;
        }
    }

    private static List<StripeInformation> toStripeInformation(List<DwrfProto.StripeInformation> stripeInformationList)
    {
        ImmutableList.Builder<StripeInformation> stripeInfoBuilder = ImmutableList.builder();
        List<byte[]> previousKeyMetadata = ImmutableList.of();
        for (DwrfProto.StripeInformation dwrfStripeInfo : stripeInformationList) {
            StripeInformation prestoStripeInfo = toStripeInformation(dwrfStripeInfo, previousKeyMetadata);
            stripeInfoBuilder.add(prestoStripeInfo);
            previousKeyMetadata = prestoStripeInfo.getKeyMetadata();
        }
        return stripeInfoBuilder.build();
    }

    private static StripeInformation toStripeInformation(DwrfProto.StripeInformation stripeInformation, List<byte[]> previousKeyMetadata)
    {
        List<byte[]> keyMetadata = stripeInformation.getKeyMetadataList().stream()
                .map(ByteString::toByteArray)
                .collect(toImmutableList());
        if (keyMetadata.isEmpty()) {
            keyMetadata = previousKeyMetadata;
        }
        OptionalLong rawDataSize = stripeInformation.hasRawDataSize() ? OptionalLong.of(stripeInformation.getRawDataSize()) : OptionalLong.empty();
        return new StripeInformation(
                stripeInformation.getNumberOfRows(),
                stripeInformation.getOffset(),
                stripeInformation.getIndexLength(),
                stripeInformation.getDataLength(),
                stripeInformation.getFooterLength(),
                rawDataSize,
                keyMetadata);
    }

    @Override
    public StripeFooter readStripeFooter(OrcDataSourceId orcDataSourceId, List<OrcType> types, InputStream inputStream)
            throws IOException
    {
        long cpuStart = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        DwrfProto.StripeFooter stripeFooter = DwrfProto.StripeFooter.parseFrom(input);
        runtimeStats.addMetricValue("DwrfReadStripeFooterTimeNanos", THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStart);
        return new StripeFooter(
                toStream(orcDataSourceId, stripeFooter.getStreamsList()),
                toColumnEncoding(types, stripeFooter.getColumnsList()),
                stripeFooter.getEncryptedGroupsList().stream()
                        .map(OrcMetadataReader::byteStringToSlice)
                        .collect(toImmutableList()));
    }

    private static Stream toStream(OrcDataSourceId orcDataSourceId, DwrfProto.Stream stream)
    {
        // reader doesn't support streams larger than 2GB
        if (stream.getLength() > Integer.MAX_VALUE) {
            throw new OrcCorruptionException(
                    orcDataSourceId,
                    "Stream size %s of one of the streams for column %s is larger than supported size %s",
                    stream.getLength(),
                    stream.getColumn(),
                    Integer.MAX_VALUE);
        }

        return new Stream(
                stream.getColumn(),
                toStreamKind(stream.getKind()),
                toIntExact(stream.getLength()),
                stream.getUseVInts(),
                stream.getSequence(),
                stream.hasOffset() ? Optional.of(stream.getOffset()) : Optional.empty());
    }

    private static List<Stream> toStream(OrcDataSourceId orcDataSourceId, List<DwrfProto.Stream> streams)
    {
        return streams.stream()
                .map((stream -> toStream(orcDataSourceId, stream)))
                .collect(toImmutableList());
    }

    private static DwrfSequenceEncoding toSequenceEncoding(OrcType type, DwrfProto.ColumnEncoding columnEncoding)
    {
        return new DwrfSequenceEncoding(
                columnEncoding.getKey(),
                new ColumnEncoding(
                        toColumnEncodingKind(type.getOrcTypeKind(), columnEncoding.getKind()),
                        columnEncoding.getDictionarySize()));
    }

    private static ColumnEncoding toColumnEncoding(OrcType type, List<DwrfProto.ColumnEncoding> columnEncodings)
    {
        DwrfProto.ColumnEncoding sequence0 = null;
        ImmutableSortedMap.Builder<Integer, DwrfSequenceEncoding> builder = ImmutableSortedMap.naturalOrder();
        for (DwrfProto.ColumnEncoding columnEncoding : columnEncodings) {
            if (columnEncoding.getSequence() == 0) {
                sequence0 = columnEncoding;
            }
            else {
                builder.put(columnEncoding.getSequence(), toSequenceEncoding(type, columnEncoding));
            }
        }

        SortedMap<Integer, DwrfSequenceEncoding> nonZeroSequences = builder.build();
        Optional<SortedMap<Integer, DwrfSequenceEncoding>> nonZeroEncodingsOptional =
                nonZeroSequences.isEmpty() ? Optional.empty() : Optional.of(nonZeroSequences);
        if (sequence0 != null) {
            return new ColumnEncoding(toColumnEncodingKind(type.getOrcTypeKind(), sequence0.getKind()), sequence0.getDictionarySize(), nonZeroEncodingsOptional);
        }
        else {
            // This is the case when value node of FLAT_MAP doesn't have encoding for sequence 0
            return new ColumnEncoding(ColumnEncodingKind.DWRF_DIRECT, 0, nonZeroEncodingsOptional);
        }
    }

    private static Map<Integer, ColumnEncoding> toColumnEncoding(List<OrcType> types, List<DwrfProto.ColumnEncoding> columnEncodings)
    {
        Map<Integer, List<DwrfProto.ColumnEncoding>> groupedColumnEncodings = new HashMap<>(columnEncodings.size());
        ImmutableMap.Builder<Integer, ColumnEncoding> resultBuilder = ImmutableMap.builder();

        for (int i = 0; i < columnEncodings.size(); i++) {
            DwrfProto.ColumnEncoding columnEncoding = columnEncodings.get(i);
            int column = columnEncoding.getColumn();

            // DWRF prior to version 6.0.8 doesn't set the value of column, infer it from the index
            if (!columnEncoding.hasColumn()) {
                column = i;
            }

            groupedColumnEncodings.computeIfAbsent(column, key -> new ArrayList<>()).add(columnEncoding);
        }

        for (Map.Entry<Integer, List<DwrfProto.ColumnEncoding>> entry : groupedColumnEncodings.entrySet()) {
            OrcType type = types.get(entry.getKey());

            resultBuilder.put(
                    entry.getKey(),
                    toColumnEncoding(type, entry.getValue()));
        }

        return resultBuilder.build();
    }

    @Override
    public List<RowGroupIndex> readRowIndexes(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        long cpuStart = THREAD_MX_BEAN.getCurrentThreadCpuTime();
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        DwrfProto.RowIndex rowIndex = DwrfProto.RowIndex.parseFrom(input);
        runtimeStats.addMetricValue("DwrfReadRowIndexesTimeNanos", THREAD_MX_BEAN.getCurrentThreadCpuTime() - cpuStart);
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
        return createColumnStatistics(
                statistics.getNumberOfValues(),
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

    static DwrfStripeCacheMode toStripeCacheMode(DwrfProto.StripeCacheMode mode)
    {
        switch (mode) {
            case INDEX:
                return DwrfStripeCacheMode.INDEX;
            case FOOTER:
                return DwrfStripeCacheMode.FOOTER;
            case BOTH:
                return DwrfStripeCacheMode.INDEX_AND_FOOTER;
            default:
                return DwrfStripeCacheMode.NONE;
        }
    }

    public static StripeEncryptionGroup toStripeEncryptionGroup(OrcDataSourceId orcDataSourceId, InputStream inputStream, List<OrcType> types)
            throws IOException
    {
        CodedInputStream codedInputStream = CodedInputStream.newInstance(inputStream);
        DwrfProto.StripeEncryptionGroup stripeEncryptionGroup = DwrfProto.StripeEncryptionGroup.parseFrom(codedInputStream);
        List<Stream> encryptedStreams = toStream(orcDataSourceId, stripeEncryptionGroup.getStreamsList());
        return new StripeEncryptionGroup(
                encryptedStreams,
                toColumnEncoding(types, stripeEncryptionGroup.getEncodingList()));
    }
}
