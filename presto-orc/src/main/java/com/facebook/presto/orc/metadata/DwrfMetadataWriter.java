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
import com.facebook.presto.orc.metadata.Stream.StreamKind;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.proto.DwrfProto.RowIndexEntry;
import com.facebook.presto.orc.proto.DwrfProto.Type;
import com.facebook.presto.orc.proto.DwrfProto.Type.Builder;
import com.facebook.presto.orc.proto.DwrfProto.UserMetadataItem;
import com.facebook.presto.orc.protobuf.ByteString;
import com.facebook.presto.orc.protobuf.MessageLite;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class DwrfMetadataWriter
        implements MetadataWriter
{
    private static final int DWRF_WRITER_VERSION = 1;
    public static final Map<String, Slice> STATIC_METADATA = ImmutableMap.<String, Slice>builder()
            .put("orc.writer.name", utf8Slice("presto"))
            .put("orc.writer.version", utf8Slice(String.valueOf(DWRF_WRITER_VERSION)))
            .build();

    @Override
    public List<Integer> getOrcMetadataVersion()
    {
        // DWRF does not have a version field
        return ImmutableList.of();
    }

    @Override
    public int writePostscript(SliceOutput output,
            int footerLength,
            int metadataLength,
            CompressionKind compression,
            int compressionBlockSize,
            Optional<DwrfStripeCacheData> dwrfStripeCacheData)
            throws IOException
    {
        DwrfProto.PostScript.Builder postScriptBuilder = DwrfProto.PostScript.newBuilder()
                .setFooterLength(footerLength)
                .setWriterVersion(DWRF_WRITER_VERSION)
                .setCompression(toCompression(compression))
                .setCompressionBlockSize(compressionBlockSize);

        dwrfStripeCacheData.ifPresent(cache -> {
            postScriptBuilder.setCacheMode(toStripeCacheMode(cache.getDwrfStripeCacheMode()));
            postScriptBuilder.setCacheSize(cache.getDwrfStripeCacheSize());
        });

        DwrfProto.PostScript postScriptProtobuf = postScriptBuilder.build();
        return writeProtobufObject(output, postScriptProtobuf);
    }

    @Override
    public int writeDwrfStripeCache(SliceOutput output, Optional<DwrfStripeCacheData> dwrfStripeCacheData)
    {
        int size = 0;
        if (dwrfStripeCacheData.isPresent()) {
            DwrfStripeCacheData cache = dwrfStripeCacheData.get();
            size = cache.getDwrfStripeCacheSize();
            output.writeBytes(cache.getDwrfStripeCacheSlice(), 0, size);
        }
        return size;
    }

    @Override
    public int writeMetadata(SliceOutput output, Metadata metadata)
    {
        return 0;
    }

    @Override
    public int writeFooter(SliceOutput output, Footer footer)
            throws IOException
    {
        DwrfProto.Footer.Builder footerProtobuf = DwrfProto.Footer.newBuilder()
                .setNumberOfRows(footer.getNumberOfRows())
                .setRowIndexStride(footer.getRowsInRowGroup())
                .addAllStripes(footer.getStripes().stream()
                        .map(DwrfMetadataWriter::toStripeInformation)
                        .collect(toImmutableList()))
                .addAllTypes(footer.getTypes().stream()
                        .map(DwrfMetadataWriter::toType)
                        .collect(toImmutableList()))
                .addAllStatistics(footer.getFileStats().stream()
                        .map(DwrfMetadataWriter::toColumnStatistics)
                        .collect(toImmutableList()))
                .addAllMetadata(footer.getUserMetadata().entrySet().stream()
                        .map(DwrfMetadataWriter::toUserMetadata)
                        .collect(toImmutableList()))
                .addAllMetadata(STATIC_METADATA.entrySet().stream()
                        .map(DwrfMetadataWriter::toUserMetadata)
                        .collect(toImmutableList()));

        if (footer.getEncryption().isPresent()) {
            footerProtobuf.setEncryption(toEncryption(footer.getEncryption().get()));
        }

        if (footer.getRawSize().isPresent()) {
            footerProtobuf.setRawDataSize(footer.getRawSize().getAsLong());
        }

        if (footer.getDwrfStripeCacheOffsets().isPresent()) {
            footerProtobuf.addAllStripeCacheOffsets(footer.getDwrfStripeCacheOffsets().get());
        }

        return writeProtobufObject(output, footerProtobuf.build());
    }

    @VisibleForTesting
    static DwrfProto.StripeInformation toStripeInformation(StripeInformation stripe)
    {
        DwrfProto.StripeInformation.Builder builder = DwrfProto.StripeInformation.newBuilder()
                .setNumberOfRows(stripe.getNumberOfRows())
                .setOffset(stripe.getOffset())
                .setIndexLength(stripe.getIndexLength())
                .setDataLength(stripe.getDataLength())
                .setFooterLength(stripe.getFooterLength())
                .addAllKeyMetadata(stripe.getKeyMetadata().stream()
                        .map(ByteString::copyFrom)
                        .collect(toImmutableList()));

        if (stripe.getRawDataSize().isPresent()) {
            builder.setRawDataSize(stripe.getRawDataSize().getAsLong());
        }
        return builder.build();
    }

    private static Type toType(OrcType type)
    {
        Builder builder = Type.newBuilder()
                .setKind(toTypeKind(type.getOrcTypeKind()))
                .addAllSubtypes(type.getFieldTypeIndexes())
                .addAllFieldNames(type.getFieldNames());

        return builder.build();
    }

    private static Type.Kind toTypeKind(OrcTypeKind orcTypeKind)
    {
        switch (orcTypeKind) {
            case BOOLEAN:
                return Type.Kind.BOOLEAN;
            case BYTE:
                return Type.Kind.BYTE;
            case SHORT:
                return Type.Kind.SHORT;
            case INT:
                return Type.Kind.INT;
            case LONG:
                return Type.Kind.LONG;
            case FLOAT:
                return Type.Kind.FLOAT;
            case DOUBLE:
                return Type.Kind.DOUBLE;
            case STRING:
            case VARCHAR:
                return Type.Kind.STRING;
            case BINARY:
                return Type.Kind.BINARY;
            case TIMESTAMP:
                return Type.Kind.TIMESTAMP;
            case LIST:
                return Type.Kind.LIST;
            case MAP:
                return Type.Kind.MAP;
            case STRUCT:
                return Type.Kind.STRUCT;
            case UNION:
                return Type.Kind.UNION;
        }
        throw new IllegalArgumentException("Unsupported type: " + orcTypeKind);
    }

    public static DwrfProto.ColumnStatistics toColumnStatistics(ColumnStatistics columnStatistics)
    {
        DwrfProto.ColumnStatistics.Builder builder = DwrfProto.ColumnStatistics.newBuilder();

        if (columnStatistics.hasNumberOfValues()) {
            builder.setNumberOfValues(columnStatistics.getNumberOfValues());
        }

        if (columnStatistics.getBooleanStatistics() != null) {
            builder.setBucketStatistics(DwrfProto.BucketStatistics.newBuilder()
                    .addCount(columnStatistics.getBooleanStatistics().getTrueValueCount())
                    .build());
        }

        if (columnStatistics.getIntegerStatistics() != null) {
            DwrfProto.IntegerStatistics.Builder integerStatistics = DwrfProto.IntegerStatistics.newBuilder()
                    .setMinimum(columnStatistics.getIntegerStatistics().getMin())
                    .setMaximum(columnStatistics.getIntegerStatistics().getMax());
            if (columnStatistics.getIntegerStatistics().getSum() != null) {
                integerStatistics.setSum(columnStatistics.getIntegerStatistics().getSum());
            }
            builder.setIntStatistics(integerStatistics.build());
        }

        if (columnStatistics.getDoubleStatistics() != null) {
            builder.setDoubleStatistics(DwrfProto.DoubleStatistics.newBuilder()
                    .setMinimum(columnStatistics.getDoubleStatistics().getMin())
                    .setMaximum(columnStatistics.getDoubleStatistics().getMax())
                    .build());
        }

        if (columnStatistics.getStringStatistics() != null) {
            DwrfProto.StringStatistics.Builder statisticsBuilder = DwrfProto.StringStatistics.newBuilder();
            if (columnStatistics.getStringStatistics().getMin() != null) {
                statisticsBuilder.setMinimumBytes(ByteString.copyFrom(columnStatistics.getStringStatistics().getMin().getBytes()));
            }
            if (columnStatistics.getStringStatistics().getMax() != null) {
                statisticsBuilder.setMaximumBytes(ByteString.copyFrom(columnStatistics.getStringStatistics().getMax().getBytes()));
            }
            statisticsBuilder.setSum(columnStatistics.getStringStatistics().getSum());
            builder.setStringStatistics(statisticsBuilder.build());
        }

        if (columnStatistics.getBinaryStatistics() != null) {
            builder.setBinaryStatistics(DwrfProto.BinaryStatistics.newBuilder()
                    .setSum(columnStatistics.getBinaryStatistics().getSum())
                    .build());
        }

        return builder.build();
    }

    private static UserMetadataItem toUserMetadata(Entry<String, Slice> entry)
    {
        return UserMetadataItem.newBuilder()
                .setName(entry.getKey())
                .setValue(ByteString.copyFrom(entry.getValue().getBytes()))
                .build();
    }

    @Override
    public int writeStripeFooter(SliceOutput output, StripeFooter footer)
            throws IOException
    {
        DwrfProto.StripeFooter footerProtobuf = DwrfProto.StripeFooter.newBuilder()
                .addAllStreams(footer.getStreams().stream()
                        .map(DwrfMetadataWriter::toStream)
                        .collect(toImmutableList()))
                .addAllColumns(footer.getColumnEncodings().entrySet().stream()
                        .map(entry -> toColumnEncoding(entry.getKey(), entry.getValue()))
                        .collect(toImmutableList()))
                .addAllEncryptedGroups(footer.getStripeEncryptionGroups().stream()
                        .map(group -> ByteString.copyFrom(group.getBytes()))
                        .collect(toImmutableList()))
                .build();

        return writeProtobufObject(output, footerProtobuf);
    }

    private static DwrfProto.Stream toStream(Stream stream)
    {
        DwrfProto.Stream.Builder streamBuilder = DwrfProto.Stream.newBuilder()
                .setColumn(stream.getColumn())
                .setKind(toStreamKind(stream.getStreamKind()))
                .setLength(stream.getLength())
                .setUseVInts(stream.isUseVInts());
        stream.getOffset().ifPresent(streamBuilder::setOffset);

        return streamBuilder.build();
    }

    private static DwrfProto.Stream.Kind toStreamKind(StreamKind streamKind)
    {
        switch (streamKind) {
            case PRESENT:
                return DwrfProto.Stream.Kind.PRESENT;
            case DATA:
                return DwrfProto.Stream.Kind.DATA;
            case SECONDARY:
                return DwrfProto.Stream.Kind.NANO_DATA;
            case LENGTH:
                return DwrfProto.Stream.Kind.LENGTH;
            case DICTIONARY_DATA:
                return DwrfProto.Stream.Kind.DICTIONARY_DATA;
            case DICTIONARY_COUNT:
                return DwrfProto.Stream.Kind.DICTIONARY_COUNT;
            case ROW_INDEX:
                return DwrfProto.Stream.Kind.ROW_INDEX;
        }
        throw new IllegalArgumentException("Unsupported stream kind: " + streamKind);
    }

    public static DwrfProto.ColumnEncoding toColumnEncoding(int columnId, ColumnEncoding columnEncoding)
    {
        checkArgument(
                !columnEncoding.getAdditionalSequenceEncodings().isPresent(),
                "DWRF writer doesn't support writing columns with non-zero sequence IDs: " + columnEncoding);

        return DwrfProto.ColumnEncoding.newBuilder()
                .setKind(toColumnEncodingKind(columnEncoding.getColumnEncodingKind()))
                .setDictionarySize(columnEncoding.getDictionarySize())
                .setColumn(columnId)
                .setSequence(0)
                .build();
    }

    private static DwrfProto.ColumnEncoding.Kind toColumnEncodingKind(ColumnEncodingKind columnEncodingKind)
    {
        switch (columnEncodingKind) {
            case DIRECT:
                return DwrfProto.ColumnEncoding.Kind.DIRECT;
            case DICTIONARY:
                return DwrfProto.ColumnEncoding.Kind.DICTIONARY;
        }
        throw new IllegalArgumentException("Unsupported column encoding kind: " + columnEncodingKind);
    }

    @Override
    public int writeRowIndexes(SliceOutput output, List<RowGroupIndex> rowGroupIndexes)
            throws IOException
    {
        DwrfProto.RowIndex rowIndexProtobuf = DwrfProto.RowIndex.newBuilder()
                .addAllEntry(rowGroupIndexes.stream()
                        .map(DwrfMetadataWriter::toRowGroupIndex)
                        .collect(toImmutableList()))
                .build();
        return writeProtobufObject(output, rowIndexProtobuf);
    }

    private static RowIndexEntry toRowGroupIndex(RowGroupIndex rowGroupIndex)
    {
        return RowIndexEntry.newBuilder()
                .addAllPositions(rowGroupIndex.getPositions().stream()
                        .map(Integer::longValue)
                        .collect(toImmutableList()))
                .setStatistics(toColumnStatistics(rowGroupIndex.getColumnStatistics()))
                .build();
    }

    private static DwrfProto.CompressionKind toCompression(CompressionKind compressionKind)
    {
        switch (compressionKind) {
            case NONE:
                return DwrfProto.CompressionKind.NONE;
            case ZLIB:
                return DwrfProto.CompressionKind.ZLIB;
            case SNAPPY:
                return DwrfProto.CompressionKind.SNAPPY;
            case LZ4:
                return DwrfProto.CompressionKind.LZ4;
            case ZSTD:
                return DwrfProto.CompressionKind.ZSTD;
        }
        throw new IllegalArgumentException("Unsupported compression kind: " + compressionKind);
    }

    private static DwrfProto.Encryption toEncryption(DwrfEncryption encryption)
    {
        return DwrfProto.Encryption.newBuilder()
                .setKeyProvider(toKeyProvider(encryption.getKeyProvider()))
                .addAllEncryptionGroups(encryption.getEncryptionGroups().stream()
                        .map(group -> toEncryptionGroup(group))
                        .collect(toImmutableList()))
                .build();
    }

    private static DwrfProto.Encryption.KeyProvider toKeyProvider(KeyProvider keyProvider)
    {
        switch (keyProvider) {
            case CRYPTO_SERVICE:
                return DwrfProto.Encryption.KeyProvider.CRYPTO_SERVICE;
            case UNKNOWN:
                return DwrfProto.Encryption.KeyProvider.UNKNOWN;
            default:
                throw new UnsupportedOperationException(format("unknown key provider: %s", keyProvider));
        }
    }

    private static DwrfProto.EncryptionGroup toEncryptionGroup(EncryptionGroup encryptionGroup)
    {
        return DwrfProto.EncryptionGroup.newBuilder()
                .addAllNodes(encryptionGroup.getNodes())
                .addAllStatistics(encryptionGroup.getStatistics().stream()
                        .map(statsSlice -> ByteString.copyFrom(statsSlice.getBytes()))
                        .collect(toImmutableList()))
                .build();
    }

    public static DwrfProto.StripeEncryptionGroup toStripeEncryptionGroup(StripeEncryptionGroup stripeEncryptionGroup)
    {
        return DwrfProto.StripeEncryptionGroup.newBuilder()
                .addAllStreams(stripeEncryptionGroup.getStreams().stream()
                        .map(DwrfMetadataWriter::toStream)
                        .collect(toImmutableList()))
                .addAllEncoding(stripeEncryptionGroup.getColumnEncodings().entrySet()
                        .stream()
                        .map(entry -> toColumnEncoding(entry.getKey(), entry.getValue()))
                        .collect(toImmutableList()))
                .build();
    }

    public static DwrfProto.FileStatistics toFileStatistics(List<ColumnStatistics> columnStatistics)
    {
        List<DwrfProto.ColumnStatistics> dwrfColumnStatistics = columnStatistics.stream()
                .map(DwrfMetadataWriter::toColumnStatistics)
                .collect(toList());
        return DwrfProto.FileStatistics.newBuilder()
                .addAllStatistics(dwrfColumnStatistics)
                .build();
    }

    private static DwrfProto.StripeCacheMode toStripeCacheMode(DwrfStripeCacheMode dwrfStripeCacheMode)
    {
        switch (dwrfStripeCacheMode) {
            case NONE:
                return DwrfProto.StripeCacheMode.NA;
            case INDEX:
                return DwrfProto.StripeCacheMode.INDEX;
            case FOOTER:
                return DwrfProto.StripeCacheMode.FOOTER;
            case INDEX_AND_FOOTER:
                return DwrfProto.StripeCacheMode.BOTH;
        }
        throw new IllegalArgumentException("Unsupported mode: " + dwrfStripeCacheMode);
    }

    private static int writeProtobufObject(OutputStream output, MessageLite object)
            throws IOException
    {
        CountingOutputStream countingOutput = new CountingOutputStream(output);
        object.writeTo(countingOutput);
        return toIntExact(countingOutput.getCount());
    }
}
