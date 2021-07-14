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
import com.facebook.presto.orc.metadata.statistics.StripeStatistics;
import com.facebook.presto.orc.proto.OrcProto;
import com.facebook.presto.orc.proto.OrcProto.RowIndexEntry;
import com.facebook.presto.orc.proto.OrcProto.Type;
import com.facebook.presto.orc.proto.OrcProto.Type.Builder;
import com.facebook.presto.orc.proto.OrcProto.UserMetadataItem;
import com.facebook.presto.orc.protobuf.ByteString;
import com.facebook.presto.orc.protobuf.MessageLite;
import com.google.common.collect.ImmutableList;
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
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toList;

public class OrcMetadataWriter
        implements MetadataWriter
{
    // see https://github.com/prestodb/orc-protobuf/blob/master/src/main/protobuf/orc_proto.proto
    private static final int PRESTO_WRITER_ID = 2;
    // in order to change this value, the master Apache ORC proto file must be updated
    private static final int ORC_WRITER_VERSION = 6;
    private static final List<Integer> ORC_METADATA_VERSION = ImmutableList.of(0, 12);

    @Override
    public List<Integer> getOrcMetadataVersion()
    {
        return ORC_METADATA_VERSION;
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
        OrcProto.PostScript postScriptProtobuf = OrcProto.PostScript.newBuilder()
                .addAllVersion(ORC_METADATA_VERSION)
                .setFooterLength(footerLength)
                .setMetadataLength(metadataLength)
                .setCompression(toCompression(compression))
                .setCompressionBlockSize(compressionBlockSize)
                .setWriterVersion(ORC_WRITER_VERSION)
                .build();

        return writeProtobufObject(output, postScriptProtobuf);
    }

    @Override
    public int writeDwrfStripeCache(SliceOutput output, Optional<DwrfStripeCacheData> dwrfStripeCacheData)
    {
        return 0;
    }

    @Override
    public int writeMetadata(SliceOutput output, Metadata metadata)
            throws IOException
    {
        OrcProto.Metadata metadataProtobuf = OrcProto.Metadata.newBuilder()
                .addAllStripeStats(metadata.getStripeStatsList().stream()
                        .map(OrcMetadataWriter::toStripeStatistics)
                        .collect(toList()))
                .build();

        return writeProtobufObject(output, metadataProtobuf);
    }

    private static OrcProto.StripeStatistics toStripeStatistics(StripeStatistics stripeStatistics)
    {
        return OrcProto.StripeStatistics.newBuilder()
                .addAllColStats(stripeStatistics.getColumnStatistics().stream()
                        .map(OrcMetadataWriter::toColumnStatistics)
                        .collect(toList()))
                .build();
    }

    @Override
    public int writeFooter(SliceOutput output, Footer footer)
            throws IOException
    {
        OrcProto.Footer footerProtobuf = OrcProto.Footer.newBuilder()
                .setWriter(PRESTO_WRITER_ID)
                .setNumberOfRows(footer.getNumberOfRows())
                .setRowIndexStride(footer.getRowsInRowGroup())
                .addAllStripes(footer.getStripes().stream()
                        .map(OrcMetadataWriter::toStripeInformation)
                        .collect(toList()))
                .addAllTypes(footer.getTypes().stream()
                        .map(OrcMetadataWriter::toType)
                        .collect(toList()))
                .addAllStatistics(footer.getFileStats().stream()
                        .map(OrcMetadataWriter::toColumnStatistics)
                        .collect(toList()))
                .addAllMetadata(footer.getUserMetadata().entrySet().stream()
                        .map(OrcMetadataWriter::toUserMetadata)
                        .collect(toList()))
                .build();

        return writeProtobufObject(output, footerProtobuf);
    }

    private static OrcProto.StripeInformation toStripeInformation(StripeInformation stripe)
    {
        return OrcProto.StripeInformation.newBuilder()
                .setNumberOfRows(stripe.getNumberOfRows())
                .setOffset(stripe.getOffset())
                .setIndexLength(stripe.getIndexLength())
                .setDataLength(stripe.getDataLength())
                .setFooterLength(stripe.getFooterLength())
                .build();
    }

    private static Type toType(OrcType type)
    {
        Builder builder = Type.newBuilder()
                .setKind(toTypeKind(type.getOrcTypeKind()))
                .addAllSubtypes(type.getFieldTypeIndexes())
                .addAllFieldNames(type.getFieldNames())
                .addAllAttributes(toStringPairList(type.getAttributes()));

        if (type.getLength().isPresent()) {
            builder.setMaximumLength(type.getLength().get());
        }
        if (type.getPrecision().isPresent()) {
            builder.setPrecision(type.getPrecision().get());
        }
        if (type.getScale().isPresent()) {
            builder.setScale(type.getScale().get());
        }
        return builder.build();
    }

    private static OrcProto.Type.Kind toTypeKind(OrcTypeKind orcTypeKind)
    {
        switch (orcTypeKind) {
            case BOOLEAN:
                return OrcProto.Type.Kind.BOOLEAN;
            case BYTE:
                return OrcProto.Type.Kind.BYTE;
            case SHORT:
                return OrcProto.Type.Kind.SHORT;
            case INT:
                return OrcProto.Type.Kind.INT;
            case LONG:
                return OrcProto.Type.Kind.LONG;
            case DECIMAL:
                return OrcProto.Type.Kind.DECIMAL;
            case FLOAT:
                return OrcProto.Type.Kind.FLOAT;
            case DOUBLE:
                return OrcProto.Type.Kind.DOUBLE;
            case STRING:
                return OrcProto.Type.Kind.STRING;
            case VARCHAR:
                return OrcProto.Type.Kind.VARCHAR;
            case CHAR:
                return OrcProto.Type.Kind.CHAR;
            case BINARY:
                return OrcProto.Type.Kind.BINARY;
            case DATE:
                return OrcProto.Type.Kind.DATE;
            case TIMESTAMP:
                return OrcProto.Type.Kind.TIMESTAMP;
            case LIST:
                return OrcProto.Type.Kind.LIST;
            case MAP:
                return OrcProto.Type.Kind.MAP;
            case STRUCT:
                return OrcProto.Type.Kind.STRUCT;
            case UNION:
                return OrcProto.Type.Kind.UNION;
        }
        throw new IllegalArgumentException("Unsupported type: " + orcTypeKind);
    }

    private static List<OrcProto.StringPair> toStringPairList(Map<String, String> attributes)
    {
        return attributes.entrySet().stream()
                .map(entry -> OrcProto.StringPair.newBuilder()
                        .setKey(entry.getKey())
                        .setValue(entry.getValue())
                        .build())
                .collect(toImmutableList());
    }

    private static OrcProto.ColumnStatistics toColumnStatistics(ColumnStatistics columnStatistics)
    {
        OrcProto.ColumnStatistics.Builder builder = OrcProto.ColumnStatistics.newBuilder();

        if (columnStatistics.hasNumberOfValues()) {
            builder.setNumberOfValues(columnStatistics.getNumberOfValues());
        }

        if (columnStatistics.getBooleanStatistics() != null) {
            builder.setBucketStatistics(OrcProto.BucketStatistics.newBuilder()
                    .addCount(columnStatistics.getBooleanStatistics().getTrueValueCount())
                    .build());
        }

        if (columnStatistics.getIntegerStatistics() != null) {
            OrcProto.IntegerStatistics.Builder integerStatistics = OrcProto.IntegerStatistics.newBuilder()
                    .setMinimum(columnStatistics.getIntegerStatistics().getMin())
                    .setMaximum(columnStatistics.getIntegerStatistics().getMax());
            if (columnStatistics.getIntegerStatistics().getSum() != null) {
                integerStatistics.setSum(columnStatistics.getIntegerStatistics().getSum());
            }
            builder.setIntStatistics(integerStatistics.build());
        }

        if (columnStatistics.getDoubleStatistics() != null) {
            builder.setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                    .setMinimum(columnStatistics.getDoubleStatistics().getMin())
                    .setMaximum(columnStatistics.getDoubleStatistics().getMax())
                    .build());
        }

        if (columnStatistics.getStringStatistics() != null) {
            OrcProto.StringStatistics.Builder statisticsBuilder = OrcProto.StringStatistics.newBuilder();
            if (columnStatistics.getStringStatistics().getMin() != null) {
                statisticsBuilder.setMinimumBytes(ByteString.copyFrom(columnStatistics.getStringStatistics().getMin().getBytes()));
            }
            if (columnStatistics.getStringStatistics().getMax() != null) {
                statisticsBuilder.setMaximumBytes(ByteString.copyFrom(columnStatistics.getStringStatistics().getMax().getBytes()));
            }
            statisticsBuilder.setSum(columnStatistics.getStringStatistics().getSum());
            builder.setStringStatistics(statisticsBuilder.build());
        }

        if (columnStatistics.getDateStatistics() != null) {
            builder.setDateStatistics(OrcProto.DateStatistics.newBuilder()
                    .setMinimum(columnStatistics.getDateStatistics().getMin())
                    .setMaximum(columnStatistics.getDateStatistics().getMax())
                    .build());
        }

        if (columnStatistics.getDecimalStatistics() != null) {
            builder.setDecimalStatistics(OrcProto.DecimalStatistics.newBuilder()
                    .setMinimum(columnStatistics.getDecimalStatistics().getMin().toString())
                    .setMaximum(columnStatistics.getDecimalStatistics().getMax().toString())
                    .build());
        }

        if (columnStatistics.getBinaryStatistics() != null) {
            builder.setBinaryStatistics(OrcProto.BinaryStatistics.newBuilder()
                    .setSum(columnStatistics.getBinaryStatistics().getSum())
                    .build());
        }

        return builder.build();
    }

    private static UserMetadataItem toUserMetadata(Entry<String, Slice> entry)
    {
        return OrcProto.UserMetadataItem.newBuilder()
                .setName(entry.getKey())
                .setValue(ByteString.copyFrom(entry.getValue().getBytes()))
                .build();
    }

    @Override
    public int writeStripeFooter(SliceOutput output, StripeFooter footer)
            throws IOException
    {
        OrcProto.StripeFooter footerProtobuf = OrcProto.StripeFooter.newBuilder()
                .addAllStreams(footer.getStreams().stream()
                        .map(OrcMetadataWriter::toStream)
                        .collect(toList()))
                .addAllColumns(footer.getColumnEncodings().entrySet().stream()
                        .sorted(Entry.comparingByKey())
                        .map(entry -> toColumnEncoding(entry.getValue()))
                        .collect(toList()))
                .build();

        return writeProtobufObject(output, footerProtobuf);
    }

    private static OrcProto.Stream toStream(Stream stream)
    {
        return OrcProto.Stream.newBuilder()
                .setColumn(stream.getColumn())
                .setKind(toStreamKind(stream.getStreamKind()))
                .setLength(stream.getLength())
                .build();
    }

    private static OrcProto.Stream.Kind toStreamKind(StreamKind streamKind)
    {
        switch (streamKind) {
            case PRESENT:
                return OrcProto.Stream.Kind.PRESENT;
            case DATA:
                return OrcProto.Stream.Kind.DATA;
            case LENGTH:
                return OrcProto.Stream.Kind.LENGTH;
            case DICTIONARY_DATA:
                return OrcProto.Stream.Kind.DICTIONARY_DATA;
            case DICTIONARY_COUNT:
                return OrcProto.Stream.Kind.DICTIONARY_COUNT;
            case SECONDARY:
                return OrcProto.Stream.Kind.SECONDARY;
            case ROW_INDEX:
                return OrcProto.Stream.Kind.ROW_INDEX;
        }
        throw new IllegalArgumentException("Unsupported stream kind: " + streamKind);
    }

    private static OrcProto.ColumnEncoding toColumnEncoding(ColumnEncoding columnEncodings)
    {
        checkArgument(
                !columnEncodings.getAdditionalSequenceEncodings().isPresent(),
                "Writing columns with non-zero sequence IDs is not supported in ORC: " + columnEncodings);

        return OrcProto.ColumnEncoding.newBuilder()
                .setKind(toColumnEncoding(columnEncodings.getColumnEncodingKind()))
                .setDictionarySize(columnEncodings.getDictionarySize())
                .build();
    }

    private static OrcProto.ColumnEncoding.Kind toColumnEncoding(ColumnEncodingKind columnEncodingKind)
    {
        switch (columnEncodingKind) {
            case DIRECT:
                return OrcProto.ColumnEncoding.Kind.DIRECT;
            case DICTIONARY:
                return OrcProto.ColumnEncoding.Kind.DICTIONARY;
            case DIRECT_V2:
                return OrcProto.ColumnEncoding.Kind.DIRECT_V2;
            case DICTIONARY_V2:
                return OrcProto.ColumnEncoding.Kind.DICTIONARY_V2;
        }
        throw new IllegalArgumentException("Unsupported column encoding kind: " + columnEncodingKind);
    }

    @Override
    public int writeRowIndexes(SliceOutput output, List<RowGroupIndex> rowGroupIndexes)
            throws IOException
    {
        OrcProto.RowIndex rowIndexProtobuf = OrcProto.RowIndex.newBuilder()
                .addAllEntry(rowGroupIndexes.stream()
                        .map(OrcMetadataWriter::toRowGroupIndex)
                        .collect(toList()))
                .build();
        return writeProtobufObject(output, rowIndexProtobuf);
    }

    private static RowIndexEntry toRowGroupIndex(RowGroupIndex rowGroupIndex)
    {
        return OrcProto.RowIndexEntry.newBuilder()
                .addAllPositions(rowGroupIndex.getPositions().stream()
                        .map(Integer::longValue)
                        .collect(toList()))
                .setStatistics(toColumnStatistics(rowGroupIndex.getColumnStatistics()))
                .build();
    }

    private static OrcProto.CompressionKind toCompression(CompressionKind compressionKind)
    {
        switch (compressionKind) {
            case NONE:
                return OrcProto.CompressionKind.NONE;
            case ZLIB:
                return OrcProto.CompressionKind.ZLIB;
            case SNAPPY:
                return OrcProto.CompressionKind.SNAPPY;
            case LZ4:
                return OrcProto.CompressionKind.LZ4;
            case ZSTD:
                return OrcProto.CompressionKind.ZSTD;
        }
        throw new IllegalArgumentException("Unsupported compression kind: " + compressionKind);
    }

    private static int writeProtobufObject(OutputStream output, MessageLite object)
            throws IOException
    {
        CountingOutputStream countingOutput = new CountingOutputStream(output);
        object.writeTo(countingOutput);
        return toIntExact(countingOutput.getCount());
    }
}
