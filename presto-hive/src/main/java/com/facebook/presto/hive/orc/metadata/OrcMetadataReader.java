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
package com.facebook.presto.hive.orc.metadata;

import com.facebook.presto.hive.shaded.com.google.protobuf.CodedInputStream;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndexEntry;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static com.facebook.presto.hive.orc.metadata.CompressionKind.SNAPPY;
import static com.facebook.presto.hive.orc.metadata.CompressionKind.UNCOMPRESSED;
import static com.facebook.presto.hive.orc.metadata.CompressionKind.ZLIB;

public class OrcMetadataReader
        implements MetadataReader
{
    @Override
    public PostScript readPostScript(byte[] data, int offset, int length)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(data, offset, length);
        OrcProto.PostScript postScript = OrcProto.PostScript.parseFrom(input);

        return new PostScript(
                postScript.getVersionList(),
                postScript.getFooterLength(),
                postScript.getMetadataLength(),
                toCompression(postScript.getCompression()),
                postScript.getCompressionBlockSize());
    }

    @Override
    public Metadata readMetadata(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.Metadata metadata = OrcProto.Metadata.parseFrom(input);
        return new Metadata(toStripeStatistics(metadata.getStripeStatsList()));
    }

    public static List<StripeStatistics> toStripeStatistics(List<OrcProto.StripeStatistics> types)
    {
        return ImmutableList.copyOf(Iterables.transform(types, new Function<OrcProto.StripeStatistics, StripeStatistics>()
        {
            @Override
            public StripeStatistics apply(OrcProto.StripeStatistics type)
            {
                return toStripeStatistics(type);
            }
        }));
    }

    public static StripeStatistics toStripeStatistics(OrcProto.StripeStatistics stripeStatistics)
    {
        return new StripeStatistics(toColumnStatistics(stripeStatistics.getColStatsList()));
    }

    @Override
    public Footer readFooter(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.Footer footer = OrcProto.Footer.parseFrom(input);
        return new Footer(
                footer.getNumberOfRows(),
                footer.getRowIndexStride(),
                toStripeInformation(footer.getStripesList()),
                toType(footer.getTypesList()),
                toColumnStatistics(footer.getStatisticsList()));
    }

    public static List<StripeInformation> toStripeInformation(List<OrcProto.StripeInformation> types)
    {
        return ImmutableList.copyOf(Iterables.transform(types, new Function<OrcProto.StripeInformation, StripeInformation>()
        {
            @Override
            public StripeInformation apply(OrcProto.StripeInformation type)
            {
                return toStripeInformation(type);
            }
        }));
    }

    public static StripeInformation toStripeInformation(OrcProto.StripeInformation stripeInformation)
    {
        return new StripeInformation(
                stripeInformation.getNumberOfRows(),
                stripeInformation.getOffset(),
                stripeInformation.getIndexLength(),
                stripeInformation.getDataLength(),
                stripeInformation.getFooterLength());
    }

    @Override
    public StripeFooter readStripeFooter(List<Type> types, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.StripeFooter stripeFooter = OrcProto.StripeFooter.parseFrom(input);
        return new StripeFooter(toStream(stripeFooter.getStreamsList()), toColumnEncoding(stripeFooter.getColumnsList()));
    }

    public static Stream toStream(OrcProto.Stream stream)
    {
        return new Stream(stream.getColumn(), toStreamKind(stream.getKind()), Ints.checkedCast(stream.getLength()), true);
    }

    public static List<Stream> toStream(List<OrcProto.Stream> streams)
    {
        return ImmutableList.copyOf(Iterables.transform(streams, new Function<OrcProto.Stream, Stream>()
        {
            @Override
            public Stream apply(OrcProto.Stream stream)
            {
                return toStream(stream);
            }
        }));
    }

    public static ColumnEncoding toColumnEncoding(OrcProto.ColumnEncoding columnEncoding)
    {
        return new ColumnEncoding(toColumnEncodingKind(columnEncoding.getKind()), columnEncoding.getDictionarySize());
    }

    public static List<ColumnEncoding> toColumnEncoding(List<OrcProto.ColumnEncoding> columnEncodings)
    {
        return ImmutableList.copyOf(Iterables.transform(columnEncodings, new Function<OrcProto.ColumnEncoding, ColumnEncoding>()
        {
            @Override
            public ColumnEncoding apply(OrcProto.ColumnEncoding columnEncoding)
            {
                return toColumnEncoding(columnEncoding);
            }
        }));
    }

    @Override
    public List<RowGroupIndex> readRowIndexes(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.RowIndex rowIndex = OrcProto.RowIndex.parseFrom(input);
        return ImmutableList.copyOf(Iterables.transform(rowIndex.getEntryList(), new Function<RowIndexEntry, RowGroupIndex>()
        {
            @Override
            public RowGroupIndex apply(OrcProto.RowIndexEntry rowIndexEntry)
            {
                return toRowGroupIndex(rowIndexEntry);
            }
        }));
    }

    private static RowGroupIndex toRowGroupIndex(RowIndexEntry rowIndexEntry)
    {
        return new RowGroupIndex(rowIndexEntry.getPositionsList(), toColumnStatistics(rowIndexEntry.getStatistics()));
    }

    private static ColumnStatistics toColumnStatistics(OrcProto.ColumnStatistics statistics)
    {
        return new ColumnStatistics(
                statistics.getNumberOfValues(),
                toBucketStatistics(statistics.getBucketStatistics()),
                toIntegerStatistics(statistics.getIntStatistics()),
                toDoubleStatistics(statistics.getDoubleStatistics()),
                toStringStatistics(statistics.getStringStatistics()),
                toDateStatistics(statistics.getDateStatistics()));
    }

    public static List<ColumnStatistics> toColumnStatistics(List<OrcProto.ColumnStatistics> columnStatistics)
    {
        if (columnStatistics == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(Iterables.transform(columnStatistics, new Function<OrcProto.ColumnStatistics, ColumnStatistics>()
        {
            @Override
            public ColumnStatistics apply(OrcProto.ColumnStatistics columnStatistics)
            {
                return toColumnStatistics(columnStatistics);
            }
        }));
    }

    private static BucketStatistics toBucketStatistics(OrcProto.BucketStatistics bucketStatistics)
    {
        return new BucketStatistics(bucketStatistics.getCountList());
    }

    private static IntegerStatistics toIntegerStatistics(OrcProto.IntegerStatistics integerStatistics)
    {
        return new IntegerStatistics(
                integerStatistics.hasMinimum() ? integerStatistics.getMinimum() : null,
                integerStatistics.hasMaximum() ? integerStatistics.getMaximum() : null);
    }

    private static DoubleStatistics toDoubleStatistics(OrcProto.DoubleStatistics doubleStatistics)
    {
        return new DoubleStatistics(
                doubleStatistics.hasMinimum() ? doubleStatistics.getMinimum() : null,
                doubleStatistics.hasMaximum() ? doubleStatistics.getMaximum() : null);
    }

    private static StringStatistics toStringStatistics(OrcProto.StringStatistics stringStatistics)
    {
        return new StringStatistics(
                stringStatistics.hasMinimum() ? stringStatistics.getMinimum() : null,
                stringStatistics.hasMaximum() ? stringStatistics.getMaximum() : null);
    }

    private static DateStatistics toDateStatistics(OrcProto.DateStatistics dateStatistics)
    {
        return new DateStatistics(
                dateStatistics.hasMinimum() ? dateStatistics.getMinimum() : null,
                dateStatistics.hasMaximum() ? dateStatistics.getMaximum() : null);
    }

    public static Type toType(OrcProto.Type type)
    {
        return new Type(toTypeKind(type.getKind()), type.getSubtypesList(), type.getFieldNamesList());
    }

    public static List<Type> toType(List<OrcProto.Type> types)
    {
        return ImmutableList.copyOf(Iterables.transform(types, new Function<OrcProto.Type, Type>()
        {
            @Override
            public Type apply(OrcProto.Type type)
            {
                return toType(type);
            }
        }));
    }

    public static Type.Kind toTypeKind(OrcProto.Type.Kind kind)
    {
        switch (kind) {
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
            case DECIMAL:
                return Type.Kind.DECIMAL;
            case DATE:
                return Type.Kind.DATE;
            case VARCHAR:
                return Type.Kind.VARCHAR;
            case CHAR:
                return Type.Kind.CHAR;
            default:
                throw new IllegalStateException(kind + " stream type not implemented yet");
        }
    }

    private static Stream.Kind toStreamKind(OrcProto.Stream.Kind kind)
    {
        switch (kind) {
            case PRESENT:
                return Stream.Kind.PRESENT;
            case DATA:
                return Stream.Kind.DATA;
            case LENGTH:
                return Stream.Kind.LENGTH;
            case DICTIONARY_DATA:
                return Stream.Kind.DICTIONARY_DATA;
            case DICTIONARY_COUNT:
                return Stream.Kind.DICTIONARY_COUNT;
            case SECONDARY:
                return Stream.Kind.SECONDARY;
            case ROW_INDEX:
                return Stream.Kind.ROW_INDEX;
            default:
                throw new IllegalStateException(kind + " stream type not implemented yet");
        }
    }

    private static ColumnEncoding.Kind toColumnEncodingKind(OrcProto.ColumnEncoding.Kind kind)
    {
        switch (kind) {
            case DIRECT:
                return ColumnEncoding.Kind.DIRECT;
            case DIRECT_V2:
                return ColumnEncoding.Kind.DIRECT_V2;
            case DICTIONARY:
                return ColumnEncoding.Kind.DICTIONARY;
            case DICTIONARY_V2:
                return ColumnEncoding.Kind.DICTIONARY_V2;
            default:
                throw new IllegalStateException(kind + " stream encoding not implemented yet");
        }
    }

    public static CompressionKind toCompression(OrcProto.CompressionKind compression)
    {
        switch (compression) {
            case NONE:
                return UNCOMPRESSED;
            case ZLIB:
                return ZLIB;
            case SNAPPY:
                return SNAPPY;
            default:
                throw new IllegalStateException(compression + " compression not implemented yet");
        }
    }
}
