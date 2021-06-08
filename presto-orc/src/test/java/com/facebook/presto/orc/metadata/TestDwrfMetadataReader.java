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

import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.ByteString;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.OrcMetadataReader.maxStringTruncateToValidRange;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.minStringTruncateToValidRange;
import static com.facebook.presto.orc.metadata.TestOrcMetadataReader.ALL_UTF8_SEQUENCES;
import static com.facebook.presto.orc.metadata.TestOrcMetadataReader.TEST_CODE_POINTS;
import static com.facebook.presto.orc.metadata.TestOrcMetadataReader.concatSlice;
import static com.facebook.presto.orc.proto.DwrfProto.Stream.Kind.DATA;
import static io.airlift.slice.SliceUtf8.codePointToUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;

public class TestDwrfMetadataReader
{
    private final long footerLength = 10;
    private final long compressionBlockSize = 8192;
    private final DwrfMetadataReader dwrfMetadataReader = new DwrfMetadataReader();
    private final DwrfProto.PostScript baseProtoPostScript = DwrfProto.PostScript.newBuilder()
            .setWriterVersion(HiveWriterVersion.ORC_HIVE_8732.getOrcWriterVersion())
            .setFooterLength(footerLength)
            .setCompression(DwrfProto.CompressionKind.ZSTD)
            .setCompressionBlockSize(compressionBlockSize)
            .setCacheSize(12)
            .setCacheMode(DwrfProto.StripeCacheMode.BOTH)
            .build();

    @Test
    public void testReadPostScript()
            throws IOException
    {
        byte[] data = baseProtoPostScript.toByteArray();

        PostScript postScript = dwrfMetadataReader.readPostScript(data, 0, data.length);
        assertEquals(postScript.getHiveWriterVersion(), HiveWriterVersion.ORC_HIVE_8732);
        assertEquals(postScript.getFooterLength(), footerLength);
        assertEquals(postScript.getCompression(), CompressionKind.ZSTD);
        assertEquals(postScript.getCompressionBlockSize(), compressionBlockSize);
        assertEquals(postScript.getDwrfStripeCacheLength().getAsInt(), 12);
        assertEquals(postScript.getDwrfStripeCacheMode().get(), DwrfStripeCacheMode.INDEX_AND_FOOTER);
    }

    @Test
    public void testReadPostScriptNoDwrfStripeCache()
            throws IOException
    {
        DwrfProto.PostScript protoPostScript = baseProtoPostScript.toBuilder()
                .clearCacheSize()
                .clearCacheMode()
                .build();
        byte[] data = protoPostScript.toByteArray();

        PostScript postScript = dwrfMetadataReader.readPostScript(data, 0, data.length);
        assertFalse(postScript.getDwrfStripeCacheLength().isPresent());
        assertFalse(postScript.getDwrfStripeCacheMode().isPresent());
    }

    @Test
    public void testReadPostScriptMissingDwrfStripeCacheLength()
            throws IOException
    {
        DwrfProto.PostScript protoPostScript = baseProtoPostScript.toBuilder()
                .clearCacheSize()
                .build();
        byte[] data = protoPostScript.toByteArray();

        PostScript postScript = dwrfMetadataReader.readPostScript(data, 0, data.length);
        assertFalse(postScript.getDwrfStripeCacheLength().isPresent());
        assertFalse(postScript.getDwrfStripeCacheMode().isPresent());
    }

    @Test
    public void testReadPostScriptMissingDwrfStripeCacheMode()
            throws IOException
    {
        DwrfProto.PostScript protoPostScript = baseProtoPostScript.toBuilder()
                .clearCacheMode()
                .build();
        byte[] data = protoPostScript.toByteArray();

        PostScript postScript = dwrfMetadataReader.readPostScript(data, 0, data.length);
        assertFalse(postScript.getDwrfStripeCacheLength().isPresent());
        assertFalse(postScript.getDwrfStripeCacheMode().isPresent());
    }

    @Test
    public void testToStripeCacheMode()
    {
        assertEquals(DwrfMetadataReader.toStripeCacheMode(DwrfProto.StripeCacheMode.INDEX), DwrfStripeCacheMode.INDEX);
        assertEquals(DwrfMetadataReader.toStripeCacheMode(DwrfProto.StripeCacheMode.FOOTER), DwrfStripeCacheMode.FOOTER);
        assertEquals(DwrfMetadataReader.toStripeCacheMode(DwrfProto.StripeCacheMode.BOTH), DwrfStripeCacheMode.INDEX_AND_FOOTER);
        assertEquals(DwrfMetadataReader.toStripeCacheMode(DwrfProto.StripeCacheMode.NA), DwrfStripeCacheMode.NONE);
    }

    @Test
    public void testReadFooter()
            throws IOException
    {
        long numberOfRows = 10;
        int rowIndexStride = 11;
        List<Integer> stripeCacheOffsets = ImmutableList.of(1, 2, 3);

        DwrfProto.Footer protoFooter = DwrfProto.Footer.newBuilder()
                .setNumberOfRows(numberOfRows)
                .setRowIndexStride(rowIndexStride)
                .addAllStripeCacheOffsets(stripeCacheOffsets)
                .build();
        byte[] data = protoFooter.toByteArray();
        InputStream inputStream = new ByteArrayInputStream(data);
        OrcDataSource orcDataSource = null; // orcDataSource only needed for encrypted files

        Footer footer = dwrfMetadataReader.readFooter(HiveWriterVersion.ORC_HIVE_8732,
                inputStream,
                DwrfEncryptionProvider.NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                orcDataSource,
                Optional.empty());

        assertEquals(footer.getNumberOfRows(), numberOfRows);
        assertEquals(footer.getRowsInRowGroup(), rowIndexStride);
        assertEquals(footer.getDwrfStripeCacheOffsets().get(), stripeCacheOffsets);
    }

    @Test
    public void testReadStripeFooterThrowsForLargeStreams()
    {
        DwrfProto.Stream stream = DwrfProto.Stream.newBuilder()
                .setKind(DATA)
                .setLength(Long.MAX_VALUE)
                .build();
        DwrfProto.StripeFooter protoStripeFooter = DwrfProto.StripeFooter.newBuilder()
                .addStreams(stream)
                .build();
        byte[] data = protoStripeFooter.toByteArray();
        InputStream inputStream = new ByteArrayInputStream(data);

        OrcDataSourceId orcDataSourceId = new OrcDataSourceId("test");
        OrcCorruptionException ex = expectThrows(OrcCorruptionException.class, () -> dwrfMetadataReader.readStripeFooter(orcDataSourceId, ImmutableList.of(), inputStream));
        assertEquals(ex.getMessage(), "java.io.IOException: Malformed ORC file. Stream size 9223372036854775807 of one of the streams for column 0 is larger than supported size 2147483647 [test]");
    }

    @Test
    public void testToStringStatistics()
    {
        // ORIGINAL version only produces stats at the row group level
        assertNull(DwrfMetadataReader.toStringStatistics(
                HiveWriterVersion.ORIGINAL,
                DwrfProto.StringStatistics.newBuilder()
                        .setMinimum("ant")
                        .setMaximum("cat")
                        .setSum(44)
                        .build(),
                false));

        // having only sum should work for current version
        for (boolean isRowGroup : ImmutableList.of(true, false)) {
            assertEquals(
                    DwrfMetadataReader.toStringStatistics(
                            HiveWriterVersion.ORC_HIVE_8732,
                            DwrfProto.StringStatistics.newBuilder()
                                    .setSum(45)
                                    .build(),
                            isRowGroup),
                    new StringStatistics(null, null, 45));
        }
        // and the ORIGINAL version row group stats (but not rolled up stats)
        assertEquals(
                DwrfMetadataReader.toStringStatistics(
                        HiveWriterVersion.ORIGINAL,
                        DwrfProto.StringStatistics.newBuilder()
                                .setSum(45)
                                .build(),
                        true),
                new StringStatistics(null, null, 45));

        // having only a min or max should work
        assertEquals(
                DwrfMetadataReader.toStringStatistics(
                        HiveWriterVersion.ORC_HIVE_8732,
                        DwrfProto.StringStatistics.newBuilder()
                                .setMinimum("ant")
                                .build(),
                        true),
                new StringStatistics(Slices.utf8Slice("ant"), null, 0));
        assertEquals(
                DwrfMetadataReader.toStringStatistics(
                        HiveWriterVersion.ORC_HIVE_8732,
                        DwrfProto.StringStatistics.newBuilder()
                                .setMaximum("cat")
                                .build(),
                        true),
                new StringStatistics(null, Slices.utf8Slice("cat"), 0));

        // normal full stat
        assertEquals(
                DwrfMetadataReader.toStringStatistics(
                        HiveWriterVersion.ORC_HIVE_8732,
                        DwrfProto.StringStatistics.newBuilder()
                                .setMinimum("ant")
                                .setMaximum("cat")
                                .setSum(79)
                                .build(),
                        true),
                new StringStatistics(Slices.utf8Slice("ant"), Slices.utf8Slice("cat"), 79));

        for (Slice prefix : ALL_UTF8_SEQUENCES) {
            for (int testCodePoint : TEST_CODE_POINTS) {
                Slice codePoint = codePointToUtf8(testCodePoint);
                for (Slice suffix : ALL_UTF8_SEQUENCES) {
                    Slice testValue = concatSlice(prefix, codePoint, suffix);
                    testStringStatisticsTruncation(testValue, HiveWriterVersion.ORIGINAL);
                    testStringStatisticsTruncation(testValue, HiveWriterVersion.ORC_HIVE_8732);
                }
            }
        }
    }

    private static void testStringStatisticsTruncation(Slice testValue, HiveWriterVersion version)
    {
        assertEquals(
                DwrfMetadataReader.toStringStatistics(
                        version,
                        DwrfProto.StringStatistics.newBuilder()
                                .setMinimumBytes(ByteString.copyFrom(testValue.getBytes()))
                                .setMaximumBytes(ByteString.copyFrom(testValue.getBytes()))
                                .setSum(79)
                                .build(),
                        true),
                createExpectedStringStatistics(version, testValue, testValue, 79));
        assertEquals(
                DwrfMetadataReader.toStringStatistics(
                        version,
                        DwrfProto.StringStatistics.newBuilder()
                                .setMinimumBytes(ByteString.copyFrom(testValue.getBytes()))
                                .setSum(79)
                                .build(),
                        true),
                createExpectedStringStatistics(version, testValue, null, 79));
        assertEquals(
                DwrfMetadataReader.toStringStatistics(
                        version,
                        DwrfProto.StringStatistics.newBuilder()
                                .setMaximumBytes(ByteString.copyFrom(testValue.getBytes()))
                                .setSum(79)
                                .build(),
                        true),
                createExpectedStringStatistics(version, null, testValue, 79));
    }

    private static StringStatistics createExpectedStringStatistics(HiveWriterVersion version, Slice min, Slice max, int sum)
    {
        return new StringStatistics(
                minStringTruncateToValidRange(min, version),
                maxStringTruncateToValidRange(max, version),
                sum);
    }
}
