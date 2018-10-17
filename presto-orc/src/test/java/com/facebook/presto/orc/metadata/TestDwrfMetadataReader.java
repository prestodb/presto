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

import com.facebook.presto.orc.metadata.PostScript.HiveWriterVersion;
import com.facebook.presto.orc.metadata.statistics.StringStatistics;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.ByteString;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.orc.metadata.OrcMetadataReader.maxStringTruncateToValidRange;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.minStringTruncateToValidRange;
import static com.facebook.presto.orc.metadata.TestOrcMetadataReader.ALL_UTF8_SEQUENCES;
import static com.facebook.presto.orc.metadata.TestOrcMetadataReader.TEST_CODE_POINTS;
import static com.facebook.presto.orc.metadata.TestOrcMetadataReader.concatSlice;
import static io.airlift.slice.SliceUtf8.codePointToUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestDwrfMetadataReader
{
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
