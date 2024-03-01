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

import com.facebook.presto.orc.proto.DwrfProto;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.toColumnEncoding;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.toColumnEncodings;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.toStream;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.toStreamKind;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_COUNT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_MAP;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.ROW_INDEX;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.protobuf.ByteString.copyFromUtf8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestDwrfMetadataWriter
{
    private static final int COLUMN_ID = 3;

    @Test
    public void testToColumnEncodingDirect()
    {
        int expectedDictionarySize = 0;
        ColumnEncoding columnEncoding = new ColumnEncoding(DIRECT, expectedDictionarySize);

        DwrfProto.ColumnEncoding actual = toColumnEncoding(COLUMN_ID, columnEncoding);

        assertEquals(actual.getColumn(), COLUMN_ID);
        assertEquals(actual.getKind(), DwrfProto.ColumnEncoding.Kind.DIRECT);
        assertEquals(actual.getDictionarySize(), expectedDictionarySize);
        assertEquals(actual.getSequence(), 0);
    }

    @Test
    public void testToColumnEncodingDictionary()
    {
        int expectedDictionarySize = 5;
        ColumnEncoding columnEncoding = new ColumnEncoding(DICTIONARY, expectedDictionarySize);

        DwrfProto.ColumnEncoding actual = toColumnEncoding(COLUMN_ID, columnEncoding);

        assertEquals(actual.getColumn(), COLUMN_ID);
        assertEquals(actual.getKind(), DwrfProto.ColumnEncoding.Kind.DICTIONARY);
        assertEquals(actual.getDictionarySize(), expectedDictionarySize);
        assertEquals(actual.getSequence(), 0);
    }

    @Test
    public void testToColumnEncodingFlatMap()
    {
        int expectedDictionarySize = 0;
        ColumnEncoding columnEncoding = new ColumnEncoding(DWRF_MAP_FLAT, expectedDictionarySize);

        DwrfProto.ColumnEncoding actual = toColumnEncoding(COLUMN_ID, columnEncoding);

        assertEquals(actual.getColumn(), COLUMN_ID);
        assertEquals(actual.getKind(), DwrfProto.ColumnEncoding.Kind.MAP_FLAT);
        assertEquals(actual.getDictionarySize(), expectedDictionarySize);
        assertEquals(actual.getSequence(), 0);
    }

    @DataProvider
    public static Object[][] sequenceKeyProvider()
    {
        return new Object[][] {
                {DwrfProto.KeyInfo.newBuilder().setIntKey(1).build(), DwrfProto.KeyInfo.newBuilder().setIntKey(5).build()},
                {DwrfProto.KeyInfo.newBuilder().setBytesKey(copyFromUtf8("key1")).build(), DwrfProto.KeyInfo.newBuilder().setBytesKey(copyFromUtf8("key2")).build()}
        };
    }

    @Test(dataProvider = "sequenceKeyProvider")
    public void testToColumnEncodingsWithSequence(DwrfProto.KeyInfo key1, DwrfProto.KeyInfo key2)
    {
        int expectedDictionarySize1 = 5;
        int expectedSequenceId1 = 0;
        ColumnEncoding valueEncoding1 = new ColumnEncoding(DIRECT, expectedDictionarySize1);
        DwrfSequenceEncoding sequenceEncoding1 = new DwrfSequenceEncoding(key1, valueEncoding1);

        int expectedDictionarySize2 = 10;
        int expectedSequenceId2 = 5;
        ColumnEncoding valueEncoding2 = new ColumnEncoding(DICTIONARY, expectedDictionarySize2);
        DwrfSequenceEncoding sequenceEncoding2 = new DwrfSequenceEncoding(key2, valueEncoding2);

        ImmutableSortedMap<Integer, DwrfSequenceEncoding> additionalSequenceEncodings = ImmutableSortedMap.of(
                expectedSequenceId1, sequenceEncoding1,
                expectedSequenceId2, sequenceEncoding2);
        ColumnEncoding columnEncoding = new ColumnEncoding(DIRECT, 0, Optional.of(additionalSequenceEncodings));

        List<DwrfProto.ColumnEncoding> actual = toColumnEncodings(ImmutableMap.of(COLUMN_ID, columnEncoding));
        assertEquals(actual.size(), 2);

        DwrfProto.ColumnEncoding actualValueEncoding1 = actual.get(0);
        assertEquals(actualValueEncoding1.getColumn(), COLUMN_ID);
        assertEquals(actualValueEncoding1.getKind(), DwrfProto.ColumnEncoding.Kind.DIRECT);
        assertEquals(actualValueEncoding1.getDictionarySize(), expectedDictionarySize1);

        assertEquals(actualValueEncoding1.getSequence(), expectedSequenceId1);
        assertEquals(actualValueEncoding1.getKey(), key1);

        DwrfProto.ColumnEncoding actualValueEncoding2 = actual.get(1);
        assertEquals(actualValueEncoding2.getColumn(), COLUMN_ID);
        assertEquals(actualValueEncoding2.getKind(), DwrfProto.ColumnEncoding.Kind.DICTIONARY);
        assertEquals(actualValueEncoding2.getDictionarySize(), expectedDictionarySize2);
        assertEquals(actualValueEncoding2.getSequence(), expectedSequenceId2);
        assertEquals(actualValueEncoding2.getKey(), key2);
    }

    @Test
    public void testToColumnEncodingsWithInvalidDeeplyNestedAdditionalSequence()
    {
        DwrfProto.KeyInfo key1 = DwrfProto.KeyInfo.newBuilder().setIntKey(1).build();
        DwrfProto.KeyInfo key2 = DwrfProto.KeyInfo.newBuilder().setIntKey(2).build();

        // level 2
        ColumnEncoding deeplyNestedValueEncoding = new ColumnEncoding(DIRECT, 0);
        DwrfSequenceEncoding deeplyNestedSequenceEncoding = new DwrfSequenceEncoding(key1, deeplyNestedValueEncoding);
        ImmutableSortedMap<Integer, DwrfSequenceEncoding> deeplyNestedSequenceEncodings = ImmutableSortedMap.of(0, deeplyNestedSequenceEncoding);

        // level 1
        ColumnEncoding nestedColumnEncoding = new ColumnEncoding(DIRECT, 0, Optional.of(deeplyNestedSequenceEncodings));
        DwrfSequenceEncoding nestedSequenceEncoding = new DwrfSequenceEncoding(key2, nestedColumnEncoding);
        ImmutableSortedMap<Integer, DwrfSequenceEncoding> nestedSequenceEncodings = ImmutableSortedMap.of(0, nestedSequenceEncoding);

        // root
        ColumnEncoding columnEncoding = new ColumnEncoding(DIRECT, 0, Optional.of(nestedSequenceEncodings));

        expectThrows(IllegalArgumentException.class, () -> toColumnEncodings(ImmutableMap.of(COLUMN_ID, columnEncoding)));
    }

    @Test
    public void testToStreamKind()
    {
        assertEquals(toStreamKind(PRESENT), DwrfProto.Stream.Kind.PRESENT);
        assertEquals(toStreamKind(IN_MAP), DwrfProto.Stream.Kind.IN_MAP);
        assertEquals(toStreamKind(DATA), DwrfProto.Stream.Kind.DATA);
        assertEquals(toStreamKind(SECONDARY), DwrfProto.Stream.Kind.NANO_DATA);
        assertEquals(toStreamKind(LENGTH), DwrfProto.Stream.Kind.LENGTH);
        assertEquals(toStreamKind(DICTIONARY_DATA), DwrfProto.Stream.Kind.DICTIONARY_DATA);
        assertEquals(toStreamKind(DICTIONARY_COUNT), DwrfProto.Stream.Kind.DICTIONARY_COUNT);
        assertEquals(toStreamKind(ROW_INDEX), DwrfProto.Stream.Kind.ROW_INDEX);
    }

    @Test
    public void testToStream()
    {
        int expectedSequence = 10;
        int expectedLength = 15;
        long expectedOffset = 25;
        boolean expectedUseVints = true;

        Stream stream = new Stream(COLUMN_ID, expectedSequence, DATA, expectedLength, expectedUseVints);
        DwrfProto.Stream actual = toStream(stream);
        assertEquals(actual.getColumn(), COLUMN_ID);
        assertEquals(actual.getSequence(), expectedSequence);
        assertEquals(actual.getKind(), DwrfProto.Stream.Kind.DATA);
        assertEquals(actual.getLength(), expectedLength);
        assertTrue(actual.getUseVInts());
        assertFalse(actual.hasOffset());

        stream = new Stream(COLUMN_ID, DATA, expectedLength, expectedUseVints, expectedSequence, Optional.of(expectedOffset));
        actual = toStream(stream);
        assertEquals(actual.getColumn(), COLUMN_ID);
        assertEquals(actual.getSequence(), expectedSequence);
        assertEquals(actual.getKind(), DwrfProto.Stream.Kind.DATA);
        assertEquals(actual.getLength(), expectedLength);
        assertTrue(actual.getUseVInts());
        assertEquals(actual.getOffset(), expectedOffset);
    }
}
