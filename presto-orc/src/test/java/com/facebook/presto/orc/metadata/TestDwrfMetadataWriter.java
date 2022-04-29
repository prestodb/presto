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
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT;
import static com.facebook.presto.orc.metadata.DwrfMetadataWriter.toColumnEncoding;
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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
