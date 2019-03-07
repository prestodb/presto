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
package com.facebook.presto.parquet.predicate;

import com.google.common.collect.ImmutableSet;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.parquet.predicate.PredicateUtils.isOnlyDictionaryEncodingPages;
import static com.google.common.collect.Sets.union;
import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.hadoop.metadata.ColumnPath.fromDotString;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPredicateUtils
{
    @Test
    @SuppressWarnings("deprecation")
    public void testDictionaryEncodingV1()
    {
        Set<Encoding> required = ImmutableSet.of(BIT_PACKED);
        Set<Encoding> optional = ImmutableSet.of(BIT_PACKED, RLE);
        Set<Encoding> repeated = ImmutableSet.of(RLE);

        Set<Encoding> notDictionary = ImmutableSet.of(PLAIN);
        Set<Encoding> mixedDictionary = ImmutableSet.of(PLAIN_DICTIONARY, PLAIN);
        Set<Encoding> dictionary = ImmutableSet.of(PLAIN_DICTIONARY);

        assertFalse(isOnlyDictionaryEncodingPages(createColumnMetaDataV1(union(required, notDictionary))), "required notDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(createColumnMetaDataV1(union(optional, notDictionary))), "optional notDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(createColumnMetaDataV1(union(repeated, notDictionary))), "repeated notDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(createColumnMetaDataV1(union(required, mixedDictionary))), "required mixedDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(createColumnMetaDataV1(union(optional, mixedDictionary))), "optional mixedDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(createColumnMetaDataV1(union(repeated, mixedDictionary))), "repeated mixedDictionary");
        assertTrue(isOnlyDictionaryEncodingPages(createColumnMetaDataV1(union(required, dictionary))), "required dictionary");
        assertTrue(isOnlyDictionaryEncodingPages(createColumnMetaDataV1(union(optional, dictionary))), "optional dictionary");
        assertTrue(isOnlyDictionaryEncodingPages(createColumnMetaDataV1(union(repeated, dictionary))), "repeated dictionary");
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDictionaryEncodingV2()
    {
        assertTrue(isOnlyDictionaryEncodingPages(createColumnMetaDataV2(RLE_DICTIONARY)));
        assertTrue(isOnlyDictionaryEncodingPages(createColumnMetaDataV2(PLAIN_DICTIONARY)));
        assertFalse(isOnlyDictionaryEncodingPages(createColumnMetaDataV2(PLAIN)));

        // Simulate fallback to plain encoding e.g. too many unique entries
        assertFalse(isOnlyDictionaryEncodingPages(createColumnMetaDataV2(RLE_DICTIONARY, PLAIN)));
    }

    private ColumnChunkMetaData createColumnMetaDataV2(Encoding... dataEncodings)
    {
        EncodingStats encodingStats = new EncodingStats.Builder()
                .withV2Pages()
                .addDictEncoding(PLAIN)
                .addDataEncodings(ImmutableSet.copyOf(dataEncodings)).build();

        return ColumnChunkMetaData.get(fromDotString("column"), BINARY, UNCOMPRESSED, encodingStats, encodingStats.getDataEncodings(), new BinaryStatistics(), 0, 0, 1, 1, 1);
    }

    @SuppressWarnings("deprecation")
    private ColumnChunkMetaData createColumnMetaDataV1(Set<Encoding> encodings)
    {
        return ColumnChunkMetaData.get(fromDotString("column"), BINARY, UNCOMPRESSED, encodings, new BinaryStatistics(), 0, 0, 1, 1, 1);
    }
}
