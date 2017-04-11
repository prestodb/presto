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
package com.facebook.presto.hive.parquet.predicate;

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;
import parquet.column.Encoding;

import java.util.Set;

import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.isOnlyDictionaryEncodingPages;
import static com.google.common.collect.Sets.union;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static parquet.column.Encoding.BIT_PACKED;
import static parquet.column.Encoding.PLAIN;
import static parquet.column.Encoding.PLAIN_DICTIONARY;
import static parquet.column.Encoding.RLE;

public class TestParquetPredicateUtils
{
    @Test
    @SuppressWarnings("deprecation")
    public void testDictionaryEncodingCasesV1()
    {
        Set<Encoding> required = ImmutableSet.of(BIT_PACKED);
        Set<Encoding> optional = ImmutableSet.of(BIT_PACKED, RLE);
        Set<Encoding> repeated = ImmutableSet.of(RLE);

        Set<Encoding> notDictionary = ImmutableSet.of(PLAIN);
        Set<Encoding> mixedDictionary = ImmutableSet.of(PLAIN_DICTIONARY, PLAIN);
        Set<Encoding> dictionary = ImmutableSet.of(PLAIN_DICTIONARY);

        assertFalse(isOnlyDictionaryEncodingPages(union(required, notDictionary)), "required notDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(union(optional, notDictionary)), "optional notDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(union(repeated, notDictionary)), "repeated notDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(union(required, mixedDictionary)), "required mixedDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(union(optional, mixedDictionary)), "optional mixedDictionary");
        assertFalse(isOnlyDictionaryEncodingPages(union(repeated, mixedDictionary)), "repeated mixedDictionary");
        assertTrue(isOnlyDictionaryEncodingPages(union(required, dictionary)), "required dictionary");
        assertTrue(isOnlyDictionaryEncodingPages(union(optional, dictionary)), "optional dictionary");
        assertTrue(isOnlyDictionaryEncodingPages(union(repeated, dictionary)), "repeated dictionary");
    }
}
