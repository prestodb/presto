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

import parquet.column.Encoding;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.isOnlyDictionaryEncodingPages;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestParquetPredicateUtils
{
    @Test
    public void testDictionaryEncodingCasesV1()
    {
        Set<Encoding> required = new HashSet<>();
        required.add(Encoding.BIT_PACKED); // max rl = 0, max dl = 0
        Set<Encoding> optional = new HashSet<>();
        optional.add(Encoding.BIT_PACKED); // max rl = 0
        optional.add(Encoding.RLE);        // max dl = 1
        Set<Encoding> repeated = new HashSet<>();
        repeated.add(Encoding.RLE);        // max rl = 1, dl = 1

        Set<Encoding> plain = new HashSet<>();
        plain.add(Encoding.PLAIN);               // no dictionary pages
        Set<Encoding> fallback = new HashSet<>();
        fallback.add(Encoding.PLAIN_DICTIONARY); // some dictionary-encoded data pages
        fallback.add(Encoding.PLAIN);            // some non-dictionary data pages
        Set<Encoding> dictionary = new HashSet<>();
        dictionary.add(Encoding.PLAIN_DICTIONARY); // both dictionary and data pages use PLAIN_DICTIONARY

        assertFalse(isOnlyDictionaryEncodingPages(union(required, plain)), "required plain");
        assertFalse(isOnlyDictionaryEncodingPages(union(optional, plain)), "optional plain");
        assertFalse(isOnlyDictionaryEncodingPages(union(repeated, plain)), "repeated plain");
        assertFalse(isOnlyDictionaryEncodingPages(union(required, fallback)), "required fallback");
        assertFalse(isOnlyDictionaryEncodingPages(union(optional, fallback)), "optional fallback");
        assertFalse(isOnlyDictionaryEncodingPages(union(repeated, fallback)), "repeated fallback");
        assertTrue(isOnlyDictionaryEncodingPages(union(required, dictionary)), "required dictionary");
        assertTrue(isOnlyDictionaryEncodingPages(union(optional, dictionary)), "optional dictionary");
        assertTrue(isOnlyDictionaryEncodingPages(union(repeated, dictionary)), "repeated dictionary");
    }

    private <T> Set<T> union(Set<T> s1, Set<T> s2)
    {
        Set<T> r = new HashSet<>();
        r.addAll(s1);
        r.addAll(s2);
        return r;
    }
}
