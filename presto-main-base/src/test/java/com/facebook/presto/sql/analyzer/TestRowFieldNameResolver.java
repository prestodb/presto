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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.type.RowType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.analyzer.RowFieldNameResolver.resolveFieldIndex;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestRowFieldNameResolver
{
    @Test
    public void testExactMatchDisambiguatesCaseVariants()
    {
        RowType rowType = RowType.from(ImmutableList.of(
                RowType.field("currencyCode", VARCHAR),
                RowType.field("currencycode", BIGINT)));

        assertEquals(resolveFieldIndex(rowType, "currencyCode", true), 0);
        assertEquals(resolveFieldIndex(rowType, "currencycode", true), 1);
    }

    @Test
    public void testUniqueCaseInsensitiveFallback()
    {
        RowType rowType = RowType.from(ImmutableList.of(RowType.field("currencyCode", VARCHAR)));

        assertEquals(resolveFieldIndex(rowType, "CURRENCYCODE", true), 0);
        assertEquals(resolveFieldIndex(rowType, "CURRENCYCODE", false), 0);
    }

    @Test
    public void testAmbiguousCaseInsensitiveFallback()
    {
        RowType rowType = RowType.from(ImmutableList.of(
                RowType.field("currencyCode", VARCHAR),
                RowType.field("currencycode", BIGINT)));

        assertThrows(IllegalArgumentException.class, () -> resolveFieldIndex(rowType, "CURRENCYCODE", true));
        assertThrows(IllegalArgumentException.class, () -> resolveFieldIndex(rowType, "currencyCode", false));
        assertThrows(IllegalArgumentException.class, () -> resolveFieldIndex(rowType, "currencycode", false));
    }

    @Test
    public void testAmbiguousDuplicateExactMatch()
    {
        RowType rowType = RowType.from(ImmutableList.of(
                RowType.field("currencyCode", VARCHAR),
                RowType.field("currencyCode", BIGINT)));

        assertThrows(IllegalArgumentException.class, () -> resolveFieldIndex(rowType, "currencyCode", true));
    }

    @Test
    public void testMissingField()
    {
        RowType rowType = RowType.from(ImmutableList.of(RowType.field("currencyCode", VARCHAR)));

        assertEquals(resolveFieldIndex(rowType, "missing", true), -1);
        assertEquals(resolveFieldIndex(rowType, "missing", false), -1);
    }
}
