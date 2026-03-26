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
package com.facebook.presto.lance;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static java.lang.Float.floatToIntBits;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLanceSqlFilterBuilder
{
    @Test
    public void testAllDomain()
    {
        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(TupleDomain.all());
        assertFalse(filter.isPresent());
    }

    @Test
    public void testNoneDomain()
    {
        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(TupleDomain.none());
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "false");
    }

    @Test
    public void testBigintEquality()
    {
        LanceColumnHandle column = new LanceColumnHandle("id", BIGINT);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.singleValue(BIGINT, 42L)));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`id` = 42");
    }

    @Test
    public void testIntegerEquality()
    {
        LanceColumnHandle column = new LanceColumnHandle("count", INTEGER);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.singleValue(INTEGER, 10L)));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`count` = 10");
    }

    @Test
    public void testInList()
    {
        LanceColumnHandle column = new LanceColumnHandle("id", BIGINT);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.multipleValues(BIGINT, com.google.common.collect.ImmutableList.of(1L, 2L, 3L))));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`id` IN (1, 2, 3)");
    }

    @Test
    public void testRange()
    {
        LanceColumnHandle column = new LanceColumnHandle("value", BIGINT);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(BIGINT, 10L, false, 20L, false)),
                false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, domain));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "(`value` > 10 AND `value` < 20)");
    }

    @Test
    public void testRangeInclusive()
    {
        LanceColumnHandle column = new LanceColumnHandle("value", BIGINT);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(BIGINT, 10L, true, 20L, true)),
                false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, domain));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "(`value` >= 10 AND `value` <= 20)");
    }

    @Test
    public void testVarcharEquality()
    {
        LanceColumnHandle column = new LanceColumnHandle("name", VarcharType.VARCHAR);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("hello"))));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`name` = 'hello'");
    }

    @Test
    public void testVarcharWithSingleQuote()
    {
        LanceColumnHandle column = new LanceColumnHandle("name", VarcharType.VARCHAR);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.singleValue(VarcharType.VARCHAR, Slices.utf8Slice("it's"))));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`name` = 'it''s'");
    }

    @Test
    public void testDateEquality()
    {
        LanceColumnHandle column = new LanceColumnHandle("dt", DATE);
        // 2024-01-15 is day 19737 since epoch
        long daysSinceEpoch = java.time.LocalDate.of(2024, 1, 15).toEpochDay();
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.singleValue(DATE, daysSinceEpoch)));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`dt` = date '2024-01-15'");
    }

    @Test
    public void testIsNull()
    {
        LanceColumnHandle column = new LanceColumnHandle("col", BIGINT);
        Domain domain = Domain.onlyNull(BIGINT);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, domain));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`col` IS NULL");
    }

    @Test
    public void testMultipleColumnsWithAnd()
    {
        LanceColumnHandle col1 = new LanceColumnHandle("a", BIGINT);
        LanceColumnHandle col2 = new LanceColumnHandle("b", BIGINT);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        col1, Domain.singleValue(BIGINT, 1L),
                        col2, Domain.singleValue(BIGINT, 2L)));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        // Order may vary, so check both parts are present
        String result = filter.get();
        assertTrue(result.contains("`a` = 1"));
        assertTrue(result.contains("`b` = 2"));
        assertTrue(result.contains(" AND "));
    }

    @Test
    public void testBooleanTrue()
    {
        LanceColumnHandle column = new LanceColumnHandle("flag", BOOLEAN);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.singleValue(BOOLEAN, true)));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`flag` = true");
    }

    @Test
    public void testBooleanFalse()
    {
        LanceColumnHandle column = new LanceColumnHandle("flag", BOOLEAN);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.singleValue(BOOLEAN, false)));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`flag` = false");
    }

    @Test
    public void testDoubleEquality()
    {
        LanceColumnHandle column = new LanceColumnHandle("price", DOUBLE);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.singleValue(DOUBLE, 3.14)));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`price` = 3.14");
    }

    @Test
    public void testNullableWithValues()
    {
        LanceColumnHandle column = new LanceColumnHandle("id", BIGINT);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.equal(BIGINT, 42L)),
                true);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, domain));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "(`id` = 42 OR `id` IS NULL)");
    }

    @Test
    public void testGreaterThan()
    {
        LanceColumnHandle column = new LanceColumnHandle("value", BIGINT);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.greaterThan(BIGINT, 10L)),
                false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, domain));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`value` > 10");
    }

    @Test
    public void testLessThanOrEqual()
    {
        LanceColumnHandle column = new LanceColumnHandle("value", BIGINT);
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 100L)),
                false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, domain));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`value` <= 100");
    }

    @Test
    public void testAllDomainForColumn()
    {
        LanceColumnHandle column = new LanceColumnHandle("id", BIGINT);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.all(BIGINT)));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertFalse(filter.isPresent());
    }

    @Test
    public void testMultipleRangesOrDisjunction()
    {
        LanceColumnHandle column = new LanceColumnHandle("value", BIGINT);
        Domain domain = Domain.create(
                ValueSet.ofRanges(
                        Range.range(BIGINT, 1L, true, 5L, true),
                        Range.range(BIGINT, 10L, true, 20L, true)),
                false);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, domain));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "((`value` >= 1 AND `value` <= 5) OR (`value` >= 10 AND `value` <= 20))");
    }

    @Test
    public void testValuesNoneNotNullable()
    {
        // #2: values.isNone() + !isNullAllowed() should produce "false", not empty
        LanceColumnHandle column = new LanceColumnHandle("col", BIGINT);
        Domain domain = Domain.none(BIGINT);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, domain));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "false");
    }

    @Test
    public void testRealEquality()
    {
        LanceColumnHandle column = new LanceColumnHandle("score", RealType.REAL);
        long floatBits = floatToIntBits(3.14f);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.singleValue(RealType.REAL, floatBits)));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`score` = 3.14");
    }

    @Test
    public void testTimestampEquality()
    {
        LanceColumnHandle column = new LanceColumnHandle("ts", TimestampType.TIMESTAMP);
        // 2024-01-15 10:30:00 UTC in microseconds since epoch
        long micros = 1705314600000000L;
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(column, Domain.singleValue(TimestampType.TIMESTAMP, micros)));

        Optional<String> filter = LanceSqlFilterBuilder.buildFilter(tupleDomain);
        assertTrue(filter.isPresent());
        assertEquals(filter.get(), "`ts` = timestamp '2024-01-15 10:30:00.000000'");
    }
}
