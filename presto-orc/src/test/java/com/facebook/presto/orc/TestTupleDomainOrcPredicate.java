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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.BooleanStatistics;
import com.facebook.presto.orc.metadata.ColumnStatistics;
import com.facebook.presto.orc.metadata.DateStatistics;
import com.facebook.presto.orc.metadata.DoubleStatistics;
import com.facebook.presto.orc.metadata.IntegerStatistics;
import com.facebook.presto.orc.metadata.StringStatistics;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.presto.orc.TupleDomainOrcPredicate.getDomain;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.getMaxSlice;
import static com.facebook.presto.orc.metadata.OrcMetadataReader.getMinSlice;
import static com.facebook.presto.spi.Domain.all;
import static com.facebook.presto.spi.Domain.create;
import static com.facebook.presto.spi.Domain.none;
import static com.facebook.presto.spi.Domain.notNull;
import static com.facebook.presto.spi.Domain.onlyNull;
import static com.facebook.presto.spi.Domain.singleValue;
import static com.facebook.presto.spi.Range.greaterThanOrEqual;
import static com.facebook.presto.spi.Range.lessThanOrEqual;
import static com.facebook.presto.spi.Range.range;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestTupleDomainOrcPredicate
{
    @Test
    public void testBoolean()
            throws Exception
    {
        assertEquals(getDomain(BOOLEAN, 0, null), none(Boolean.class));
        assertEquals(getDomain(BOOLEAN, 10, null), all(Boolean.class));

        assertEquals(getDomain(BOOLEAN, 0, booleanColumnStats(null, null)), none(Boolean.class));
        assertEquals(getDomain(BOOLEAN, 0, booleanColumnStats(0L, null)), none(Boolean.class));
        assertEquals(getDomain(BOOLEAN, 0, booleanColumnStats(0L, 0L)), none(Boolean.class));

        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(0L, 0L)), onlyNull(Boolean.class));
        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(10L, null)), notNull(Boolean.class));

        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(10L, 10L)), singleValue(true));
        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(10L, 0L)), singleValue(false));

        assertEquals(getDomain(BOOLEAN, 20, booleanColumnStats(10L, 5L)), all(Boolean.class));

        assertEquals(getDomain(BOOLEAN, 20, booleanColumnStats(10L, 10L)), new Domain(SortedRangeSet.of(Range.equal(true)), true));
        assertEquals(getDomain(BOOLEAN, 20, booleanColumnStats(10L, 0L)), new Domain(SortedRangeSet.of(Range.equal(false)), true));
    }

    private static ColumnStatistics booleanColumnStats(Long numberOfValues, Long trueValueCount)
    {
        BooleanStatistics booleanStatistics = null;
        if (trueValueCount != null) {
            booleanStatistics = new BooleanStatistics(trueValueCount);
        }
        return new ColumnStatistics(numberOfValues, booleanStatistics, null, null, null, null);
    }

    @Test
    public void testBigint()
            throws Exception
    {
        assertEquals(getDomain(BIGINT, 0, null), none(Long.class));
        assertEquals(getDomain(BIGINT, 10, null), all(Long.class));

        assertEquals(getDomain(BIGINT, 0, integerColumnStats(null, null, null)), none(Long.class));
        assertEquals(getDomain(BIGINT, 0, integerColumnStats(0L, null, null)), none(Long.class));
        assertEquals(getDomain(BIGINT, 0, integerColumnStats(0L, 100L, 100L)), none(Long.class));

        assertEquals(getDomain(BIGINT, 10, integerColumnStats(0L, null, null)), onlyNull(Long.class));
        assertEquals(getDomain(BIGINT, 10, integerColumnStats(10L, null, null)), notNull(Long.class));

        assertEquals(getDomain(BIGINT, 10, integerColumnStats(10L, 100L, 100L)), singleValue(100L));

        assertEquals(getDomain(BIGINT, 10, integerColumnStats(10L, 0L, 100L)), create(SortedRangeSet.of(range(0L, true, 100L, true)), false));
        assertEquals(getDomain(BIGINT, 10, integerColumnStats(10L, null, 100L)), create(SortedRangeSet.of(lessThanOrEqual(100L)), false));
        assertEquals(getDomain(BIGINT, 10, integerColumnStats(10L, 0L, null)), create(SortedRangeSet.of(greaterThanOrEqual(0L)), false));

        assertEquals(getDomain(BIGINT, 10, integerColumnStats(5L, 0L, 100L)), create(SortedRangeSet.of(range(0L, true, 100L, true)), true));
        assertEquals(getDomain(BIGINT, 10, integerColumnStats(5L, null, 100L)), create(SortedRangeSet.of(lessThanOrEqual(100L)), true));
        assertEquals(getDomain(BIGINT, 10, integerColumnStats(5L, 0L, null)), create(SortedRangeSet.of(greaterThanOrEqual(0L)), true));
    }

    private static ColumnStatistics integerColumnStats(Long numberOfValues, Long minimum, Long maximum)
    {
        return new ColumnStatistics(numberOfValues, null, new IntegerStatistics(minimum, maximum), null, null, null);
    }

    @Test
    public void testDouble()
            throws Exception
    {
        assertEquals(getDomain(DOUBLE, 0, null), none(Double.class));
        assertEquals(getDomain(DOUBLE, 10, null), all(Double.class));

        assertEquals(getDomain(DOUBLE, 0, doubleColumnStats(null, null, null)), none(Double.class));
        assertEquals(getDomain(DOUBLE, 0, doubleColumnStats(0L, null, null)), none(Double.class));
        assertEquals(getDomain(DOUBLE, 0, doubleColumnStats(0L, 42.24, 42.24)), none(Double.class));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(0L, null, null)), onlyNull(Double.class));
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(10L, null, null)), notNull(Double.class));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(10L, 42.24, 42.24)), singleValue(42.24));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(10L, 3.3, 42.24)), create(SortedRangeSet.of(range(3.3, true, 42.24, true)), false));
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(10L, null, 42.24)), create(SortedRangeSet.of(lessThanOrEqual(42.24)), false));
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(10L, 3.3, null)), create(SortedRangeSet.of(greaterThanOrEqual(3.3)), false));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(5L, 3.3, 42.24)), create(SortedRangeSet.of(range(3.3, true, 42.24, true)), true));
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(5L, null, 42.24)), create(SortedRangeSet.of(lessThanOrEqual(42.24)), true));
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(5L, 3.3, null)), create(SortedRangeSet.of(greaterThanOrEqual(3.3)), true));
    }

    private static ColumnStatistics doubleColumnStats(Long numberOfValues, Double minimum, Double maximum)
    {
        return new ColumnStatistics(numberOfValues, null, null, new DoubleStatistics(minimum, maximum), null, null);
    }

    @Test
    public void testString()
            throws Exception
    {
        assertEquals(getDomain(VARCHAR, 0, null), none(Slice.class));
        assertEquals(getDomain(VARCHAR, 10, null), all(Slice.class));

        assertEquals(getDomain(VARCHAR, 0, stringColumnStats(null, null, null)), none(Slice.class));
        assertEquals(getDomain(VARCHAR, 0, stringColumnStats(0L, null, null)), none(Slice.class));
        assertEquals(getDomain(VARCHAR, 0, stringColumnStats(0L, "taco", "taco")), none(Slice.class));

        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(0L, null, null)), onlyNull(Slice.class));
        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(10L, null, null)), notNull(Slice.class));

        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(10L, "taco", "taco")), singleValue(utf8Slice("taco")));

        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(10L, "apple", "taco")), create(SortedRangeSet.of(range(utf8Slice("apple"), true, utf8Slice("taco"), true)), false));
        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(10L, null, "taco")), create(SortedRangeSet.of(lessThanOrEqual(utf8Slice("taco"))), false));
        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(10L, "apple", null)), create(SortedRangeSet.of(greaterThanOrEqual(utf8Slice("apple"))), false));

        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(5L, "apple", "taco")), create(SortedRangeSet.of(range(utf8Slice("apple"), true, utf8Slice("taco"), true)), true));
        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(5L, null, "taco")), create(SortedRangeSet.of(lessThanOrEqual(utf8Slice("taco"))), true));
        assertEquals(getDomain(VARCHAR, 10, stringColumnStats(5L, "apple", null)), create(SortedRangeSet.of(greaterThanOrEqual(utf8Slice("apple"))), true));
    }

    private static ColumnStatistics stringColumnStats(Long numberOfValues, String minimum, String maximum)
    {
        return new ColumnStatistics(numberOfValues, null, null, null, new StringStatistics(getMinSlice(minimum), getMaxSlice(maximum)), null);
    }

    @Test
    public void testDate()
            throws Exception
    {
        assertEquals(getDomain(DATE, 0, null), none(Long.class));
        assertEquals(getDomain(DATE, 10, null), all(Long.class));

        assertEquals(getDomain(DATE, 0, dateColumnStats(null, null, null)), none(Long.class));
        assertEquals(getDomain(DATE, 0, dateColumnStats(0L, null, null)), none(Long.class));
        assertEquals(getDomain(DATE, 0, dateColumnStats(0L, 100, 100)), none(Long.class));

        assertEquals(getDomain(DATE, 10, dateColumnStats(0L, null, null)), onlyNull(Long.class));
        assertEquals(getDomain(DATE, 10, dateColumnStats(10L, null, null)), notNull(Long.class));

        assertEquals(getDomain(DATE, 10, dateColumnStats(10L, 100, 100)), singleValue(100L));

        assertEquals(getDomain(DATE, 10, dateColumnStats(10L, 0, 100)), create(SortedRangeSet.of(range(0L, true, 100L, true)), false));
        assertEquals(getDomain(DATE, 10, dateColumnStats(10L, null, 100)), create(SortedRangeSet.of(lessThanOrEqual(100L)), false));
        assertEquals(getDomain(DATE, 10, dateColumnStats(10L, 0, null)), create(SortedRangeSet.of(greaterThanOrEqual(0L)), false));

        assertEquals(getDomain(DATE, 10, dateColumnStats(5L, 0, 100)), create(SortedRangeSet.of(range(0L, true, 100L, true)), true));
        assertEquals(getDomain(DATE, 10, dateColumnStats(5L, null, 100)), create(SortedRangeSet.of(lessThanOrEqual(100L)), true));
        assertEquals(getDomain(DATE, 10, dateColumnStats(5L, 0, null)), create(SortedRangeSet.of(greaterThanOrEqual(0L)), true));
    }

    private static ColumnStatistics dateColumnStats(Long numberOfValues, Integer minimum, Integer maximum)
    {
        return new ColumnStatistics(numberOfValues, null, null, null, null, new DateStatistics(minimum, maximum));
    }
}
