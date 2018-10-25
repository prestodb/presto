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
package com.facebook.presto.parquet;

import com.facebook.presto.parquet.predicate.DictionaryDescriptor;
import com.facebook.presto.parquet.predicate.TupleDomainParquetPredicate;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;
import parquet.column.ColumnDescriptor;
import parquet.column.statistics.BinaryStatistics;
import parquet.column.statistics.BooleanStatistics;
import parquet.column.statistics.DoubleStatistics;
import parquet.column.statistics.FloatStatistics;
import parquet.column.statistics.LongStatistics;
import parquet.column.statistics.Statistics;
import parquet.io.api.Binary;
import parquet.schema.PrimitiveType;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static com.facebook.presto.parquet.predicate.TupleDomainParquetPredicate.getDomain;
import static com.facebook.presto.spi.predicate.Domain.all;
import static com.facebook.presto.spi.predicate.Domain.create;
import static com.facebook.presto.spi.predicate.Domain.notNull;
import static com.facebook.presto.spi.predicate.Domain.singleValue;
import static com.facebook.presto.spi.predicate.Range.range;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static parquet.column.statistics.Statistics.getStatsBasedOnType;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.Type.Repetition.OPTIONAL;

public class TestTupleDomainParquetPredicate
{
    @Test
    public void testBoolean()
    {
        assertEquals(getDomain(BOOLEAN, 0, null), all(BOOLEAN));

        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(true, true)), singleValue(BOOLEAN, true));
        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(false, false)), singleValue(BOOLEAN, false));

        assertEquals(getDomain(BOOLEAN, 20, booleanColumnStats(false, true)), all(BOOLEAN));
    }

    private static BooleanStatistics booleanColumnStats(boolean minimum, boolean maximum)
    {
        BooleanStatistics statistics = new BooleanStatistics();
        statistics.setMinMax(minimum, maximum);
        return statistics;
    }

    @Test
    public void testBigint()
    {
        assertEquals(getDomain(BIGINT, 0, null), all(BIGINT));

        assertEquals(getDomain(BIGINT, 10, longColumnStats(100L, 100L)), singleValue(BIGINT, 100L));

        assertEquals(getDomain(BIGINT, 10, longColumnStats(0L, 100L)), create(ValueSet.ofRanges(range(BIGINT, 0L, true, 100L, true)), false));
    }

    private static LongStatistics longColumnStats(long minimum, long maximum)
    {
        LongStatistics statistics = new LongStatistics();
        statistics.setMinMax(minimum, maximum);
        return statistics;
    }

    @Test
    public void testInteger()
    {
        assertEquals(getDomain(INTEGER, 0, null), all(INTEGER));

        assertEquals(getDomain(INTEGER, 10, longColumnStats(100, 100)), singleValue(INTEGER, 100L));

        assertEquals(getDomain(INTEGER, 10, longColumnStats(0, 100)), create(ValueSet.ofRanges(range(INTEGER, 0L, true, 100L, true)), false));

        assertEquals(getDomain(INTEGER, 20, longColumnStats(0, 2147483648L)), notNull(INTEGER));
    }

    @Test
    public void testSmallint()
    {
        assertEquals(getDomain(SMALLINT, 0, null), all(SMALLINT));

        assertEquals(getDomain(SMALLINT, 10, longColumnStats(100, 100)), singleValue(SMALLINT, 100L));

        assertEquals(getDomain(SMALLINT, 10, longColumnStats(0, 100)), create(ValueSet.ofRanges(range(SMALLINT, 0L, true, 100L, true)), false));

        assertEquals(getDomain(SMALLINT, 20, longColumnStats(0, 2147483648L)), notNull(SMALLINT));
    }

    @Test
    public void testTinyint()
    {
        assertEquals(getDomain(TINYINT, 0, null), all(TINYINT));

        assertEquals(getDomain(TINYINT, 10, longColumnStats(100, 100)), singleValue(TINYINT, 100L));

        assertEquals(getDomain(TINYINT, 10, longColumnStats(0, 100)), create(ValueSet.ofRanges(range(TINYINT, 0L, true, 100L, true)), false));

        assertEquals(getDomain(TINYINT, 20, longColumnStats(0, 2147483648L)), notNull(TINYINT));
    }

    @Test
    public void testDouble()
    {
        assertEquals(getDomain(DOUBLE, 0, null), all(DOUBLE));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(42.24, 42.24)), singleValue(DOUBLE, 42.24));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(3.3, 42.24)), create(ValueSet.ofRanges(range(DOUBLE, 3.3, true, 42.24, true)), false));
    }

    private static DoubleStatistics doubleColumnStats(double minimum, double maximum)
    {
        DoubleStatistics statistics = new DoubleStatistics();
        statistics.setMinMax(minimum, maximum);
        return statistics;
    }

    @Test
    public void testString()
    {
        assertEquals(getDomain(createUnboundedVarcharType(), 0, null), all(createUnboundedVarcharType()));

        assertEquals(getDomain(createUnboundedVarcharType(), 10, stringColumnStats("taco", "taco")), singleValue(createUnboundedVarcharType(), utf8Slice("taco")));

        assertEquals(getDomain(createUnboundedVarcharType(), 10, stringColumnStats("apple", "taco")), create(ValueSet.ofRanges(range(createUnboundedVarcharType(), utf8Slice("apple"), true, utf8Slice("taco"), true)), false));

        assertEquals(getDomain(createUnboundedVarcharType(), 10, stringColumnStats("中国", "美利坚")), create(ValueSet.ofRanges(range(createUnboundedVarcharType(), utf8Slice("中国"), true, utf8Slice("美利坚"), true)), false));
    }

    private static BinaryStatistics stringColumnStats(String minimum, String maximum)
    {
        BinaryStatistics statistics = new BinaryStatistics();
        statistics.setMinMax(Binary.fromString(minimum), Binary.fromString(maximum));
        return statistics;
    }

    @Test
    public void testFloat()
    {
        assertEquals(getDomain(REAL, 0, null), all(REAL));

        float minimum = 4.3f;
        float maximum = 40.3f;

        assertEquals(getDomain(REAL, 10, floatColumnStats(minimum, minimum)), singleValue(REAL, (long) floatToRawIntBits(minimum)));

        assertEquals(
                getDomain(REAL, 10, floatColumnStats(minimum, maximum)),
                create(ValueSet.ofRanges(range(REAL, (long) floatToRawIntBits(minimum), true, (long) floatToRawIntBits(maximum), true)), false));

        assertEquals(getDomain(REAL, 10, floatColumnStats(maximum, minimum)), create(ValueSet.all(REAL), false));
    }

    @Test
    public void testMatchesWithStatistics()
    {
        String value = "Test";
        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {"path"}, BINARY, 0, 0);
        RichColumnDescriptor column = new RichColumnDescriptor(columnDescriptor, new PrimitiveType(OPTIONAL, BINARY, "Test column"));
        TupleDomain<ColumnDescriptor> effectivePredicate = getEffectivePredicate(column, createVarcharType(255), utf8Slice(value));
        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column));
        Statistics stats = getStatsBasedOnType(column.getType());
        stats.setNumNulls(1L);
        stats.setMinMaxFromBytes(value.getBytes(), value.getBytes());
        assertTrue(parquetPredicate.matches(2, singletonMap(column, stats)));
    }

    @Test
    public void testMatchesWithDescriptors()
    {
        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {"path"}, BINARY, 0, 0);
        RichColumnDescriptor column = new RichColumnDescriptor(columnDescriptor, new PrimitiveType(OPTIONAL, BINARY, "Test column"));
        TupleDomain<ColumnDescriptor> effectivePredicate = getEffectivePredicate(column, createVarcharType(255), EMPTY_SLICE);
        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column));
        DictionaryPage page = new DictionaryPage(Slices.wrappedBuffer(new byte[] {0, 0, 0, 0}), 1, PLAIN_DICTIONARY);
        assertTrue(parquetPredicate.matches(singletonMap(column, new DictionaryDescriptor(column, Optional.of(page)))));
    }

    private TupleDomain<ColumnDescriptor> getEffectivePredicate(RichColumnDescriptor column, VarcharType type, Slice value)
    {
        ColumnDescriptor predicateColumn = new ColumnDescriptor(column.getPath(), column.getType(), 0, 0);
        Domain predicateDomain = singleValue(type, value);
        Map<ColumnDescriptor, Domain> predicateColumns = singletonMap(predicateColumn, predicateDomain);
        return withColumnDomains(predicateColumns);
    }

    private static FloatStatistics floatColumnStats(float minimum, float maximum)
    {
        FloatStatistics statistics = new FloatStatistics();
        statistics.setMinMax(minimum, maximum);
        return statistics;
    }
}
