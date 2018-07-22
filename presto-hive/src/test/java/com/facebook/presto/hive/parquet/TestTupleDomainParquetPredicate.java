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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.parquet.predicate.ParquetDictionaryDescriptor;
import com.facebook.presto.hive.parquet.predicate.TupleDomainParquetPredicate;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.VarcharType;
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
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static com.facebook.presto.hive.parquet.predicate.TupleDomainParquetPredicate.getDomain;
import static com.facebook.presto.spi.predicate.Domain.all;
import static com.facebook.presto.spi.predicate.Domain.create;
import static com.facebook.presto.spi.predicate.Domain.notNull;
import static com.facebook.presto.spi.predicate.Domain.singleValue;
import static com.facebook.presto.spi.predicate.Range.range;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static parquet.column.statistics.Statistics.getStatsBasedOnType;

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
        RichColumnDescriptor column = getColumn();
        String value = "Test";
        TupleDomain<ColumnDescriptor> effectivePredicate = getEffectivePredicate(column, createVarcharType(255), value);
        List<RichColumnDescriptor> columns = singletonList(column);
        TupleDomainParquetPredicate predicate = new TupleDomainParquetPredicate(effectivePredicate, columns);
        Statistics stats = getStatsBasedOnType(column.getType());
        stats.setNumNulls(1L);
        stats.setMinMaxFromBytes(value.getBytes(), value.getBytes());
        assertTrue(predicate.matches(2, singletonMap(column, stats)));
    }

    @Test
    public void testMatchesWithDescriptors()
    {
        RichColumnDescriptor column = getColumn();
        String value = "Test2";
        TupleDomain<ColumnDescriptor> effectivePredicate = getEffectivePredicate(column, createVarcharType(255), value);
        List<RichColumnDescriptor> columns = singletonList(column);
        TupleDomainParquetPredicate predicate = new TupleDomainParquetPredicate(effectivePredicate, columns);
        ParquetDictionaryPage page = new ParquetDictionaryPage(utf8Slice(value), 2, PLAIN_DICTIONARY);
        assertTrue(predicate.matches(singletonMap(column, new ParquetDictionaryDescriptor(column, Optional.of(page)))));
    }

    private TupleDomain<ColumnDescriptor> getEffectivePredicate(RichColumnDescriptor column, VarcharType type, String value)
    {
        ColumnDescriptor predicateColumn = new ColumnDescriptor(column.getPath(), column.getType(), 0, 0);
        Domain predicateDomain = Domain.singleValue(type, utf8Slice(value));
        Map<ColumnDescriptor, Domain> predicateColumns = singletonMap(predicateColumn, predicateDomain);
        return TupleDomain.withColumnDomains(predicateColumns);
    }

    private RichColumnDescriptor getColumn()
    {
        PrimitiveType type = new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "Test column");
        return new RichColumnDescriptor(new String[] {"path"}, type, 0, 0);
    }

    private static FloatStatistics floatColumnStats(float minimum, float maximum)
    {
        FloatStatistics statistics = new FloatStatistics();
        statistics.setMinMax(minimum, maximum);
        return statistics;
    }
}
