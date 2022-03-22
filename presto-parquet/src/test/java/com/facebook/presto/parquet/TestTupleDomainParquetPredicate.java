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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.parquet.predicate.DictionaryDescriptor;
import com.facebook.presto.parquet.predicate.TupleDomainParquetPredicate;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.bytes.LittleEndianDataOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.predicate.Domain.create;
import static com.facebook.presto.common.predicate.Domain.notNull;
import static com.facebook.presto.common.predicate.Domain.singleValue;
import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static com.facebook.presto.parquet.predicate.TupleDomainParquetPredicate.getDomain;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.NaN;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTupleDomainParquetPredicate
{
    private static final ParquetDataSourceId ID = new ParquetDataSourceId("testFile");

    @Test
    public void testBoolean()
            throws ParquetCorruptionException
    {
        String column = "BooleanColumn";
        assertEquals(getDomain(BOOLEAN, 0, null, ID, column, true), Domain.all(BOOLEAN));

        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(true, true), ID, column, true), singleValue(BOOLEAN, true));
        assertEquals(getDomain(BOOLEAN, 10, booleanColumnStats(false, false), ID, column, true), singleValue(BOOLEAN, false));

        assertEquals(getDomain(BOOLEAN, 20, booleanColumnStats(false, true), ID, column, true), Domain.all(BOOLEAN));
    }

    private static BooleanStatistics booleanColumnStats(boolean minimum, boolean maximum)
    {
        BooleanStatistics statistics = new BooleanStatistics();
        statistics.setMinMax(minimum, maximum);
        return statistics;
    }

    @Test
    public void testBigint()
            throws ParquetCorruptionException
    {
        String column = "BigintColumn";
        assertEquals(getDomain(BIGINT, 0, null, ID, column, true), Domain.all(BIGINT));

        assertEquals(getDomain(BIGINT, 10, longColumnStats(100L, 100L), ID, column, true), singleValue(BIGINT, 100L));

        assertEquals(getDomain(BIGINT, 10, longColumnStats(0L, 100L), ID, column, true), create(ValueSet.ofRanges(range(BIGINT, 0L, true, 100L, true)), false));

        assertEquals(getDomain(BIGINT, 20, longOnlyNullsStats(10), ID, column, true), create(ValueSet.all(BIGINT), true));
        // ignore corrupted statistics
        assertEquals(getDomain(BIGINT, 10, longColumnStats(100L, 0L), ID, column, false), create(ValueSet.all(BIGINT), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(BIGINT, 10, longColumnStats(100L, 10L), ID, column, true))
                .withMessage("Corrupted statistics for column \"BigintColumn\" in Parquet file \"testFile\": [min: 100, max: 10, num_nulls: 0]");
    }

    @Test
    public void testInteger()
            throws ParquetCorruptionException
    {
        String column = "IntegerColumn";
        assertEquals(getDomain(INTEGER, 0, null, ID, column, true), Domain.all(INTEGER));

        assertEquals(getDomain(INTEGER, 10, longColumnStats(100, 100), ID, column, true), singleValue(INTEGER, 100L));

        assertEquals(getDomain(INTEGER, 10, longColumnStats(0, 100), ID, column, true), create(ValueSet.ofRanges(range(INTEGER, 0L, true, 100L, true)), false));

        assertEquals(getDomain(INTEGER, 20, longColumnStats(0, 2147483648L), ID, column, true), notNull(INTEGER));

        assertEquals(getDomain(INTEGER, 20, longOnlyNullsStats(10), ID, column, true), create(ValueSet.all(INTEGER), true));
        // ignore corrupted statistics
        assertEquals(getDomain(INTEGER, 10, longColumnStats(2147483648L, 0), ID, column, false), create(ValueSet.all(INTEGER), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(INTEGER, 10, longColumnStats(2147483648L, 10), ID, column, true))
                .withMessage("Corrupted statistics for column \"IntegerColumn\" in Parquet file \"testFile\": [min: 2147483648, max: 10, num_nulls: 0]");
    }

    @Test
    public void testSmallint()
            throws ParquetCorruptionException
    {
        String column = "SmallintColumn";
        assertEquals(getDomain(SMALLINT, 0, null, ID, column, true), Domain.all(SMALLINT));

        assertEquals(getDomain(SMALLINT, 10, longColumnStats(100, 100), ID, column, true), singleValue(SMALLINT, 100L));

        assertEquals(getDomain(SMALLINT, 10, longColumnStats(0, 100), ID, column, true), create(ValueSet.ofRanges(range(SMALLINT, 0L, true, 100L, true)), false));

        assertEquals(getDomain(SMALLINT, 20, longColumnStats(0, 2147483648L), ID, column, true), notNull(SMALLINT));

        assertEquals(getDomain(SMALLINT, 20, longOnlyNullsStats(10), ID, column, true), create(ValueSet.all(SMALLINT), true));
        // ignore corrupted statistics
        assertEquals(getDomain(SMALLINT, 10, longColumnStats(2147483648L, 0), ID, column, false), create(ValueSet.all(SMALLINT), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(SMALLINT, 10, longColumnStats(2147483648L, 10), ID, column, true))
                .withMessage("Corrupted statistics for column \"SmallintColumn\" in Parquet file \"testFile\": [min: 2147483648, max: 10, num_nulls: 0]");
    }

    @Test
    public void testTinyint()
            throws ParquetCorruptionException
    {
        String column = "TinyintColumn";
        assertEquals(getDomain(TINYINT, 0, null, ID, column, true), Domain.all(TINYINT));

        assertEquals(getDomain(TINYINT, 10, longColumnStats(100, 100), ID, column, true), singleValue(TINYINT, 100L));

        assertEquals(getDomain(TINYINT, 10, longColumnStats(0, 100), ID, column, true), create(ValueSet.ofRanges(range(TINYINT, 0L, true, 100L, true)), false));

        assertEquals(getDomain(TINYINT, 20, longColumnStats(0, 2147483648L), ID, column, true), notNull(TINYINT));

        assertEquals(getDomain(TINYINT, 20, longOnlyNullsStats(10), ID, column, true), create(ValueSet.all(TINYINT), true));
        // ignore corrupted statistics
        assertEquals(getDomain(TINYINT, 10, longColumnStats(2147483648L, 0), ID, column, false), create(ValueSet.all(TINYINT), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(TINYINT, 10, longColumnStats(2147483648L, 10), ID, column, true))
                .withMessage("Corrupted statistics for column \"TinyintColumn\" in Parquet file \"testFile\": [min: 2147483648, max: 10, num_nulls: 0]");
    }

    @Test
    public void testDouble()
            throws Exception
    {
        String column = "DoubleColumn";
        assertEquals(getDomain(DOUBLE, 0, null, ID, column, true), Domain.all(DOUBLE));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(42.24, 42.24), ID, column, true), singleValue(DOUBLE, 42.24));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(3.3, 42.24), ID, column, true), create(ValueSet.ofRanges(range(DOUBLE, 3.3, true, 42.24, true)), false));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(NaN, NaN), ID, column, true), Domain.notNull(DOUBLE));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(NaN, NaN, true), ID, column, true), Domain.all(DOUBLE));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(3.3, NaN), ID, column, true), Domain.notNull(DOUBLE));

        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(3.3, NaN, true), ID, column, true), Domain.all(DOUBLE));

        assertEquals(getDomain(DOUBLE, doubleDictionaryDescriptor(NaN)), Domain.all(DOUBLE));

        assertEquals(getDomain(DOUBLE, doubleDictionaryDescriptor(3.3, NaN)), Domain.all(DOUBLE));

        // ignore corrupted statistics
        assertEquals(getDomain(DOUBLE, 10, doubleColumnStats(42.24, 3.3), ID, column, false), create(ValueSet.all(DOUBLE), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(DOUBLE, 10, doubleColumnStats(42.24, 3.3), ID, column, true))
                .withMessage("Corrupted statistics for column \"DoubleColumn\" in Parquet file \"testFile\": [min: 42.24, max: 3.3, num_nulls: 0]");
    }

    @Test
    public void testString()
            throws ParquetCorruptionException
    {
        String column = "StringColumn";
        assertEquals(getDomain(createUnboundedVarcharType(), 0, null, ID, column, true), Domain.all(createUnboundedVarcharType()));

        assertEquals(getDomain(createUnboundedVarcharType(), 10, stringColumnStats("taco", "taco"), ID, column, true), singleValue(createUnboundedVarcharType(), utf8Slice("taco")));

        assertEquals(getDomain(createUnboundedVarcharType(), 10, stringColumnStats("apple", "taco"), ID, column, true), create(ValueSet.ofRanges(range(createUnboundedVarcharType(), utf8Slice("apple"), true, utf8Slice("taco"), true)), false));

        assertEquals(getDomain(createUnboundedVarcharType(), 10, stringColumnStats("中国", "美利坚"), ID, column, true), create(ValueSet.ofRanges(range(createUnboundedVarcharType(), utf8Slice("中国"), true, utf8Slice("美利坚"), true)), false));

        // ignore corrupted statistics
        assertEquals(getDomain(createUnboundedVarcharType(), 10, stringColumnStats("taco", "apple"), ID, column, false), create(ValueSet.all(createUnboundedVarcharType()), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(createUnboundedVarcharType(), 10, stringColumnStats("taco", "apple"), ID, column, true))
                .withMessage("Corrupted statistics for column \"StringColumn\" in Parquet file \"testFile\": [min: taco, max: apple, num_nulls: 0]");
    }

    private static Statistics stringColumnStats(String minimum, String maximum)
    {
        Statistics.Builder builder = Statistics.getBuilderForReading(new PrimitiveType(OPTIONAL, BINARY, "testFile", UTF8));
        builder.withMin(minimum.getBytes())
               .withMax(maximum.getBytes())
               .withNumNulls(0);
        return builder.build();
    }

    @Test
    public void testFloat()
            throws Exception
    {
        String column = "FloatColumn";
        assertEquals(getDomain(REAL, 0, null, ID, column, true), Domain.all(REAL));

        float minimum = 4.3f;
        float maximum = 40.3f;

        assertEquals(getDomain(REAL, 10, floatColumnStats(minimum, minimum), ID, column, true), singleValue(REAL, (long) floatToRawIntBits(minimum)));

        assertEquals(
                getDomain(REAL, 10, floatColumnStats(minimum, maximum), ID, column, true),
                create(ValueSet.ofRanges(range(REAL, (long) floatToRawIntBits(minimum), true, (long) floatToRawIntBits(maximum), true)), false));

        assertEquals(getDomain(REAL, 10, floatColumnStats(NaN, NaN), ID, column, true), Domain.notNull(REAL));

        assertEquals(getDomain(REAL, 10, floatColumnStats(NaN, NaN, true), ID, column, true), Domain.all(REAL));

        assertEquals(getDomain(REAL, 10, floatColumnStats(minimum, NaN), ID, column, true), Domain.notNull(REAL));

        assertEquals(getDomain(REAL, 10, floatColumnStats(minimum, NaN, true), ID, column, true), Domain.all(REAL));

        assertEquals(getDomain(REAL, floatDictionaryDescriptor(NaN)), Domain.all(REAL));

        assertEquals(getDomain(REAL, floatDictionaryDescriptor(minimum, NaN)), Domain.all(REAL));

        // ignore corrupted statistics
        assertEquals(getDomain(REAL, 10, floatColumnStats(maximum, minimum), ID, column, false), create(ValueSet.all(REAL), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(REAL, 10, floatColumnStats(maximum, minimum), ID, column, true))
                .withMessage("Corrupted statistics for column \"FloatColumn\" in Parquet file \"testFile\": [min: 40.3, max: 4.3, num_nulls: 0]");
    }

    @Test
    public void testDate()
            throws ParquetCorruptionException
    {
        String column = "DateColumn";
        assertEquals(getDomain(DATE, 0, null, ID, column, true), Domain.all(DATE));
        assertEquals(getDomain(DATE, 10, intColumnStats(100, 100), ID, column, true), singleValue(DATE, 100L));
        assertEquals(getDomain(DATE, 10, intColumnStats(0, 100), ID, column, true), create(ValueSet.ofRanges(range(DATE, 0L, true, 100L, true)), false));

        // ignore corrupted statistics
        assertEquals(getDomain(DATE, 10, intColumnStats(200, 100), ID, column, false), create(ValueSet.all(DATE), false));
        // fail on corrupted statistics
        assertThatExceptionOfType(ParquetCorruptionException.class)
                .isThrownBy(() -> getDomain(DATE, 10, intColumnStats(200, 100), ID, column, true))
                .withMessage("Corrupted statistics for column \"DateColumn\" in Parquet file \"testFile\": [min: 200, max: 100, num_nulls: 0]");
    }

    @Test
    public void testVarcharMatchesWithStatistics()
            throws ParquetCorruptionException
    {
        String value = "Test";
        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {"path"}, BINARY, 0, 0);
        RichColumnDescriptor column = new RichColumnDescriptor(columnDescriptor, new PrimitiveType(OPTIONAL, BINARY, "Test column"));
        TupleDomain<ColumnDescriptor> effectivePredicate = getEffectivePredicate(column, createVarcharType(255), utf8Slice(value));
        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column));
        Statistics<?> stats = getStatsBasedOnType(column.getType());
        stats.setNumNulls(1L);
        stats.setMinMaxFromBytes(value.getBytes(), value.getBytes());
        assertTrue(parquetPredicate.matches(2, singletonMap(column, stats), ID, true));
    }

    @Test(dataProvider = "typeForParquetInt32")
    public void testIntegerMatchesWithStatistics(Type typeForParquetInt32)
            throws ParquetCorruptionException
    {
        RichColumnDescriptor column = new RichColumnDescriptor(
                new ColumnDescriptor(new String[] {"path"}, INT32, 0, 0),
                new PrimitiveType(OPTIONAL, INT32, "Test column"));
        TupleDomain<ColumnDescriptor> effectivePredicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                column,
                Domain.create(ValueSet.of(typeForParquetInt32, 42L, 43L, 44L, 112L), false)));
        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column));

        assertTrue(parquetPredicate.matches(2, ImmutableMap.of(column, intColumnStats(32, 42)), ID, true));
        assertFalse(parquetPredicate.matches(2, ImmutableMap.of(column, intColumnStats(30, 40)), ID, true));
        assertEquals(parquetPredicate.matches(2, ImmutableMap.of(column, intColumnStats(1024, 0x10000 + 42)), ID, true), (typeForParquetInt32 != INTEGER)); // stats invalid for smallint/tinyint
    }

    @DataProvider
    public Object[][] typeForParquetInt32()
    {
        return new Object[][] {
                {INTEGER},
                {SMALLINT},
                {TINYINT},
        };
    }

    @Test
    public void testBigintMatchesWithStatistics()
            throws ParquetCorruptionException
    {
        RichColumnDescriptor column = new RichColumnDescriptor(
                new ColumnDescriptor(new String[] {"path"}, INT64, 0, 0),
                new PrimitiveType(OPTIONAL, INT64, "Test column"));
        TupleDomain<ColumnDescriptor> effectivePredicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                column,
                Domain.create(ValueSet.of(BIGINT, 42L, 43L, 44L, 404L), false)));
        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column));

        assertTrue(parquetPredicate.matches(2, ImmutableMap.of(column, longColumnStats(32, 42)), ID, true));
        assertFalse(parquetPredicate.matches(2, ImmutableMap.of(column, longColumnStats(30, 40)), ID, true));
        assertFalse(parquetPredicate.matches(2, ImmutableMap.of(column, longColumnStats(1024, 0x10000 + 42)), ID, true));
    }

    @Test
    public void testVarcharMatchesWithDictionaryDescriptor()
    {
        ColumnDescriptor columnDescriptor = new ColumnDescriptor(new String[] {"path"}, new PrimitiveType(OPTIONAL, BINARY, 0, ""), 0, 0);
        RichColumnDescriptor column = new RichColumnDescriptor(columnDescriptor, new PrimitiveType(OPTIONAL, BINARY, "Test column"));
        TupleDomain<ColumnDescriptor> effectivePredicate = getEffectivePredicate(column, createVarcharType(255), EMPTY_SLICE);
        TupleDomainParquetPredicate parquetPredicate = new TupleDomainParquetPredicate(effectivePredicate, singletonList(column));
        DictionaryPage page = new DictionaryPage(Slices.wrappedBuffer(new byte[] {0, 0, 0, 0}), 1, PLAIN_DICTIONARY);
        assertTrue(parquetPredicate.matches(new DictionaryDescriptor(column, Optional.of(page))));
    }

    private TupleDomain<ColumnDescriptor> getEffectivePredicate(RichColumnDescriptor column, VarcharType type, Slice value)
    {
        ColumnDescriptor predicateColumn = new ColumnDescriptor(column.getPath(), column.getType(), 0, 0);
        Domain predicateDomain = singleValue(type, value);
        Map<ColumnDescriptor, Domain> predicateColumns = singletonMap(predicateColumn, predicateDomain);
        return withColumnDomains(predicateColumns);
    }

    private static DoubleStatistics doubleColumnStats(double minimum, double maximum)
    {
        return doubleColumnStats(minimum, maximum, false);
    }

    private static DoubleStatistics doubleColumnStats(double minimum, double maximum, boolean hasNulls)
    {
        DoubleStatistics statistics = new DoubleStatistics();
        statistics.setMinMax(minimum, maximum);
        if (hasNulls) {
            statistics.setNumNulls(1);
        }
        return statistics;
    }

    private static FloatStatistics floatColumnStats(float minimum, float maximum)
    {
        return floatColumnStats(minimum, maximum, false);
    }

    private static FloatStatistics floatColumnStats(float minimum, float maximum, boolean hasNulls)
    {
        FloatStatistics statistics = new FloatStatistics();
        statistics.setMinMax(minimum, maximum);
        if (hasNulls) {
            statistics.setNumNulls(1);
        }
        return statistics;
    }

    private static IntStatistics intColumnStats(int minimum, int maximum)
    {
        IntStatistics statistics = new IntStatistics();
        statistics.setMinMax(minimum, maximum);
        return statistics;
    }

    private static Statistics<Long> longColumnStats(long minimum, long maximum)
    {
        LongStatistics statistics = new LongStatistics();
        statistics.setMinMax(minimum, maximum);
        return statistics;
    }

    private DictionaryDescriptor floatDictionaryDescriptor(float... values)
            throws Exception
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(buffer)) {
            for (float value : values) {
                out.writeFloat(value);
            }
        }
        return new DictionaryDescriptor(
                new ColumnDescriptor(new String[] {"dummy"}, new PrimitiveType(OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, 0, ""), 1, 1),
                Optional.of(new DictionaryPage(Slices.wrappedBuffer(buffer.toByteArray()), values.length, PLAIN_DICTIONARY)));
    }

    private DictionaryDescriptor doubleDictionaryDescriptor(double... values)
            throws Exception
    {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(buffer)) {
            for (double value : values) {
                out.writeDouble(value);
            }
        }
        return new DictionaryDescriptor(
                new ColumnDescriptor(new String[] {"dummy"}, new PrimitiveType(OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, 0, ""), 1, 1),
                Optional.of(new DictionaryPage(Slices.wrappedBuffer(buffer.toByteArray()), values.length, PLAIN_DICTIONARY)));
    }

    private static LongStatistics longOnlyNullsStats(long numNulls)
    {
        LongStatistics statistics = new LongStatistics();
        statistics.setNumNulls(numNulls);
        return statistics;
    }
}
