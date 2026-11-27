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
package com.facebook.presto.hudi.common;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.HoodieValueTypeInfo;
import org.apache.hudi.avro.model.LongWrapper;
import org.apache.hudi.org.apache.avro.generic.GenericData;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.stats.ValueType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.hudi.common.HudiColumnStatsConverter.toDomain;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHudiColumnStatsConverter
{
    @Test
    public void testHudiAvroStatistics()
    {
        HoodieMetadataColumnStats statistics = HoodieMetadataColumnStats.newBuilder()
                .setFileName("file.parquet")
                .setColumnName("x")
                .setMinValue(new LongWrapper(1L))
                .setMaxValue(new LongWrapper(2L))
                .setValueCount(3L)
                .setNullCount(0L)
                .setTotalSize(0L)
                .setTotalUncompressedSize(0L)
                .setIsDeleted(false)
                .build();
        assertEquals(toDomain(BIGINT, statistics), range(1, 2));

        GenericData.Record genericBound = new GenericData.Record(LongWrapper.getClassSchema());
        genericBound.put("value", 1L);
        statistics.setMinValue(genericBound);
        assertEquals(toDomain(BIGINT, statistics), range(1, 2));
        ValueMetadata nativeEncoding = ValueMetadata.getValueMetadata(
                HoodieValueTypeInfo.newBuilder().setTypeOrdinal(ValueType.LONG.ordinal()).build());
        statistics.setValueType(nativeEncoding.getValueTypeInfo());
        statistics.setMinValue(nativeEncoding.wrapValue(1L));
        statistics.setMaxValue(nativeEncoding.wrapValue(2L));
        assertEquals(toDomain(BIGINT, statistics), range(1, 2));
        statistics.setIsDeleted(true);
        assertEquals(toDomain(BIGINT, statistics), Domain.all(BIGINT));
        statistics.setIsDeleted(false);
        statistics.setValueType(null);
        statistics.setMinValue("invalid encoding");
        assertEquals(toDomain(BIGINT, statistics), Domain.all(BIGINT));
        statistics.setValueCount(null);
        assertEquals(toDomain(BIGINT, statistics), Domain.all(BIGINT));
        assertEquals(toDomain(BIGINT, (HoodieMetadataColumnStats) null), Domain.all(BIGINT));
    }

    @DataProvider
    public Object[][] integralTypes()
    {
        return new Object[][] {
                {TINYINT, (long) Byte.MIN_VALUE, (long) Byte.MAX_VALUE},
                {SMALLINT, (long) Short.MIN_VALUE, (long) Short.MAX_VALUE},
                {INTEGER, (long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE},
                {BIGINT, Long.MIN_VALUE, Long.MAX_VALUE}};
    }

    @Test(dataProvider = "integralTypes")
    public void testInclusiveIntegralBounds(Type type, long minimum, long maximum)
    {
        Domain domain = toDomain(type, 3, 1, minimum, maximum);
        assertEquals(domain, Domain.create(ValueSet.ofRanges(Range.range(type, minimum, true, maximum, true)), true));
        assertTrue(domain.includesNullableValue(minimum));
        assertTrue(domain.includesNullableValue(maximum));
        assertTrue(domain.includesNullableValue(null));
        assertEquals(toDomain(type, 2, 0, 3, 3L), Domain.singleValue(type, 3L));
    }

    @Test
    public void testIntegralRepresentations()
    {
        assertEquals(toDomain(BIGINT, 2, 0, (byte) 1, (short) 2), range(1, 2));
        assertEquals(toDomain(BIGINT, 2, 0, 1, 2L), range(1, 2));
    }

    @Test
    public void testOverflowAndReversedBounds()
    {
        assertEquals(toDomain(TINYINT, 2, 0, -129L, 1L), Domain.all(TINYINT));
        assertEquals(toDomain(TINYINT, 2, 0, 1L, 128L), Domain.all(TINYINT));
        assertEquals(toDomain(SMALLINT, 2, 0, -32769L, 1L), Domain.all(SMALLINT));
        assertEquals(toDomain(SMALLINT, 2, 0, 1L, 32768L), Domain.all(SMALLINT));
        assertEquals(toDomain(INTEGER, 2, 0, (long) Integer.MIN_VALUE - 1, 1L), Domain.all(INTEGER));
        assertEquals(toDomain(INTEGER, 2, 0, 1L, (long) Integer.MAX_VALUE + 1), Domain.all(INTEGER));
        assertEquals(toDomain(BIGINT, 2, 0, 2L, 1L), Domain.all(BIGINT));
    }

    @DataProvider
    public Object[][] invalidBounds()
    {
        return new Object[][] {
                {1.5, 2.5},
                {Double.NaN, Double.POSITIVE_INFINITY},
                {1.0f, 2.0f},
                {BigInteger.ZERO, BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE)},
                {BigDecimal.ZERO, BigDecimal.ONE},
                {"1", "2"},
                {false, true},
                {1L, "2"}};
    }

    @Test(dataProvider = "invalidBounds")
    public void testInvalidBounds(Object minimum, Object maximum)
    {
        assertEquals(toDomain(BIGINT, 2, 0, minimum, maximum), Domain.all(BIGINT));
    }

    @Test
    public void testNullCountsAndMissingBounds()
    {
        assertEquals(toDomain(BIGINT, 3, 3, null, null), Domain.onlyNull(BIGINT));
        assertEquals(toDomain(BIGINT, 3, 3, 1L, 2L), Domain.all(BIGINT));
        assertEquals(toDomain(BIGINT, 3, 3, null, 2L), Domain.all(BIGINT));
        assertEquals(toDomain(BIGINT, 3, 0, null, null), Domain.notNull(BIGINT));
        assertEquals(toDomain(BIGINT, 3, 0, 1L, null), Domain.notNull(BIGINT));
        assertEquals(toDomain(BIGINT, 3, 0, null, 2L), Domain.notNull(BIGINT));
        assertEquals(toDomain(BIGINT, 3, 1, null, null), Domain.all(BIGINT));
    }

    @DataProvider
    public Object[][] invalidCounts()
    {
        return new Object[][] {
                {0L, 0L},
                {-1L, 0L},
                {3L, -1L},
                {3L, 4L},
                {Long.MAX_VALUE, Long.MIN_VALUE}};
    }

    @Test(dataProvider = "invalidCounts")
    public void testInvalidCounts(long valueCount, long nullCount)
    {
        assertEquals(toDomain(BIGINT, valueCount, nullCount, 1L, 2L), Domain.all(BIGINT));
        assertEquals(toDomain(BIGINT, valueCount, nullCount, null, null), Domain.all(BIGINT));
    }

    @Test
    public void testBooleanBounds()
    {
        assertEquals(toDomain(BOOLEAN, 3, 0, false, false), Domain.singleValue(BOOLEAN, false));
        assertEquals(toDomain(BOOLEAN, 3, 0, true, true), Domain.singleValue(BOOLEAN, true));
        assertEquals(toDomain(BOOLEAN, 3, 0, false, true), Domain.notNull(BOOLEAN));
        assertEquals(toDomain(BOOLEAN, 3, 1, false, true), Domain.all(BOOLEAN));
        assertEquals(toDomain(BOOLEAN, 3, 1, true, true), Domain.create(ValueSet.of(BOOLEAN, true), true));
        assertEquals(toDomain(BOOLEAN, 3, 3, null, null), Domain.onlyNull(BOOLEAN));
        assertEquals(toDomain(BOOLEAN, 3, 0, true, false), Domain.all(BOOLEAN));
        assertEquals(toDomain(BOOLEAN, 3, 0, 0L, 1L), Domain.all(BOOLEAN));
    }

    @DataProvider
    public Object[][] unsupportedTypes()
    {
        return new Object[][] {
                {DATE},
                {TIMESTAMP},
                {DOUBLE},
                {REAL},
                {VARCHAR},
                {createVarcharType(3)},
                {VARBINARY},
                {createDecimalType(10, 2)}};
    }

    @Test(dataProvider = "unsupportedTypes")
    public void testUnsupportedTypes(Type type)
    {
        assertEquals(toDomain(type, 3, 0, 1L, 2L), Domain.all(type));
        assertEquals(toDomain(type, 3, 3, null, null), Domain.all(type));
    }

    private static Domain range(long minimum, long maximum)
    {
        return Domain.create(ValueSet.ofRanges(Range.range(BIGINT, minimum, true, maximum, true)), false);
    }
}
