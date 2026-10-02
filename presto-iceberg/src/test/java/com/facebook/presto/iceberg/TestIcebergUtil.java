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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.block.MethodHandleUtil;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.IcebergColumnHandle.LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE;
import static com.facebook.presto.iceberg.IcebergUtil.DOUBLE_NEGATIVE_INFINITE;
import static com.facebook.presto.iceberg.IcebergUtil.DOUBLE_NEGATIVE_ZERO;
import static com.facebook.presto.iceberg.IcebergUtil.DOUBLE_POSITIVE_INFINITE;
import static com.facebook.presto.iceberg.IcebergUtil.DOUBLE_POSITIVE_ZERO;
import static com.facebook.presto.iceberg.IcebergUtil.REAL_NEGATIVE_INFINITE;
import static com.facebook.presto.iceberg.IcebergUtil.REAL_NEGATIVE_ZERO;
import static com.facebook.presto.iceberg.IcebergUtil.REAL_POSITIVE_INFINITE;
import static com.facebook.presto.iceberg.IcebergUtil.REAL_POSITIVE_ZERO;
import static com.facebook.presto.iceberg.IcebergUtil.getAdjacentValue;
import static com.facebook.presto.iceberg.IcebergUtil.getMetadataColumnConstraints;
import static com.facebook.presto.iceberg.IcebergUtil.getNonMetadataColumnConstraints;
import static com.facebook.presto.iceberg.IcebergUtil.getTargetSplitSize;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestIcebergUtil
{
    @Test
    public void testPreviousValueForBigint()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getAdjacentValue(BIGINT, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(BIGINT, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));

        assertThat(getAdjacentValue(BIGINT, 1234L, true))
                .isEqualTo(Optional.of(1233L));

        assertThat(getAdjacentValue(BIGINT, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(BIGINT, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForBigint()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getAdjacentValue(BIGINT, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(BIGINT, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getAdjacentValue(BIGINT, 1234L, false))
                .isEqualTo(Optional.of(1235L));

        assertThat(getAdjacentValue(BIGINT, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(BIGINT, maxValue, false))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForIntegerAndDate()
    {
        long minValue = Integer.MIN_VALUE;
        long maxValue = Integer.MAX_VALUE;

        assertThat(getAdjacentValue(INTEGER, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(INTEGER, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));
        assertThat(getAdjacentValue(DATE, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(DATE, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));

        assertThat(getAdjacentValue(INTEGER, 1234L, true))
                .isEqualTo(Optional.of(1233L));
        assertThat(getAdjacentValue(DATE, 1234L, true))
                .isEqualTo(Optional.of(1233L));

        assertThat(getAdjacentValue(INTEGER, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(INTEGER, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
        assertThat(getAdjacentValue(DATE, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(DATE, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForIntegerAndDate()
    {
        long minValue = Integer.MIN_VALUE;
        long maxValue = Integer.MAX_VALUE;

        assertThat(getAdjacentValue(INTEGER, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(INTEGER, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));
        assertThat(getAdjacentValue(DATE, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(DATE, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getAdjacentValue(INTEGER, 1234L, false))
                .isEqualTo(Optional.of(1235L));
        assertThat(getAdjacentValue(DATE, 1234L, false))
                .isEqualTo(Optional.of(1235L));

        assertThat(getAdjacentValue(INTEGER, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(INTEGER, maxValue, false))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(DATE, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(DATE, maxValue, false))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForTimestamp()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getAdjacentValue(TIMESTAMP, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(TIMESTAMP, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));

        assertThat(getAdjacentValue(TIMESTAMP, 1234L, true))
                .isEqualTo(Optional.of(1233L));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, 1234L, true))
                .isEqualTo(Optional.of(1233L));

        assertThat(getAdjacentValue(TIMESTAMP, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(TIMESTAMP, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForTimestamp()
    {
        long minValue = Long.MIN_VALUE;
        long maxValue = Long.MAX_VALUE;

        assertThat(getAdjacentValue(TIMESTAMP, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(TIMESTAMP, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getAdjacentValue(TIMESTAMP, 1234L, false))
                .isEqualTo(Optional.of(1235L));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, 1234L, false))
                .isEqualTo(Optional.of(1235L));

        assertThat(getAdjacentValue(TIMESTAMP, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(TIMESTAMP, maxValue, false))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(TIMESTAMP_MICROSECONDS, maxValue, false))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForSmallInt()
    {
        long minValue = Short.MIN_VALUE;
        long maxValue = Short.MAX_VALUE;

        assertThat(getAdjacentValue(SMALLINT, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(SMALLINT, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));

        assertThat(getAdjacentValue(SMALLINT, 1234L, true))
                .isEqualTo(Optional.of(1233L));

        assertThat(getAdjacentValue(SMALLINT, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(SMALLINT, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForSmallInt()
    {
        long minValue = Short.MIN_VALUE;
        long maxValue = Short.MAX_VALUE;

        assertThat(getAdjacentValue(SMALLINT, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(SMALLINT, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getAdjacentValue(SMALLINT, 1234L, false))
                .isEqualTo(Optional.of(1235L));

        assertThat(getAdjacentValue(SMALLINT, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(SMALLINT, maxValue, false))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousValueForTinyInt()
    {
        long minValue = Byte.MIN_VALUE;
        long maxValue = Byte.MAX_VALUE;

        assertThat(getAdjacentValue(TINYINT, minValue, true))
                .isEqualTo(Optional.empty());
        assertThat(getAdjacentValue(TINYINT, minValue + 1, true))
                .isEqualTo(Optional.of(minValue));

        assertThat(getAdjacentValue(TINYINT, 123L, true))
                .isEqualTo(Optional.of(122L));

        assertThat(getAdjacentValue(TINYINT, maxValue - 1, true))
                .isEqualTo(Optional.of(maxValue - 2));
        assertThat(getAdjacentValue(TINYINT, maxValue, true))
                .isEqualTo(Optional.of(maxValue - 1));
    }

    @Test
    public void testNextValueForTinyInt()
    {
        long minValue = Byte.MIN_VALUE;
        long maxValue = Byte.MAX_VALUE;

        assertThat(getAdjacentValue(TINYINT, minValue, false))
                .isEqualTo(Optional.of(minValue + 1));
        assertThat(getAdjacentValue(TINYINT, minValue + 1, false))
                .isEqualTo(Optional.of(minValue + 2));

        assertThat(getAdjacentValue(TINYINT, 123L, false))
                .isEqualTo(Optional.of(124L));

        assertThat(getAdjacentValue(TINYINT, maxValue - 1, false))
                .isEqualTo(Optional.of(maxValue));
        assertThat(getAdjacentValue(TINYINT, maxValue, false))
                .isEqualTo(Optional.empty());
    }

    @Test
    public void testPreviousAndNextValueForDouble()
    {
        assertThat(getAdjacentValue(DOUBLE, DOUBLE_NEGATIVE_INFINITE, true))
                .isEqualTo(Optional.empty());
        assertThat(longBitsToDouble((long) getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_NEGATIVE_INFINITE, false).get(), true).get()))
                .isEqualTo(Double.NEGATIVE_INFINITY);

        assertThat(getAdjacentValue(DOUBLE, DOUBLE_POSITIVE_ZERO, true))
                .isEqualTo(getAdjacentValue(DOUBLE, DOUBLE_NEGATIVE_ZERO, true));
        assertThat(longBitsToDouble((long) getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_NEGATIVE_ZERO, false).get(), true).get()))
                .isEqualTo(0.0d);
        assertThat(getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_NEGATIVE_ZERO, false).get(), true).get())
                .isEqualTo(DOUBLE_POSITIVE_ZERO);
        assertThat(longBitsToDouble((long) getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_POSITIVE_ZERO, true).get(), false).get()))
                .isEqualTo(0.0d);
        assertThat(getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_POSITIVE_ZERO, true).get(), false).get())
                .isEqualTo(DOUBLE_POSITIVE_ZERO);

        assertThat(getAdjacentValue(DOUBLE, DOUBLE_POSITIVE_INFINITE, false))
                .isEqualTo(Optional.empty());
        assertThat(longBitsToDouble((long) getAdjacentValue(DOUBLE, getAdjacentValue(DOUBLE, DOUBLE_POSITIVE_INFINITE, true).get(), false).get()))
                .isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    public void testPreviousAndNextValueForReal()
    {
        assertThat(getAdjacentValue(REAL, REAL_NEGATIVE_INFINITE, true))
                .isEqualTo(Optional.empty());
        assertThat(intBitsToFloat((int) getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_NEGATIVE_INFINITE, false).get(), true).get()))
                .isEqualTo(Float.NEGATIVE_INFINITY);

        assertThat(getAdjacentValue(REAL, REAL_POSITIVE_ZERO, true))
                .isEqualTo(getAdjacentValue(REAL, REAL_NEGATIVE_ZERO, true));
        assertThat(intBitsToFloat((int) getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_NEGATIVE_ZERO, false).get(), true).get()))
                .isEqualTo(0.0f);
        assertThat(getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_NEGATIVE_ZERO, false).get(), true).get())
                .isEqualTo(REAL_POSITIVE_ZERO);
        assertThat(intBitsToFloat((int) getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_POSITIVE_ZERO, true).get(), false).get()))
                .isEqualTo(0.0f);
        assertThat(getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_POSITIVE_ZERO, true).get(), false).get())
                .isEqualTo(REAL_POSITIVE_ZERO);

        assertThat(getAdjacentValue(REAL, REAL_POSITIVE_INFINITE, false))
                .isEqualTo(Optional.empty());
        assertThat(intBitsToFloat((int) getAdjacentValue(REAL, getAdjacentValue(REAL, REAL_POSITIVE_INFINITE, true).get(), false).get()))
                .isEqualTo(Float.POSITIVE_INFINITY);
    }

    @Test
    public void testPreviousValueForOtherType()
    {
        assertThat(getAdjacentValue(VARCHAR, "anystr", true))
                .isEmpty();
        assertThat(getAdjacentValue(BOOLEAN, true, true))
                .isEmpty();
        assertThat(getAdjacentValue(TIME, 123L, true))
                .isEmpty();
        assertThat(getAdjacentValue(DecimalType.createDecimalType(8, 2), 12345L, true))
                .isEmpty();
        assertThat(getAdjacentValue(DecimalType.createDecimalType(20, 2),
                encodeScaledValue(new BigDecimal(111111111111111123.45)), true))
                .isEmpty();
    }

    @Test
    public void testNextValueForOtherType()
    {
        assertThat(getAdjacentValue(VARCHAR, "anystr", false))
                .isEmpty();
        assertThat(getAdjacentValue(BOOLEAN, true, false))
                .isEmpty();
        assertThat(getAdjacentValue(TIME, 123L, false))
                .isEmpty();
        assertThat(getAdjacentValue(DecimalType.createDecimalType(8, 2), 12345L, false))
                .isEmpty();
        assertThat(getAdjacentValue(DecimalType.createDecimalType(20, 2),
                encodeScaledValue(new BigDecimal(111111111111111123.45)), false))
                .isEmpty();
    }

    @Test
    public void testGetTargetSplitSize()
    {
        assertEquals(1024, getTargetSplitSize(1024, 512).toBytes());
        assertEquals(512, getTargetSplitSize(0, 512).toBytes());
    }

    @DataProvider
    public Object[][] compressionCodecMatrix()
    {
        return new Object[][] {
                // format, codec, expectedSupport
                {HiveStorageFormat.PARQUET, HiveCompressionCodec.NONE, true},
                {HiveStorageFormat.PARQUET, HiveCompressionCodec.SNAPPY, true},
                {HiveStorageFormat.PARQUET, HiveCompressionCodec.GZIP, true},
                {HiveStorageFormat.PARQUET, HiveCompressionCodec.LZ4, false},
                {HiveStorageFormat.PARQUET, HiveCompressionCodec.ZSTD, true},
                {HiveStorageFormat.ORC, HiveCompressionCodec.NONE, true},
                {HiveStorageFormat.ORC, HiveCompressionCodec.SNAPPY, true},
                {HiveStorageFormat.ORC, HiveCompressionCodec.GZIP, true},
                {HiveStorageFormat.ORC, HiveCompressionCodec.ZSTD, true},
                {HiveStorageFormat.ORC, HiveCompressionCodec.LZ4, true},
        };
    }

    @Test(dataProvider = "compressionCodecMatrix")
    public void testCompressionCodecSupport(HiveStorageFormat format, HiveCompressionCodec codec, boolean expectedSupport)
    {
        assertThat(codec.isSupportedStorageFormat(format))
                .as("Codec %s support for %s format", codec, format)
                .isEqualTo(expectedSupport);
    }

    @Test
    public void testParquetCompressionCodecAvailability()
    {
        assertThat(HiveCompressionCodec.NONE.getParquetCompressionCodec()).isNotNull();
        assertThat(HiveCompressionCodec.SNAPPY.getParquetCompressionCodec()).isNotNull();
        assertThat(HiveCompressionCodec.GZIP.getParquetCompressionCodec()).isNotNull();

        assertThat(HiveCompressionCodec.LZ4.getParquetCompressionCodec()).isNotNull();
        assertThat(HiveCompressionCodec.ZSTD.getParquetCompressionCodec()).isNotNull();
    }

    @Test
    public void testRoutesLastUpdatedSequenceNumberToMetadataConstraints()
    {
        Domain leSeqTen = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 10L)), false);
        TupleDomain<IcebergColumnHandle> all = TupleDomain.withColumnDomains(
                ImmutableMap.of(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE, leSeqTen));

        TupleDomain<IcebergColumnHandle> nonMetadata = getNonMetadataColumnConstraints(all);
        TupleDomain<IcebergColumnHandle> metadata = getMetadataColumnConstraints(all);

        assertThat(nonMetadata.getDomains().get()).doesNotContainKey(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE);
        assertThat(metadata.getDomains().get()).containsEntry(LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_HANDLE, leSeqTen);
    }

    @Test
    public void testToHiveColumnsWithTimeType()
    {
        List<Types.NestedField> icebergColumns = ImmutableList.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "time", Types.TimeType.get()),
                Types.NestedField.optional(3, "name", Types.StringType.get()));

        List<Column> hiveColumns = IcebergUtil.toHiveColumns(icebergColumns);
        assertThat(hiveColumns).hasSize(3);

        assertThat(hiveColumns.get(0).getName()).isEqualTo("id");
        assertThat(hiveColumns.get(0).getType()).isEqualTo(HiveType.HIVE_LONG);

        assertThat(hiveColumns.get(1).getName()).isEqualTo("time");
        assertThat(hiveColumns.get(1).getType()).isEqualTo(HiveType.HIVE_LONG);

        assertThat(hiveColumns.get(2).getName()).isEqualTo("name");
        assertThat(hiveColumns.get(2).getType()).isEqualTo(HiveType.HIVE_STRING);
    }

    /**
     * Verifies that unknown fields are restored even when the file also differs from the table for
     * other schema-evolution reasons (e.g. a non-unknown field added after the file was written).
     */
    @Test
    public void testReadTypeUnknownWithOtherSchemaEvolution()
    {
        // table: ROW(a INTEGER, u UNKNOWN, b BIGINT); file: ROW(a INTEGER) — b was added after file written
        RowType tableType = RowType.from(ImmutableList.of(
                RowType.field("a", INTEGER),
                RowType.field("u", UNKNOWN),
                RowType.field("b", BIGINT)));
        RowType fileType = RowType.from(ImmutableList.of(
                RowType.field("a", INTEGER)));

        // The merged read type must include both the unknown field and the newer non-unknown field
        RowType expected = tableType;
        assertThat(UnknownFieldTypes.readType(tableType, fileType)).isEqualTo(expected);
    }

    @Test
    public void testReadTypeUnknownOnlyDifference()
    {
        // table: ROW(a INTEGER, u UNKNOWN, b BIGINT); file: ROW(a INTEGER, b BIGINT)
        RowType tableType = RowType.from(ImmutableList.of(
                RowType.field("a", INTEGER),
                RowType.field("u", UNKNOWN),
                RowType.field("b", BIGINT)));
        RowType fileType = RowType.from(ImmutableList.of(
                RowType.field("a", INTEGER),
                RowType.field("b", BIGINT)));

        assertThat(UnknownFieldTypes.readType(tableType, fileType)).isEqualTo(tableType);
    }

    @Test
    public void testReadTypeArrayWithUnknownElement()
    {
        // ARRAY(ROW(x INTEGER, u UNKNOWN)) vs file ARRAY(ROW(x INTEGER))
        RowType tableRow = RowType.from(ImmutableList.of(
                RowType.field("x", INTEGER),
                RowType.field("u", UNKNOWN)));
        RowType fileRow = RowType.from(ImmutableList.of(
                RowType.field("x", INTEGER)));

        ArrayType tableType = new ArrayType(tableRow);
        ArrayType fileType = new ArrayType(fileRow);

        assertThat(UnknownFieldTypes.readType(tableType, fileType)).isEqualTo(tableType);
    }

    @Test
    public void testReadTypeMapWithUnknownInKey()
    {
        // MAP(ROW(k INTEGER, u UNKNOWN), VARCHAR) vs file MAP(ROW(k INTEGER), VARCHAR)
        RowType tableKey = RowType.from(ImmutableList.of(
                RowType.field("k", INTEGER),
                RowType.field("u", UNKNOWN)));
        RowType fileKey = RowType.from(ImmutableList.of(
                RowType.field("k", INTEGER)));
        MapType tableType = new MapType(tableKey, VARCHAR,
                MethodHandleUtil.methodHandle(TestIcebergUtil.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestIcebergUtil.class, "throwUnsupportedOperation"));
        MapType fileType = new MapType(fileKey, VARCHAR,
                MethodHandleUtil.methodHandle(TestIcebergUtil.class, "throwUnsupportedOperation"),
                MethodHandleUtil.methodHandle(TestIcebergUtil.class, "throwUnsupportedOperation"));

        MapType result = (MapType) UnknownFieldTypes.readType(tableType, fileType);
        assertThat(result.getKeyType()).isEqualTo(tableKey);
        assertThat(result.getValueType()).isEqualTo(VARCHAR);
    }

    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }

    @Test
    public void testReadTypeNoUnknown()
    {
        // No unknown fields — file type should be returned unchanged
        RowType tableType = RowType.from(ImmutableList.of(
                RowType.field("a", INTEGER),
                RowType.field("b", BIGINT)));
        RowType fileType = RowType.from(ImmutableList.of(
                RowType.field("a", INTEGER),
                RowType.field("b", BIGINT)));

        assertThat(UnknownFieldTypes.readType(tableType, fileType)).isSameAs(fileType);
    }

    /**
     * When a ROW column has both hyphenated field names and an unknown field, the Parquet file stores
     * the hyphenated names Avro-encoded (e.g. "field-one" → "field_x2done") and omits the unknown
     * field entirely. The merged read type must match the encoded name back to the original field and
     * still restore the unknown field.
     */
    @Test
    public void testReadTypeHyphenatedFieldNameWithUnknown()
    {
        // table: ROW("field-one" INTEGER, "null-col" UNKNOWN)
        // file: ROW(field_x2done INTEGER)  — Avro-encoded, unknown field absent
        RowType tableType = RowType.from(ImmutableList.of(
                RowType.field("field-one", INTEGER),
                RowType.field("null-col", UNKNOWN)));
        RowType fileType = RowType.from(ImmutableList.of(
                RowType.field("field_x2done", INTEGER)));

        // "field-one" must be matched to "field_x2done" (same field, Avro-encoded). The result
        // uses the encoded name so that constructField can locate it in the Parquet GroupColumnIO.
        // "null-col" is unknown and restored from the table type under its original name.
        RowType expected = RowType.from(ImmutableList.of(
                RowType.field("field_x2done", INTEGER),
                RowType.field("null-col", UNKNOWN)));
        assertThat(UnknownFieldTypes.readType(tableType, fileType)).isEqualTo(expected);
    }

    /**
     * Hive has no equivalent of the Iceberg V3 unknown type, so it is recorded as Hive's all-null
     * type, which is also what Spark records for its NullType.
     */
    @Test
    public void testToHiveColumnsWithUnknownType()
    {
        List<Types.NestedField> icebergColumns = ImmutableList.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "unknown_column", Types.UnknownType.get()),
                Types.NestedField.optional(3, "nested", Types.StructType.of(
                        Types.NestedField.optional(4, "unknown_field", Types.UnknownType.get()))));

        List<Column> hiveColumns = IcebergUtil.toHiveColumns(icebergColumns);
        assertThat(hiveColumns).hasSize(3);

        assertThat(hiveColumns.get(0).getName()).isEqualTo("id");
        assertThat(hiveColumns.get(0).getType()).isEqualTo(HiveType.HIVE_LONG);

        assertThat(hiveColumns.get(1).getName()).isEqualTo("unknown_column");
        assertThat(hiveColumns.get(1).getType()).isEqualTo(HiveType.valueOf("void"));

        assertThat(hiveColumns.get(2).getName()).isEqualTo("nested");
        assertThat(hiveColumns.get(2).getType()).isEqualTo(HiveType.valueOf("struct<unknown_field:void>"));
    }
}
