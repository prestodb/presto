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
package com.facebook.presto.parquet.reader;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PrimitiveIterator;

import static java.util.Arrays.asList;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.filter2.predicate.LogicalInverter.invert;
import static org.apache.parquet.schema.OriginalType.DECIMAL;
import static org.apache.parquet.schema.OriginalType.UINT_8;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestColumnIndexBuilder
{
    public static class BinaryDecimalIsNullOrZeroUdp
            extends UserDefinedPredicate<Binary>
    {
        private static final Binary ZERO = decimalBinary("0.0");

        @Override
        public boolean keep(Binary value)
        {
            return value == null || value.equals(ZERO);
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics)
        {
            Comparator<Binary> cmp = statistics.getComparator();
            return cmp.compare(statistics.getMin(), ZERO) > 0 || cmp.compare(statistics.getMax(), ZERO) < 0;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics)
        {
            Comparator<Binary> cmp = statistics.getComparator();
            return cmp.compare(statistics.getMin(), ZERO) == 0 && cmp.compare(statistics.getMax(), ZERO) == 0;
        }
    }

    public static class BinaryUtf8StartsWithB
            extends UserDefinedPredicate<Binary>
    {
        private static final Binary B = stringBinary("B");
        private static final Binary C = stringBinary("C");

        @Override
        public boolean keep(Binary value)
        {
            return value != null && value.length() > 0 && value.getBytesUnsafe()[0] == 'B';
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics)
        {
            Comparator<Binary> cmp = statistics.getComparator();
            return cmp.compare(statistics.getMin(), C) >= 0 || cmp.compare(statistics.getMax(), B) < 0;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics)
        {
            Comparator<Binary> cmp = statistics.getComparator();
            return cmp.compare(statistics.getMin(), B) >= 0 && cmp.compare(statistics.getMax(), C) < 0;
        }
    }

    public static class BooleanIsTrueOrNull
            extends UserDefinedPredicate<Boolean>
    {
        @Override
        public boolean keep(Boolean value)
        {
            return value == null || value;
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Boolean> statistics)
        {
            return statistics.getComparator().compare(statistics.getMax(), true) != 0;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Boolean> statistics)
        {
            return statistics.getComparator().compare(statistics.getMin(), true) == 0;
        }
    }

    public static class DoubleIsInteger
            extends UserDefinedPredicate<Double>
    {
        @Override
        public boolean keep(Double value)
        {
            return value != null && Math.floor(value) == value;
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Double> statistics)
        {
            double min = statistics.getMin();
            double max = statistics.getMax();
            Comparator<Double> cmp = statistics.getComparator();
            return cmp.compare(Math.floor(min), Math.floor(max)) == 0 && cmp.compare(Math.floor(min), min) != 0
                    && cmp.compare(Math.floor(max), max) != 0;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Double> statistics)
        {
            double min = statistics.getMin();
            double max = statistics.getMax();
            Comparator<Double> cmp = statistics.getComparator();
            return cmp.compare(min, max) == 0 && cmp.compare(Math.floor(min), min) == 0;
        }
    }

    public static class FloatIsInteger
            extends UserDefinedPredicate<Float>
    {
        private static float floor(float value)
        {
            return (float) Math.floor(value);
        }

        @Override
        public boolean keep(Float value)
        {
            return value != null && Math.floor(value) == value;
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Float> statistics)
        {
            float min = statistics.getMin();
            float max = statistics.getMax();
            Comparator<Float> cmp = statistics.getComparator();
            return cmp.compare(floor(min), floor(max)) == 0 && cmp.compare(floor(min), min) != 0
                    && cmp.compare(floor(max), max) != 0;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Float> statistics)
        {
            float min = statistics.getMin();
            float max = statistics.getMax();
            Comparator<Float> cmp = statistics.getComparator();
            return cmp.compare(min, max) == 0 && cmp.compare(floor(min), min) == 0;
        }
    }

    public static class IntegerIsDivisibleWith3
            extends UserDefinedPredicate<Integer>
    {
        @Override
        public boolean keep(Integer value)
        {
            return value != null && value % 3 == 0;
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Integer> statistics)
        {
            int min = statistics.getMin();
            int max = statistics.getMax();
            return min % 3 != 0 && max % 3 != 0 && max - min < 3;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Integer> statistics)
        {
            int min = statistics.getMin();
            int max = statistics.getMax();
            return min == max && min % 3 == 0;
        }
    }

    public static class LongIsDivisibleWith3
            extends UserDefinedPredicate<Long>
    {
        @Override
        public boolean keep(Long value)
        {
            return value != null && value % 3 == 0;
        }

        @Override
        public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Long> statistics)
        {
            long min = statistics.getMin();
            long max = statistics.getMax();
            return min % 3 != 0 && max % 3 != 0 && max - min < 3;
        }

        @Override
        public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Long> statistics)
        {
            long min = statistics.getMin();
            long max = statistics.getMax();
            return min == max && min % 3 == 0;
        }
    }

    @Test
    public void testBuildBinaryDecimal()
    {
        PrimitiveType type = Types.required(BINARY).as(DECIMAL).precision(12).scale(2).named("test_binary_decimal");
        ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        //assertThat(builder, instanceOf(BinaryColumnIndexBuilder.class));
        assertNull(builder.build());
        Operators.BinaryColumn col = binaryColumn("test_col");

        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, decimalBinary("-0.17"), decimalBinary("1234567890.12")));
        builder.add(sb.stats(type, decimalBinary("-234.23"), null, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, decimalBinary("-9999293.23"), decimalBinary("2348978.45")));
        builder.add(sb.stats(type, null, null, null, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, decimalBinary("87656273")));
        assertEquals(8, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        ColumnIndex columnIndex = builder.build();
        assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 0, 3, 3, 0, 4, 2, 0);
        assertCorrectNullPages(columnIndex, true, false, false, true, false, true, true, false);
        assertCorrectValues(columnIndex.getMaxValues(),
                null,
                decimalBinary("1234567890.12"),
                decimalBinary("-234.23"),
                null,
                decimalBinary("2348978.45"),
                null,
                null,
                decimalBinary("87656273"));
        assertCorrectValues(columnIndex.getMinValues(),
                null,
                decimalBinary("-0.17"),
                decimalBinary("-234.23"),
                null,
                decimalBinary("-9999293.23"),
                null,
                null,
                decimalBinary("87656273"));
        assertCorrectFiltering(columnIndex, eq(col, decimalBinary("0.0")), 1, 4);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 5, 6);
        assertCorrectFiltering(columnIndex, notEq(col, decimalBinary("87656273")), 0, 1, 2, 3, 4, 5, 6);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 4, 7);
        assertCorrectFiltering(columnIndex, gt(col, decimalBinary("2348978.45")), 1);
        assertCorrectFiltering(columnIndex, gtEq(col, decimalBinary("2348978.45")), 1, 4);
        assertCorrectFiltering(columnIndex, lt(col, decimalBinary("-234.23")), 4);
        assertCorrectFiltering(columnIndex, ltEq(col, decimalBinary("-234.23")), 2, 4);
        assertCorrectFiltering(columnIndex, userDefined(col, BinaryDecimalIsNullOrZeroUdp.class), 0, 1, 2, 3, 4, 5, 6);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryDecimalIsNullOrZeroUdp.class)), 1, 2, 4, 7);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null, null, null));
        builder.add(sb.stats(type, decimalBinary("-9999293.23"), decimalBinary("-234.23")));
        builder.add(sb.stats(type, decimalBinary("-0.17"), decimalBinary("87656273")));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, decimalBinary("87656273")));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, decimalBinary("1234567890.12"), null, null, null));
        builder.add(sb.stats(type, null, null, null));
        assertEquals(8, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 4, 0, 0, 2, 0, 2, 3, 3);
        assertCorrectNullPages(columnIndex, true, false, false, true, false, true, false, true);
        assertCorrectValues(columnIndex.getMaxValues(),
                null,
                decimalBinary("-234.23"),
                decimalBinary("87656273"),
                null,
                decimalBinary("87656273"),
                null,
                decimalBinary("1234567890.12"),
                null);
        assertCorrectValues(columnIndex.getMinValues(),
                null,
                decimalBinary("-9999293.23"),
                decimalBinary("-0.17"),
                null,
                decimalBinary("87656273"),
                null,
                decimalBinary("1234567890.12"),
                null);
        assertCorrectFiltering(columnIndex, eq(col, decimalBinary("87656273")), 2, 4);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 3, 5, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, decimalBinary("87656273")), 0, 1, 2, 3, 5, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 4, 6);
        assertCorrectFiltering(columnIndex, gt(col, decimalBinary("87656273")), 6);
        assertCorrectFiltering(columnIndex, gtEq(col, decimalBinary("87656273")), 2, 4, 6);
        assertCorrectFiltering(columnIndex, lt(col, decimalBinary("-0.17")), 1);
        assertCorrectFiltering(columnIndex, ltEq(col, decimalBinary("-0.17")), 1, 2);
        assertCorrectFiltering(columnIndex, userDefined(col, BinaryDecimalIsNullOrZeroUdp.class), 0, 2, 3, 5, 6, 7);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryDecimalIsNullOrZeroUdp.class)), 1, 2, 4, 6);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, decimalBinary("1234567890.12"), null, null, null));
        builder.add(sb.stats(type, null, null, null, null));
        builder.add(sb.stats(type, decimalBinary("1234567890.12"), decimalBinary("87656273")));
        builder.add(sb.stats(type, decimalBinary("987656273"), decimalBinary("-0.17")));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, decimalBinary("-234.23"), decimalBinary("-9999293.23")));
        assertEquals(8, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 3, 2, 3, 4, 0, 0, 2, 0);
        assertCorrectNullPages(columnIndex, true, true, false, true, false, false, true, false);
        assertCorrectValues(columnIndex.getMaxValues(),
                null,
                null,
                decimalBinary("1234567890.12"),
                null,
                decimalBinary("1234567890.12"),
                decimalBinary("987656273"),
                null,
                decimalBinary("-234.23"));
        assertCorrectValues(columnIndex.getMinValues(),
                null,
                null,
                decimalBinary("1234567890.12"),
                null,
                decimalBinary("87656273"),
                decimalBinary("-0.17"),
                null,
                decimalBinary("-9999293.23"));
        assertCorrectFiltering(columnIndex, eq(col, decimalBinary("1234567890.12")), 2, 4);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 6);
        assertCorrectFiltering(columnIndex, notEq(col, decimalBinary("0.0")), 0, 1, 2, 3, 4, 5, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, null), 2, 4, 5, 7);
        assertCorrectFiltering(columnIndex, gt(col, decimalBinary("1234567890.12")));
        assertCorrectFiltering(columnIndex, gtEq(col, decimalBinary("1234567890.12")), 2, 4);
        assertCorrectFiltering(columnIndex, lt(col, decimalBinary("-0.17")), 7);
        assertCorrectFiltering(columnIndex, ltEq(col, decimalBinary("-0.17")), 5, 7);
        assertCorrectFiltering(columnIndex, userDefined(col, BinaryDecimalIsNullOrZeroUdp.class), 0, 1, 2, 3, 5, 6);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryDecimalIsNullOrZeroUdp.class)), 2, 4, 5, 7);
    }

    @Test
    public void testBuildBinaryUtf8()
    {
        PrimitiveType type = Types.required(BINARY).as(UTF8).named("test_binary_utf8");
        ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        //assertThat(builder, instanceOf(BinaryColumnIndexBuilder.class));
        assertNull(builder.build());
        Operators.BinaryColumn col = binaryColumn("test_col");

        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, stringBinary("Jeltz"), stringBinary("Slartibartfast"), null, null));
        builder.add(sb.stats(type, null, null, null, null, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, stringBinary("Beeblebrox"), stringBinary("Prefect")));
        builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Trilian"), null));
        builder.add(sb.stats(type, stringBinary("Beeblebrox")));
        builder.add(sb.stats(type, null, null));
        assertEquals(8, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        ColumnIndex columnIndex = builder.build();
        assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 2, 5, 2, 0, 1, 0, 2);
        assertCorrectNullPages(columnIndex, true, false, true, true, false, false, false, true);
        assertCorrectValues(columnIndex.getMaxValues(),
                null,
                stringBinary("Slartibartfast"),
                null,
                null,
                stringBinary("Prefect"),
                stringBinary("Trilian"),
                stringBinary("Beeblebrox"),
                null);
        assertCorrectValues(columnIndex.getMinValues(),
                null,
                stringBinary("Jeltz"),
                null,
                null,
                stringBinary("Beeblebrox"),
                stringBinary("Dent"),
                stringBinary("Beeblebrox"),
                null);
        assertCorrectFiltering(columnIndex, eq(col, stringBinary("Marvin")), 1, 4, 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 5, 7);
        assertCorrectFiltering(columnIndex, notEq(col, stringBinary("Beeblebrox")), 0, 1, 2, 3, 4, 5, 7);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 4, 5, 6);
        assertCorrectFiltering(columnIndex, gt(col, stringBinary("Prefect")), 1, 5);
        assertCorrectFiltering(columnIndex, gtEq(col, stringBinary("Prefect")), 1, 4, 5);
        assertCorrectFiltering(columnIndex, lt(col, stringBinary("Dent")), 4, 6);
        assertCorrectFiltering(columnIndex, ltEq(col, stringBinary("Dent")), 4, 5, 6);
        assertCorrectFiltering(columnIndex, userDefined(col, BinaryUtf8StartsWithB.class), 4, 6);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryUtf8StartsWithB.class)), 0, 1, 2, 3, 4, 5, 7);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, stringBinary("Beeblebrox"), stringBinary("Dent"), null, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null, null, null, null));
        builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Jeltz")));
        builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Prefect"), null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, stringBinary("Slartibartfast")));
        builder.add(sb.stats(type, null, null));
        assertEquals(8, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 2, 5, 0, 1, 2, 0, 2);
        assertCorrectNullPages(columnIndex, false, true, true, false, false, true, false, true);
        assertCorrectValues(columnIndex.getMaxValues(),
                stringBinary("Dent"),
                null,
                null,
                stringBinary("Jeltz"),
                stringBinary("Prefect"),
                null,
                stringBinary("Slartibartfast"),
                null);
        assertCorrectValues(columnIndex.getMinValues(),
                stringBinary("Beeblebrox"),
                null,
                null,
                stringBinary("Dent"),
                stringBinary("Dent"),
                null,
                stringBinary("Slartibartfast"),
                null);
        assertCorrectFiltering(columnIndex, eq(col, stringBinary("Jeltz")), 3, 4);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 4, 5, 7);
        assertCorrectFiltering(columnIndex, notEq(col, stringBinary("Slartibartfast")), 0, 1, 2, 3, 4, 5, 7);
        assertCorrectFiltering(columnIndex, notEq(col, null), 0, 3, 4, 6);
        assertCorrectFiltering(columnIndex, gt(col, stringBinary("Marvin")), 4, 6);
        assertCorrectFiltering(columnIndex, gtEq(col, stringBinary("Marvin")), 4, 6);
        assertCorrectFiltering(columnIndex, lt(col, stringBinary("Dent")), 0);
        assertCorrectFiltering(columnIndex, ltEq(col, stringBinary("Dent")), 0, 3, 4);
        assertCorrectFiltering(columnIndex, userDefined(col, BinaryUtf8StartsWithB.class), 0);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryUtf8StartsWithB.class)), 0, 1, 2, 3, 4, 5, 6, 7);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, stringBinary("Slartibartfast")));
        builder.add(sb.stats(type, null, null, null, null, null));
        builder.add(sb.stats(type, stringBinary("Prefect"), stringBinary("Jeltz"), null));
        builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Dent")));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, stringBinary("Dent"), stringBinary("Beeblebrox"), null, null));
        assertEquals(8, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 0, 5, 1, 0, 2, 2, 2);
        assertCorrectNullPages(columnIndex, true, false, true, false, false, true, true, false);
        assertCorrectValues(columnIndex.getMaxValues(),
                null,
                stringBinary("Slartibartfast"),
                null,
                stringBinary("Prefect"),
                stringBinary("Dent"),
                null,
                null,
                stringBinary("Dent"));
        assertCorrectValues(columnIndex.getMinValues(),
                null,
                stringBinary("Slartibartfast"),
                null,
                stringBinary("Jeltz"),
                stringBinary("Dent"),
                null,
                null,
                stringBinary("Beeblebrox"));
        assertCorrectFiltering(columnIndex, eq(col, stringBinary("Marvin")), 3);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 5, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, stringBinary("Dent")), 0, 1, 2, 3, 5, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 4, 7);
        assertCorrectFiltering(columnIndex, gt(col, stringBinary("Prefect")), 1);
        assertCorrectFiltering(columnIndex, gtEq(col, stringBinary("Prefect")), 1, 3);
        assertCorrectFiltering(columnIndex, lt(col, stringBinary("Marvin")), 3, 4, 7);
        assertCorrectFiltering(columnIndex, ltEq(col, stringBinary("Marvin")), 3, 4, 7);
        assertCorrectFiltering(columnIndex, userDefined(col, BinaryUtf8StartsWithB.class), 7);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryUtf8StartsWithB.class)), 0, 1, 2, 3, 4, 5, 6, 7);
    }

    @Test
    public void testStaticBuildBinary()
    {
        ColumnIndex columnIndex = ColumnIndexBuilder.build(
                Types.required(BINARY).as(UTF8).named("test_binary_utf8"),
                BoundaryOrder.ASCENDING,
                asList(true, true, false, false, true, false, true, false),
                asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L),
                toBBList(
                        null,
                        null,
                        stringBinary("Beeblebrox"),
                        stringBinary("Dent"),
                        null,
                        stringBinary("Jeltz"),
                        null,
                        stringBinary("Slartibartfast")),
                toBBList(
                        null,
                        null,
                        stringBinary("Dent"),
                        stringBinary("Dent"),
                        null,
                        stringBinary("Prefect"),
                        null,
                        stringBinary("Slartibartfast")));
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 1, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectNullPages(columnIndex, true, true, false, false, true, false, true, false);
        assertCorrectValues(columnIndex.getMaxValues(),
                null,
                null,
                stringBinary("Dent"),
                stringBinary("Dent"),
                null,
                stringBinary("Prefect"),
                null,
                stringBinary("Slartibartfast"));
        assertCorrectValues(columnIndex.getMinValues(),
                null,
                null,
                stringBinary("Beeblebrox"),
                stringBinary("Dent"),
                null,
                stringBinary("Jeltz"),
                null,
                stringBinary("Slartibartfast"));
    }

    @Test
    public void testFilterWithoutNullCounts()
    {
        ColumnIndex columnIndex = ColumnIndexBuilder.build(
                Types.required(BINARY).as(UTF8).named("test_binary_utf8"),
                BoundaryOrder.ASCENDING,
                asList(true, true, false, false, true, false, true, false),
                null,
                toBBList(
                        null,
                        null,
                        stringBinary("Beeblebrox"),
                        stringBinary("Dent"),
                        null,
                        stringBinary("Jeltz"),
                        null,
                        stringBinary("Slartibartfast")),
                toBBList(
                        null,
                        null,
                        stringBinary("Dent"),
                        stringBinary("Dent"),
                        null,
                        stringBinary("Prefect"),
                        null,
                        stringBinary("Slartibartfast")));
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertNull(columnIndex.getNullCounts());
        assertCorrectNullPages(columnIndex, true, true, false, false, true, false, true, false);
        assertCorrectValues(columnIndex.getMaxValues(),
                null,
                null,
                stringBinary("Dent"),
                stringBinary("Dent"),
                null,
                stringBinary("Prefect"),
                null,
                stringBinary("Slartibartfast"));
        assertCorrectValues(columnIndex.getMinValues(),
                null,
                null,
                stringBinary("Beeblebrox"),
                stringBinary("Dent"),
                null,
                stringBinary("Jeltz"),
                null,
                stringBinary("Slartibartfast"));

        Operators.BinaryColumn col = binaryColumn("test_col");
        assertCorrectFiltering(columnIndex, eq(col, stringBinary("Dent")), 2, 3);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 5, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, stringBinary("Dent")), 0, 1, 2, 3, 4, 5, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, null), 2, 3, 5, 7);
        assertCorrectFiltering(columnIndex, userDefined(col, BinaryDecimalIsNullOrZeroUdp.class), 0, 1, 2, 3, 4, 5, 6, 7);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, BinaryDecimalIsNullOrZeroUdp.class)), 2, 3, 5, 7);
    }

    @Test
    public void testBuildBoolean()
    {
        PrimitiveType type = Types.required(BOOLEAN).named("test_boolean");
        ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        //assertThat(builder, instanceOf(BooleanColumnIndexBuilder.class));
        assertNull(builder.build());
        Operators.BooleanColumn col = booleanColumn("test_col");

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(type, false, true));
        builder.add(sb.stats(type, true, false, null));
        builder.add(sb.stats(type, true, true, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, false, false));
        assertEquals(5, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        ColumnIndex columnIndex = builder.build();
        assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0);
        assertCorrectNullPages(columnIndex, false, false, false, true, false);
        assertCorrectValues(columnIndex.getMaxValues(), true, true, true, null, false);
        assertCorrectValues(columnIndex.getMinValues(), false, false, true, null, false);
        assertCorrectFiltering(columnIndex, eq(col, true), 0, 1, 2);
        assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
        assertCorrectFiltering(columnIndex, notEq(col, true), 0, 1, 2, 3, 4);
        assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4);
        assertCorrectFiltering(columnIndex, userDefined(col, BooleanIsTrueOrNull.class), 0, 1, 2, 3);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, BooleanIsTrueOrNull.class)), 0, 1, 4);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, false, false));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, null, null, null, null));
        builder.add(sb.stats(type, false, true, null));
        builder.add(sb.stats(type, false, true, null, null));
        builder.add(sb.stats(type, null, null, null));
        assertEquals(7, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 0, 3, 4, 1, 2, 3);
        assertCorrectNullPages(columnIndex, true, false, true, true, false, false, true);
        assertCorrectValues(columnIndex.getMaxValues(), null, false, null, null, true, true, null);
        assertCorrectValues(columnIndex.getMinValues(), null, false, null, null, false, false, null);
        assertCorrectFiltering(columnIndex, eq(col, true), 4, 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 5, 6);
        assertCorrectFiltering(columnIndex, notEq(col, true), 0, 1, 2, 3, 4, 5, 6);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 4, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, BooleanIsTrueOrNull.class), 0, 2, 3, 4, 5, 6);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, BooleanIsTrueOrNull.class)), 1, 4, 5);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, true, true));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, null, null, null, null));
        builder.add(sb.stats(type, true, false, null));
        builder.add(sb.stats(type, false, false, null, null));
        builder.add(sb.stats(type, null, null, null));
        assertEquals(7, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 0, 3, 4, 1, 2, 3);
        assertCorrectNullPages(columnIndex, true, false, true, true, false, false, true);
        assertCorrectValues(columnIndex.getMaxValues(), null, true, null, null, true, false, null);
        assertCorrectValues(columnIndex.getMinValues(), null, true, null, null, false, false, null);
        assertCorrectFiltering(columnIndex, eq(col, true), 1, 4);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 5, 6);
        assertCorrectFiltering(columnIndex, notEq(col, true), 0, 2, 3, 4, 5, 6);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 4, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, BooleanIsTrueOrNull.class), 0, 1, 2, 3, 4, 5, 6);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, BooleanIsTrueOrNull.class)), 4, 5);
    }

    @Test
    public void testStaticBuildBoolean()
    {
        ColumnIndex columnIndex = ColumnIndexBuilder.build(
                Types.required(BOOLEAN).named("test_boolean"),
                BoundaryOrder.DESCENDING,
                asList(false, true, false, true, false, true),
                asList(9L, 8L, 7L, 6L, 5L, 0L),
                toBBList(false, null, false, null, true, null),
                toBBList(true, null, false, null, true, null));
        assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 9, 8, 7, 6, 5, 0);
        assertCorrectNullPages(columnIndex, false, true, false, true, false, true);
        assertCorrectValues(columnIndex.getMaxValues(), true, null, false, null, true, null);
        assertCorrectValues(columnIndex.getMinValues(), false, null, false, null, true, null);
    }

    @Test
    public void testBuildDouble()
    {
        PrimitiveType type = Types.required(DOUBLE).named("test_double");
        ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        //assertThat(builder, instanceOf(DoubleColumnIndexBuilder.class));
        assertNull(builder.build());
        Operators.DoubleColumn col = doubleColumn("test_col");

        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(type, -4.2, -4.1));
        builder.add(sb.stats(type, -11.7, 7.0, null));
        builder.add(sb.stats(type, 2.2, 2.2, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 1.9, 2.32));
        builder.add(sb.stats(type, -21.0, 8.1));
        assertEquals(6, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        ColumnIndex columnIndex = builder.build();
        assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
        assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
        assertCorrectValues(columnIndex.getMaxValues(), -4.1, 7.0, 2.2, null, 2.32, 8.1);
        assertCorrectValues(columnIndex.getMinValues(), -4.2, -11.7, 2.2, null, 1.9, -21.0);
        assertCorrectFiltering(columnIndex, eq(col, 0.0), 1, 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
        assertCorrectFiltering(columnIndex, notEq(col, 2.2), 0, 1, 2, 3, 4, 5);
        assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, gt(col, 2.2), 1, 4, 5);
        assertCorrectFiltering(columnIndex, gtEq(col, 2.2), 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, lt(col, -4.2), 1, 5);
        assertCorrectFiltering(columnIndex, ltEq(col, -4.2), 0, 1, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, DoubleIsInteger.class), 1, 4, 5);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, DoubleIsInteger.class)), 0, 1, 2, 3, 4, 5);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, -532.3, -345.2, null, null));
        builder.add(sb.stats(type, -234.7, -234.6, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, -234.6, 2.99999));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 3.0, 42.83));
        builder.add(sb.stats(type, null, null));
        assertEquals(9, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
        assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
        assertCorrectValues(columnIndex.getMaxValues(), null, -345.2, -234.6, null, null, 2.99999, null, 42.83, null);
        assertCorrectValues(columnIndex.getMinValues(), null, -532.3, -234.7, null, null, -234.6, null, 3.0, null);
        assertCorrectFiltering(columnIndex, eq(col, 0.0), 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 6, 8);
        assertCorrectFiltering(columnIndex, notEq(col, 0.0), 0, 1, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 5, 7);
        assertCorrectFiltering(columnIndex, gt(col, 2.99999), 7);
        assertCorrectFiltering(columnIndex, gtEq(col, 2.99999), 5, 7);
        assertCorrectFiltering(columnIndex, lt(col, -234.6), 1, 2);
        assertCorrectFiltering(columnIndex, ltEq(col, -234.6), 1, 2, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, DoubleIsInteger.class), 1, 5, 7);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, DoubleIsInteger.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null, null, null, null));
        builder.add(sb.stats(type, 532.3, 345.2));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 234.7, 234.6, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 234.69, -2.99999));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, -3.0, -42.83));
        assertEquals(9, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
        assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
        assertCorrectValues(columnIndex.getMaxValues(), null, 532.3, null, 234.7, null, 234.69, null, null, -3.0);
        assertCorrectValues(columnIndex.getMinValues(), null, 345.2, null, 234.6, null, -2.99999, null, null, -42.83);
        assertCorrectFiltering(columnIndex, eq(col, 234.6), 3, 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, 2.2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 5, 8);
        assertCorrectFiltering(columnIndex, gt(col, 2.2), 1, 3, 5);
        assertCorrectFiltering(columnIndex, gtEq(col, 234.69), 1, 3, 5);
        assertCorrectFiltering(columnIndex, lt(col, -2.99999), 8);
        assertCorrectFiltering(columnIndex, ltEq(col, -2.99999), 5, 8);
        assertCorrectFiltering(columnIndex, userDefined(col, DoubleIsInteger.class), 1, 5, 8);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, DoubleIsInteger.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    }

    @Test
    public void testBuildDoubleZeroNaN()
    {
        PrimitiveType type = Types.required(DOUBLE).named("test_double");
        ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(type, -1.0, -0.0));
        builder.add(sb.stats(type, 0.0, 1.0));
        builder.add(sb.stats(type, 1.0, 100.0));
        ColumnIndex columnIndex = builder.build();
        assertCorrectValues(columnIndex.getMinValues(), -1.0, -0.0, 1.0);
        assertCorrectValues(columnIndex.getMaxValues(), 0.0, 1.0, 100.0);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        builder.add(sb.stats(type, -1.0, -0.0));
        builder.add(sb.stats(type, 0.0, Double.NaN));
        builder.add(sb.stats(type, 1.0, 100.0));
        assertNull(builder.build());
    }

    @Test
    public void testStaticBuildDouble()
    {
        ColumnIndex columnIndex = ColumnIndexBuilder.build(
                Types.required(DOUBLE).named("test_double"),
                BoundaryOrder.UNORDERED,
                asList(false, false, false, false, false, false),
                asList(0L, 1L, 2L, 3L, 4L, 5L),
                toBBList(-1.0, -2.0, -3.0, -4.0, -5.0, -6.0),
                toBBList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0));
        assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 4, 5);
        assertCorrectNullPages(columnIndex, false, false, false, false, false, false);
        assertCorrectValues(columnIndex.getMaxValues(), 1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
        assertCorrectValues(columnIndex.getMinValues(), -1.0, -2.0, -3.0, -4.0, -5.0, -6.0);
    }

    @Test
    public void testBuildFloat()
    {
        PrimitiveType type = Types.required(FLOAT).named("test_float");
        ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        //assertThat(builder, instanceOf(FloatColumnIndexBuilder.class));
        assertNull(builder.build());
        Operators.FloatColumn col = floatColumn("test_col");

        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(type, -4.2f, -4.1f));
        builder.add(sb.stats(type, -11.7f, 7.0f, null));
        builder.add(sb.stats(type, 2.2f, 2.2f, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 1.9f, 2.32f));
        builder.add(sb.stats(type, -21.0f, 8.1f));
        assertEquals(6, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        ColumnIndex columnIndex = builder.build();
        assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
        assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
        assertCorrectValues(columnIndex.getMaxValues(), -4.1f, 7.0f, 2.2f, null, 2.32f, 8.1f);
        assertCorrectValues(columnIndex.getMinValues(), -4.2f, -11.7f, 2.2f, null, 1.9f, -21.0f);
        assertCorrectFiltering(columnIndex, eq(col, 0.0f), 1, 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
        assertCorrectFiltering(columnIndex, notEq(col, 2.2f), 0, 1, 2, 3, 4, 5);
        assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, gt(col, 2.2f), 1, 4, 5);
        assertCorrectFiltering(columnIndex, gtEq(col, 2.2f), 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, lt(col, 0.0f), 0, 1, 5);
        assertCorrectFiltering(columnIndex, ltEq(col, 1.9f), 0, 1, 4, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, FloatIsInteger.class), 1, 4, 5);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, FloatIsInteger.class)), 0, 1, 2, 3, 4, 5);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, -532.3f, -345.2f, null, null));
        builder.add(sb.stats(type, -300.6f, -234.7f, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, -234.6f, 2.99999f));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 3.0f, 42.83f));
        builder.add(sb.stats(type, null, null));
        assertEquals(9, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
        assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
        assertCorrectValues(columnIndex.getMaxValues(), null, -345.2f, -234.7f, null, null, 2.99999f, null, 42.83f, null);
        assertCorrectValues(columnIndex.getMinValues(), null, -532.3f, -300.6f, null, null, -234.6f, null, 3.0f, null);
        assertCorrectFiltering(columnIndex, eq(col, 0.0f), 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 6, 8);
        assertCorrectFiltering(columnIndex, notEq(col, 2.2f), 0, 1, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 5, 7);
        assertCorrectFiltering(columnIndex, gt(col, 2.2f), 5, 7);
        assertCorrectFiltering(columnIndex, gtEq(col, -234.7f), 2, 5, 7);
        assertCorrectFiltering(columnIndex, lt(col, -234.6f), 1, 2);
        assertCorrectFiltering(columnIndex, ltEq(col, -234.6f), 1, 2, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, FloatIsInteger.class), 1, 2, 5, 7);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, FloatIsInteger.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null, null, null, null));
        builder.add(sb.stats(type, 532.3f, 345.2f));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 234.7f, 234.6f, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 234.6f, -2.99999f));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, -3.0f, -42.83f));
        assertEquals(9, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
        assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
        assertCorrectValues(columnIndex.getMaxValues(), null, 532.3f, null, 234.7f, null, 234.6f, null, null, -3.0f);
        assertCorrectValues(columnIndex.getMinValues(), null, 345.2f, null, 234.6f, null, -2.99999f, null, null, -42.83f);
        assertCorrectFiltering(columnIndex, eq(col, 234.65f), 3);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, 2.2f), 0, 1, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 5, 8);
        assertCorrectFiltering(columnIndex, gt(col, 2.2f), 1, 3, 5);
        assertCorrectFiltering(columnIndex, gtEq(col, 2.2f), 1, 3, 5);
        assertCorrectFiltering(columnIndex, lt(col, 0.0f), 5, 8);
        assertCorrectFiltering(columnIndex, ltEq(col, 0.0f), 5, 8);
        assertCorrectFiltering(columnIndex, userDefined(col, FloatIsInteger.class), 1, 5, 8);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, FloatIsInteger.class)), 0, 1, 2, 3, 4, 5, 6, 7, 8);
    }

    @Test
    public void testBuildFloatZeroNaN()
    {
        PrimitiveType type = Types.required(FLOAT).named("test_float");
        ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(type, -1.0f, -0.0f));
        builder.add(sb.stats(type, 0.0f, 1.0f));
        builder.add(sb.stats(type, 1.0f, 100.0f));
        ColumnIndex columnIndex = builder.build();
        assertCorrectValues(columnIndex.getMinValues(), -1.0f, -0.0f, 1.0f);
        assertCorrectValues(columnIndex.getMaxValues(), 0.0f, 1.0f, 100.0f);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        builder.add(sb.stats(type, -1.0f, -0.0f));
        builder.add(sb.stats(type, 0.0f, Float.NaN));
        builder.add(sb.stats(type, 1.0f, 100.0f));
        assertNull(builder.build());
    }

    @Test
    public void testStaticBuildFloat()
    {
        ColumnIndex columnIndex = ColumnIndexBuilder.build(
                Types.required(FLOAT).named("test_float"),
                BoundaryOrder.ASCENDING,
                asList(true, true, true, false, false, false),
                asList(9L, 8L, 7L, 6L, 0L, 0L),
                toBBList(null, null, null, -3.0f, -2.0f, 0.1f),
                toBBList(null, null, null, -2.0f, 0.0f, 6.0f));
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 9, 8, 7, 6, 0, 0);
        assertCorrectNullPages(columnIndex, true, true, true, false, false, false);
        assertCorrectValues(columnIndex.getMaxValues(), null, null, null, -2.0f, 0.0f, 6.0f);
        assertCorrectValues(columnIndex.getMinValues(), null, null, null, -3.0f, -2.0f, 0.1f);
    }

    @Test
    public void testBuildInt32()
    {
        PrimitiveType type = Types.required(INT32).named("test_int32");
        ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        //assertThat(builder, instanceOf(IntColumnIndexBuilder.class));
        assertNull(builder.build());
        Operators.IntColumn col = intColumn("test_col");

        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(type, -4, 10));
        builder.add(sb.stats(type, -11, 7, null));
        builder.add(sb.stats(type, 2, 2, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 1, 2));
        builder.add(sb.stats(type, -21, 8));
        assertEquals(6, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        ColumnIndex columnIndex = builder.build();
        assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
        assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
        assertCorrectValues(columnIndex.getMaxValues(), 10, 7, 2, null, 2, 8);
        assertCorrectValues(columnIndex.getMinValues(), -4, -11, 2, null, 1, -21);
        assertCorrectFiltering(columnIndex, eq(col, 2), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
        assertCorrectFiltering(columnIndex, notEq(col, 2), 0, 1, 2, 3, 4, 5);
        assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, gt(col, 2), 0, 1, 5);
        assertCorrectFiltering(columnIndex, gtEq(col, 2), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, lt(col, 2), 0, 1, 4, 5);
        assertCorrectFiltering(columnIndex, ltEq(col, 2), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisibleWith3.class), 0, 1, 5);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, IntegerIsDivisibleWith3.class)), 0, 1, 2, 3, 4, 5);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, -532, -345, null, null));
        builder.add(sb.stats(type, -500, -42, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, -42, 2));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 3, 42));
        builder.add(sb.stats(type, null, null));
        assertEquals(9, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
        assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
        assertCorrectValues(columnIndex.getMaxValues(), null, -345, -42, null, null, 2, null, 42, null);
        assertCorrectValues(columnIndex.getMinValues(), null, -532, -500, null, null, -42, null, 3, null);
        assertCorrectFiltering(columnIndex, eq(col, 2), 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 6, 8);
        assertCorrectFiltering(columnIndex, notEq(col, 2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 5, 7);
        assertCorrectFiltering(columnIndex, gt(col, 2), 7);
        assertCorrectFiltering(columnIndex, gtEq(col, 2), 5, 7);
        assertCorrectFiltering(columnIndex, lt(col, 2), 1, 2, 5);
        assertCorrectFiltering(columnIndex, ltEq(col, 2), 1, 2, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisibleWith3.class), 1, 2, 5, 7);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, IntegerIsDivisibleWith3.class)), 0, 1, 2, 3, 4, 5, 6, 7,
                8);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null, null, null, null));
        builder.add(sb.stats(type, 532, 345));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 234, 42, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 42, -2));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, -3, -42));
        assertEquals(9, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
        assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
        assertCorrectValues(columnIndex.getMaxValues(), null, 532, null, 234, null, 42, null, null, -3);
        assertCorrectValues(columnIndex.getMinValues(), null, 345, null, 42, null, -2, null, null, -42);
        assertCorrectFiltering(columnIndex, eq(col, 2), 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, 2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 5, 8);
        assertCorrectFiltering(columnIndex, gt(col, 2), 1, 3, 5);
        assertCorrectFiltering(columnIndex, gtEq(col, 2), 1, 3, 5);
        assertCorrectFiltering(columnIndex, lt(col, 2), 5, 8);
        assertCorrectFiltering(columnIndex, ltEq(col, 2), 5, 8);
        assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisibleWith3.class), 1, 3, 5, 8);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, IntegerIsDivisibleWith3.class)), 0, 1, 2, 3, 4, 5, 6, 7,
                8);
    }

    @Test
    public void testStaticBuildInt32()
    {
        ColumnIndex columnIndex = ColumnIndexBuilder.build(
                Types.required(INT32).named("test_int32"),
                BoundaryOrder.DESCENDING,
                asList(false, false, false, true, true, true),
                asList(0L, 10L, 0L, 3L, 5L, 7L),
                toBBList(10, 8, 6, null, null, null),
                toBBList(9, 7, 5, null, null, null));
        assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 0, 10, 0, 3, 5, 7);
        assertCorrectNullPages(columnIndex, false, false, false, true, true, true);
        assertCorrectValues(columnIndex.getMaxValues(), 9, 7, 5, null, null, null);
        assertCorrectValues(columnIndex.getMinValues(), 10, 8, 6, null, null, null);
    }

    @Test
    public void testBuildUInt8()
    {
        PrimitiveType type = Types.required(INT32).as(UINT_8).named("test_uint8");
        ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        //assertThat(builder, instanceOf(IntColumnIndexBuilder.class));
        assertNull(builder.build());
        Operators.IntColumn col = intColumn("test_col");

        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(type, 4, 10));
        builder.add(sb.stats(type, 11, 17, null));
        builder.add(sb.stats(type, 2, 2, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 1, 0xFF));
        builder.add(sb.stats(type, 0xEF, 0xFA));
        assertEquals(6, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        ColumnIndex columnIndex = builder.build();
        assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 0, 1, 2, 3, 0, 0);
        assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
        assertCorrectValues(columnIndex.getMaxValues(), 10, 17, 2, null, 0xFF, 0xFA);
        assertCorrectValues(columnIndex.getMinValues(), 4, 11, 2, null, 1, 0xEF);
        assertCorrectFiltering(columnIndex, eq(col, 2), 2, 4);
        assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
        assertCorrectFiltering(columnIndex, notEq(col, 2), 0, 1, 2, 3, 4, 5);
        assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, gt(col, 2), 0, 1, 4, 5);
        assertCorrectFiltering(columnIndex, gtEq(col, 2), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, lt(col, 0xEF), 0, 1, 2, 4);
        assertCorrectFiltering(columnIndex, ltEq(col, 0xEF), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisibleWith3.class), 0, 1, 4, 5);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, IntegerIsDivisibleWith3.class)), 0, 1, 2, 3, 4, 5);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 0, 0, null, null));
        builder.add(sb.stats(type, 0, 42, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 42, 0xEE));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 0xEF, 0xFF));
        builder.add(sb.stats(type, null, null));
        assertEquals(9, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
        assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
        assertCorrectValues(columnIndex.getMaxValues(), null, 0, 42, null, null, 0xEE, null, 0xFF, null);
        assertCorrectValues(columnIndex.getMinValues(), null, 0, 0, null, null, 42, null, 0xEF, null);
        assertCorrectFiltering(columnIndex, eq(col, 2), 2);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 6, 8);
        assertCorrectFiltering(columnIndex, notEq(col, 2), 0, 1, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 5, 7);
        assertCorrectFiltering(columnIndex, gt(col, 0xEE), 7);
        assertCorrectFiltering(columnIndex, gtEq(col, 0xEE), 5, 7);
        assertCorrectFiltering(columnIndex, lt(col, 42), 1, 2);
        assertCorrectFiltering(columnIndex, ltEq(col, 42), 1, 2, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisibleWith3.class), 1, 2, 5, 7);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, IntegerIsDivisibleWith3.class)), 0, 1, 2, 3, 4, 5, 6, 7,
                8);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null, null, null, null));
        builder.add(sb.stats(type, 0xFF, 0xFF));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 0xEF, 0xEA, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 0xEE, 42));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 41, 0));
        assertEquals(9, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
        assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
        assertCorrectValues(columnIndex.getMaxValues(), null, 0xFF, null, 0xEF, null, 0xEE, null, null, 41);
        assertCorrectValues(columnIndex.getMinValues(), null, 0xFF, null, 0xEA, null, 42, null, null, 0);
        assertCorrectFiltering(columnIndex, eq(col, 0xAB), 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, 0xFF), 0, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 5, 8);
        assertCorrectFiltering(columnIndex, gt(col, 0xFF));
        assertCorrectFiltering(columnIndex, gtEq(col, 0xFF), 1);
        assertCorrectFiltering(columnIndex, lt(col, 42), 8);
        assertCorrectFiltering(columnIndex, ltEq(col, 42), 5, 8);
        assertCorrectFiltering(columnIndex, userDefined(col, IntegerIsDivisibleWith3.class), 1, 3, 5, 8);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, IntegerIsDivisibleWith3.class)), 0, 2, 3, 4, 5, 6, 7,
                8);
    }

    @Test
    public void testBuildInt64()
    {
        PrimitiveType type = Types.required(INT64).named("test_int64");
        ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        //assertThat(builder, instanceOf(LongColumnIndexBuilder.class));
        assertNull(builder.build());
        Operators.LongColumn col = longColumn("test_col");

        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(type, -4L, 10L));
        builder.add(sb.stats(type, -11L, 7L, null));
        builder.add(sb.stats(type, 2L, 2L, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 1L, 2L));
        builder.add(sb.stats(type, -21L, 8L));
        assertEquals(6, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        ColumnIndex columnIndex = builder.build();
        assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 0L, 1L, 2L, 3L, 0L, 0L);
        assertCorrectNullPages(columnIndex, false, false, false, true, false, false);
        assertCorrectValues(columnIndex.getMaxValues(), 10L, 7L, 2L, null, 2L, 8L);
        assertCorrectValues(columnIndex.getMinValues(), -4L, -11L, 2L, null, 1L, -21L);
        assertCorrectFiltering(columnIndex, eq(col, 0L), 0, 1, 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 1, 2, 3);
        assertCorrectFiltering(columnIndex, notEq(col, 0L), 0, 1, 2, 3, 4, 5);
        assertCorrectFiltering(columnIndex, notEq(col, null), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, gt(col, 2L), 0, 1, 5);
        assertCorrectFiltering(columnIndex, gtEq(col, 2L), 0, 1, 2, 4, 5);
        assertCorrectFiltering(columnIndex, lt(col, -21L));
        assertCorrectFiltering(columnIndex, ltEq(col, -21L), 5);
        assertCorrectFiltering(columnIndex, userDefined(col, LongIsDivisibleWith3.class), 0, 1, 5);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, LongIsDivisibleWith3.class)), 0, 1, 2, 3, 4, 5);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, -532L, -345L, null, null));
        builder.add(sb.stats(type, -234L, -42L, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, -42L, 2L));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, -3L, 42L));
        builder.add(sb.stats(type, null, null));
        assertEquals(9, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 2, 2, 1, 2, 3, 0, 2, 0, 2);
        assertCorrectNullPages(columnIndex, true, false, false, true, true, false, true, false, true);
        assertCorrectValues(columnIndex.getMaxValues(), null, -345L, -42L, null, null, 2L, null, 42L, null);
        assertCorrectValues(columnIndex.getMinValues(), null, -532L, -234L, null, null, -42L, null, -3L, null);
        assertCorrectFiltering(columnIndex, eq(col, -42L), 2, 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 1, 2, 3, 4, 6, 8);
        assertCorrectFiltering(columnIndex, notEq(col, -42L), 0, 1, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 2, 5, 7);
        assertCorrectFiltering(columnIndex, gt(col, 2L), 7);
        assertCorrectFiltering(columnIndex, gtEq(col, 2L), 5, 7);
        assertCorrectFiltering(columnIndex, lt(col, -42L), 1, 2);
        assertCorrectFiltering(columnIndex, ltEq(col, -42L), 1, 2, 5);
        assertCorrectFiltering(columnIndex, userDefined(col, LongIsDivisibleWith3.class), 1, 2, 5, 7);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, LongIsDivisibleWith3.class)), 0, 1, 2, 3, 4, 5, 6, 7,
                8);

        builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
        sb = new StatsBuilder();
        builder.add(sb.stats(type, null, null, null, null, null));
        builder.add(sb.stats(type, 532L, 345L));
        builder.add(sb.stats(type, null, null, null));
        builder.add(sb.stats(type, 234L, 42L, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, 42L, -2L));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, null, null));
        builder.add(sb.stats(type, -3L, -42L));
        assertEquals(9, builder.getPageCount());
        assertEquals(sb.getMinMaxSize(), builder.getMinMaxSize());
        columnIndex = builder.build();
        assertEquals(BoundaryOrder.DESCENDING, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 5, 0, 3, 1, 2, 0, 2, 2, 0);
        assertCorrectNullPages(columnIndex, true, false, true, false, true, false, true, true, false);
        assertCorrectValues(columnIndex.getMaxValues(), null, 532L, null, 234L, null, 42L, null, null, -3L);
        assertCorrectValues(columnIndex.getMinValues(), null, 345L, null, 42L, null, -2L, null, null, -42L);
        assertCorrectFiltering(columnIndex, eq(col, 0L), 5);
        assertCorrectFiltering(columnIndex, eq(col, null), 0, 2, 3, 4, 6, 7);
        assertCorrectFiltering(columnIndex, notEq(col, 0L), 0, 1, 2, 3, 4, 5, 6, 7, 8);
        assertCorrectFiltering(columnIndex, notEq(col, null), 1, 3, 5, 8);
        assertCorrectFiltering(columnIndex, gt(col, 2L), 1, 3, 5);
        assertCorrectFiltering(columnIndex, gtEq(col, 2L), 1, 3, 5);
        assertCorrectFiltering(columnIndex, lt(col, -42L));
        assertCorrectFiltering(columnIndex, ltEq(col, -42L), 8);
        assertCorrectFiltering(columnIndex, userDefined(col, LongIsDivisibleWith3.class), 1, 3, 5, 8);
        assertCorrectFiltering(columnIndex, invert(userDefined(col, LongIsDivisibleWith3.class)), 0, 1, 2, 3, 4, 5, 6, 7,
                8);
    }

    @Test
    public void testStaticBuildInt64()
    {
        ColumnIndex columnIndex = ColumnIndexBuilder.build(
                Types.required(INT64).named("test_int64"),
                BoundaryOrder.UNORDERED,
                asList(true, false, true, false, true, false),
                asList(1L, 2L, 3L, 4L, 5L, 6L),
                toBBList(null, 2L, null, 4L, null, 9L),
                toBBList(null, 3L, null, 15L, null, 10L));
        assertEquals(BoundaryOrder.UNORDERED, columnIndex.getBoundaryOrder());
        assertCorrectNullCounts(columnIndex, 1, 2, 3, 4, 5, 6);
        assertCorrectNullPages(columnIndex, true, false, true, false, true, false);
        assertCorrectValues(columnIndex.getMaxValues(), null, 3L, null, 15L, null, 10L);
        assertCorrectValues(columnIndex.getMinValues(), null, 2L, null, 4L, null, 9L);
    }

    @Test
    public void testNoOpBuilder()
    {
        ColumnIndexBuilder builder = ColumnIndexBuilder.getNoOpBuilder();
        StatsBuilder sb = new StatsBuilder();
        builder.add(sb.stats(Types.required(BINARY).as(UTF8).named("test_binary_utf8"), stringBinary("Jeltz"),
                stringBinary("Slartibartfast"), null, null));
        builder.add(sb.stats(Types.required(BOOLEAN).named("test_boolean"), true, true, null, null));
        builder.add(sb.stats(Types.required(DOUBLE).named("test_double"), null, null, null));
        builder.add(sb.stats(Types.required(INT32).named("test_int32"), null, null));
        builder.add(sb.stats(Types.required(INT64).named("test_int64"), -234L, -42L, null));
        assertEquals(0, builder.getPageCount());
        assertEquals(0, builder.getMinMaxSize());
        assertNull(builder.build());
    }

    private static List<ByteBuffer> toBBList(Binary... values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.length);
        for (Binary value : values) {
            if (value == null) {
                buffers.add(ByteBuffer.allocate(0));
            }
            else {
                buffers.add(value.toByteBuffer());
            }
        }
        return buffers;
    }

    private static List<ByteBuffer> toBBList(Boolean... values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.length);
        for (Boolean value : values) {
            if (value == null) {
                buffers.add(ByteBuffer.allocate(0));
            }
            else {
                buffers.add(ByteBuffer.wrap(BytesUtils.booleanToBytes(value)));
            }
        }
        return buffers;
    }

    private static List<ByteBuffer> toBBList(Double... values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.length);
        for (Double value : values) {
            if (value == null) {
                buffers.add(ByteBuffer.allocate(0));
            }
            else {
                buffers.add(ByteBuffer.wrap(BytesUtils.longToBytes(Double.doubleToLongBits(value))));
            }
        }
        return buffers;
    }

    private static List<ByteBuffer> toBBList(Float... values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.length);
        for (Float value : values) {
            if (value == null) {
                buffers.add(ByteBuffer.allocate(0));
            }
            else {
                buffers.add(ByteBuffer.wrap(BytesUtils.intToBytes(Float.floatToIntBits(value))));
            }
        }
        return buffers;
    }

    private static List<ByteBuffer> toBBList(Integer... values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.length);
        for (Integer value : values) {
            if (value == null) {
                buffers.add(ByteBuffer.allocate(0));
            }
            else {
                buffers.add(ByteBuffer.wrap(BytesUtils.intToBytes(value)));
            }
        }
        return buffers;
    }

    private static List<ByteBuffer> toBBList(Long... values)
    {
        List<ByteBuffer> buffers = new ArrayList<>(values.length);
        for (Long value : values) {
            if (value == null) {
                buffers.add(ByteBuffer.allocate(0));
            }
            else {
                buffers.add(ByteBuffer.wrap(BytesUtils.longToBytes(value)));
            }
        }
        return buffers;
    }

    private static Binary decimalBinary(String num)
    {
        return Binary.fromConstantByteArray(new BigDecimal(num).unscaledValue().toByteArray());
    }

    private static Binary stringBinary(String str)
    {
        return Binary.fromString(str);
    }

    private static void assertCorrectValues(List<ByteBuffer> values, Binary... expectedValues)
    {
        assertEquals(expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; ++i) {
            Binary expectedValue = expectedValues[i];
            ByteBuffer value = values.get(i);
            if (expectedValue == null) {
                assertFalse(value.hasRemaining(), "The byte buffer should be empty for null pages");
            }
            else {
                assertArrayEquals("Invalid value for page " + i, expectedValue.getBytesUnsafe(), value.array());
            }
        }
    }

    private static void assertCorrectValues(List<ByteBuffer> values, Boolean... expectedValues)
    {
        assertEquals(expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; ++i) {
            Boolean expectedValue = expectedValues[i];
            ByteBuffer value = values.get(i);
            if (expectedValue == null) {
                assertFalse(value.hasRemaining(), "The byte buffer should be empty for null pages");
            }
            else {
                assertEquals(1, value.remaining(), "The byte buffer should be 1 byte long for boolean");
                assertEquals(expectedValue.booleanValue(), value.get(0) != 0, "Invalid value for page " + i);
            }
        }
    }

    private static void assertCorrectValues(List<ByteBuffer> values, Double... expectedValues)
    {
        assertEquals(expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; ++i) {
            Double expectedValue = expectedValues[i];
            ByteBuffer value = values.get(i);
            if (expectedValue == null) {
                assertFalse(value.hasRemaining(), "The byte buffer should be empty for null pages");
            }
            else {
                assertEquals(8, value.remaining(), "The byte buffer should be 8 bytes long for double");
                assertTrue(Double.compare(expectedValue.doubleValue(), value.getDouble(0)) == 0, "Invalid value for page " + i);
            }
        }
    }

    private static void assertCorrectValues(List<ByteBuffer> values, Float... expectedValues)
    {
        assertEquals(expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; ++i) {
            Float expectedValue = expectedValues[i];
            ByteBuffer value = values.get(i);
            if (expectedValue == null) {
                assertFalse(value.hasRemaining(), "The byte buffer should be empty for null pages");
            }
            else {
                assertEquals(4, value.remaining(), "The byte buffer should be 4 bytes long for double");
                assertTrue(Float.compare(expectedValue.floatValue(), value.getFloat(0)) == 0, "Invalid value for page " + i);
            }
        }
    }

    private static void assertCorrectValues(List<ByteBuffer> values, Integer... expectedValues)
    {
        assertEquals(expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; ++i) {
            Integer expectedValue = expectedValues[i];
            ByteBuffer value = values.get(i);
            if (expectedValue == null) {
                assertFalse(value.hasRemaining(), "The byte buffer should be empty for null pages");
            }
            else {
                assertEquals(4, value.remaining(), "The byte buffer should be 4 bytes long for int32");
                assertEquals(expectedValue.intValue(), value.getInt(0), "Invalid value for page " + i);
            }
        }
    }

    private static void assertCorrectValues(List<ByteBuffer> values, Long... expectedValues)
    {
        assertEquals(expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; ++i) {
            Long expectedValue = expectedValues[i];
            ByteBuffer value = values.get(i);
            if (expectedValue == null) {
                assertFalse(value.hasRemaining(), "The byte buffer should be empty for null pages");
            }
            else {
                assertEquals(8, value.remaining(), "The byte buffer should be 8 bytes long for int64");
                assertEquals(expectedValue.intValue(), value.getLong(0), "Invalid value for page " + i);
            }
        }
    }

    private static void assertCorrectNullCounts(ColumnIndex columnIndex, long... expectedNullCounts)
    {
        List<Long> nullCounts = columnIndex.getNullCounts();
        assertEquals(expectedNullCounts.length, nullCounts.size());
        for (int i = 0; i < expectedNullCounts.length; ++i) {
            assertEquals(expectedNullCounts[i], nullCounts.get(i).longValue(), "Invalid null count at page " + i);
        }
    }

    private static void assertCorrectNullPages(ColumnIndex columnIndex, boolean... expectedNullPages)
    {
        List<Boolean> nullPages = columnIndex.getNullPages();
        assertEquals(expectedNullPages.length, nullPages.size());
        for (int i = 0; i < expectedNullPages.length; ++i) {
            assertEquals(expectedNullPages[i], nullPages.get(i).booleanValue(), "Invalid null pages at page " + i);
        }
    }

    private static class StatsBuilder
    {
        private long minMaxSize;

        Statistics<?> stats(PrimitiveType type, Object... values)
        {
            Statistics<?> stats = Statistics.createStats(type);
            for (Object value : values) {
                if (value == null) {
                    stats.incrementNumNulls();
                    continue;
                }
                switch (type.getPrimitiveTypeName()) {
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                    case INT96:
                        stats.updateStats((Binary) value);
                        break;
                    case BOOLEAN:
                        stats.updateStats((boolean) value);
                        break;
                    case DOUBLE:
                        stats.updateStats((double) value);
                        break;
                    case FLOAT:
                        stats.updateStats((float) value);
                        break;
                    case INT32:
                        stats.updateStats((int) value);
                        break;
                    case INT64:
                        stats.updateStats((long) value);
                        break;
                    default:
                        fail("Unsupported value type for stats: " + value.getClass());
                }
            }
            if (stats.hasNonNullValue()) {
                minMaxSize += stats.getMinBytes().length;
                minMaxSize += stats.getMaxBytes().length;
            }
            return stats;
        }

        long getMinMaxSize()
        {
            return minMaxSize;
        }
    }

    private static void assertCorrectFiltering(ColumnIndex ci, FilterPredicate predicate, int... expectedIndexes)
    {
        checkEquals(predicate.accept(ci), expectedIndexes);
    }

    static void checkEquals(PrimitiveIterator.OfInt actualIt, int... expectedValues)
    {
        IntList actualList = new IntArrayList();
        actualIt.forEachRemaining((int value) -> actualList.add(value));
        int[] actualValues = actualList.toIntArray();
        assertArrayEquals("ExpectedValues: " + Arrays.toString(expectedValues) + " ActualValues: " + Arrays.toString(actualValues),
                expectedValues, actualValues);
    }
}
