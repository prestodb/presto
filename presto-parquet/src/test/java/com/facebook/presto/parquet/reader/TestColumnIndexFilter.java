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

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.internal.filter2.columnindex.RowRanges;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.LongStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.filter2.predicate.LogicalInverter.invert;
import static org.apache.parquet.internal.column.columnindex.BoundaryOrder.ASCENDING;
import static org.apache.parquet.internal.column.columnindex.BoundaryOrder.DESCENDING;
import static org.apache.parquet.internal.column.columnindex.BoundaryOrder.UNORDERED;
import static org.apache.parquet.internal.filter2.columnindex.ColumnIndexFilter.calculateRowRanges;
import static org.apache.parquet.io.api.Binary.fromString;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

/**
 * Unit tests of {@link ColumnIndexFilter}
 */
public class TestColumnIndexFilter
{
    private static class CIBuilder
    {
        private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[0]);
        private final PrimitiveType type;
        private final BoundaryOrder order;
        private List<Boolean> nullPages = new ArrayList<>();
        private List<Long> nullCounts = new ArrayList<>();
        private List<ByteBuffer> minValues = new ArrayList<>();
        private List<ByteBuffer> maxValues = new ArrayList<>();

        CIBuilder(PrimitiveType type, BoundaryOrder order)
        {
            this.type = type;
            this.order = order;
        }

        CIBuilder addNullPage(long nullCount)
        {
            nullPages.add(true);
            nullCounts.add(nullCount);
            minValues.add(EMPTY);
            maxValues.add(EMPTY);
            return this;
        }

        CIBuilder addPage(long nullCount, int min, int max)
        {
            nullPages.add(false);
            nullCounts.add(nullCount);
            minValues.add(ByteBuffer.wrap(BytesUtils.intToBytes(min)));
            maxValues.add(ByteBuffer.wrap(BytesUtils.intToBytes(max)));
            return this;
        }

        CIBuilder addPage(long nullCount, String min, String max)
        {
            nullPages.add(false);
            nullCounts.add(nullCount);
            minValues.add(ByteBuffer.wrap(min.getBytes(UTF_8)));
            maxValues.add(ByteBuffer.wrap(max.getBytes(UTF_8)));
            return this;
        }

        CIBuilder addPage(long nullCount, double min, double max)
        {
            nullPages.add(false);
            nullCounts.add(nullCount);
            minValues.add(ByteBuffer.wrap(BytesUtils.longToBytes(Double.doubleToLongBits(min))));
            maxValues.add(ByteBuffer.wrap(BytesUtils.longToBytes(Double.doubleToLongBits(max))));
            return this;
        }

        ColumnIndex build()
        {
            return ColumnIndexBuilder.build(type, order, nullPages, nullCounts, minValues, maxValues);
        }
    }

    private static class OIBuilder
    {
        private final OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();

        OIBuilder addPage(long rowCount)
        {
            builder.add(1234, rowCount);
            return this;
        }

        OffsetIndex build()
        {
            return builder.build();
        }
    }

    public static class AnyInt
            extends UserDefinedPredicate<Integer>
    {
        @Override
        public boolean keep(Integer value)
        {
            return true;
        }

        @Override
        public boolean canDrop(Statistics<Integer> statistics)
        {
            return false;
        }

        @Override
        public boolean inverseCanDrop(Statistics<Integer> statistics)
        {
            return true;
        }
    }

    /**
     * <pre>
     * row     column1        column2        column3        column4        column5
     *                                                 (no column index)
     *      ------0------  ------0------  ------0------  ------0------  ------0------
     * 0.   1              Zulu           2.03                          null
     *      ------1------  ------1------  ------1------  ------1------  ------1------
     * 1.   2              Yankee         4.67                          null
     * 2.   3              Xray           3.42                          null
     * 3.   4              Whiskey        8.71                          null
     *                     ------2------                 ------2------
     * 4.   5              Victor         0.56                          null
     * 5.   6              Uniform        4.30                          null
     *                                    ------2------  ------3------
     * 6.   null           null           null                          null
     *      ------2------                                ------4------
     * 7.   7              Tango          3.50                          null
     *                     ------3------
     * 8.   7              null           3.14                          null
     *      ------3------
     * 9.   7              null           null                          null
     *                                    ------3------
     * 10.  null           null           9.99                          null
     *                     ------4------
     * 11.  8              Sierra         8.78                          null
     *                                                   ------5------
     * 12.  9              Romeo          9.56                          null
     * 13.  10             Quebec         2.71                          null
     *      ------4------
     * 14.  11             Papa           5.71                          null
     * 15.  12             Oscar          4.09                          null
     *                     ------5------  ------4------  ------6------
     * 16.  13             November       null                          null
     * 17.  14             Mike           null                          null
     * 18.  15             Lima           0.36                          null
     * 19.  16             Kilo           2.94                          null
     * 20.  17             Juliett        4.23                          null
     *      ------5------  ------6------                 ------7------
     * 21.  18             India          null                          null
     * 22.  19             Hotel          5.32                          null
     *                                    ------5------
     * 23.  20             Golf           4.17                          null
     * 24.  21             Foxtrot        7.92                          null
     * 25.  22             Echo           7.95                          null
     *                                   ------6------
     * 26.  23             Delta          null                          null
     *      ------6------
     * 27.  24             Charlie        null                          null
     *                                                   ------8------
     * 28.  25             Bravo          null                          null
     *                     ------7------
     * 29.  26             Alfa           null                          null
     * </pre>
     */
    private static final long TOTAL_ROW_COUNT = 30;
    private static final ColumnIndex COLUMN1_CI = new CIBuilder(optional(INT32).named("column1"), ASCENDING)
            .addPage(0, 1, 1)
            .addPage(1, 2, 6)
            .addPage(0, 7, 7)
            .addPage(1, 7, 10)
            .addPage(0, 11, 17)
            .addPage(0, 18, 23)
            .addPage(0, 24, 26)
            .build();
    private static final OffsetIndex COLUMN1_OI = new OIBuilder()
            .addPage(1)
            .addPage(6)
            .addPage(2)
            .addPage(5)
            .addPage(7)
            .addPage(6)
            .addPage(3)
            .build();
    private static final ColumnIndex COLUMN2_CI = new CIBuilder(optional(BINARY).as(stringType()).named("column2"), DESCENDING)
            .addPage(0, "Zulu", "Zulu")
            .addPage(0, "Whiskey", "Yankee")
            .addPage(1, "Tango", "Victor")
            .addNullPage(3)
            .addPage(0, "Oscar", "Sierra")
            .addPage(0, "Juliett", "November")
            .addPage(0, "Bravo", "India")
            .addPage(0, "Alfa", "Alfa")
            .build();
    private static final OffsetIndex COLUMN2_OI = new OIBuilder()
            .addPage(1)
            .addPage(3)
            .addPage(4)
            .addPage(3)
            .addPage(5)
            .addPage(5)
            .addPage(8)
            .addPage(1)
            .build();
    private static final ColumnIndex COLUMN3_CI = new CIBuilder(optional(DOUBLE).named("column3"), UNORDERED)
            .addPage(0, 2.03, 2.03)
            .addPage(0, 0.56, 8.71)
            .addPage(2, 3.14, 3.50)
            .addPage(0, 2.71, 9.99)
            .addPage(3, 0.36, 5.32)
            .addPage(0, 4.17, 7.95)
            .addNullPage(4)
            .build();
    private static final OffsetIndex COLUMN3_OI = new OIBuilder()
            .addPage(1)
            .addPage(5)
            .addPage(4)
            .addPage(6)
            .addPage(7)
            .addPage(3)
            .addPage(4)
            .build();
    private static final ColumnIndex COLUMN4_CI = null;
    private static final OffsetIndex COLUMN4_OI = new OIBuilder()
            .addPage(1)
            .addPage(3)
            .addPage(2)
            .addPage(1)
            .addPage(5)
            .addPage(4)
            .addPage(5)
            .addPage(7)
            .addPage(2)
            .build();
    private static final ColumnIndex COLUMN5_CI = new CIBuilder(optional(INT64).named("column5"), ASCENDING)
            .addNullPage(1)
            .addNullPage(29)
            .build();
    private static final OffsetIndex COLUMN5_OI = new OIBuilder()
            .addPage(1)
            .addPage(29)
            .build();
    private static final ColumnIndexStore STORE = new ColumnIndexStore()
    {
        @Override
        public ColumnIndex getColumnIndex(ColumnPath column)
        {
            switch (column.toDotString()) {
                case "column1":
                    return COLUMN1_CI;
                case "column2":
                    return COLUMN2_CI;
                case "column3":
                    return COLUMN3_CI;
                case "column4":
                    return COLUMN4_CI;
                case "column5":
                    return COLUMN5_CI;
                default:
                    return null;
            }
        }

        @Override
        public OffsetIndex getOffsetIndex(ColumnPath column)
        {
            switch (column.toDotString()) {
                case "column1":
                    return COLUMN1_OI;
                case "column2":
                    return COLUMN2_OI;
                case "column3":
                    return COLUMN3_OI;
                case "column4":
                    return COLUMN4_OI;
                case "column5":
                    return COLUMN5_OI;
                default:
                    throw new MissingOffsetIndexException(column);
            }
        }
    };

    private static Set<ColumnPath> paths(String... columns)
    {
        Set<ColumnPath> paths = new HashSet<>();
        for (String column : columns) {
            paths.add(ColumnPath.fromDotString(column));
        }
        return paths;
    }

    private static void assertAllRows(RowRanges ranges, long rowCount)
    {
        LongList actualList = new LongArrayList();
        ranges.iterator().forEachRemaining((long value) -> actualList.add(value));
        LongList expectedList = new LongArrayList();
        LongStream.range(0, rowCount).forEach(expectedList::add);
        assertArrayEquals(expectedList + " != " + actualList, expectedList.toLongArray(), actualList.toLongArray());
    }

    private static void assertRows(RowRanges ranges, long... expectedRows)
    {
        LongList actualList = new LongArrayList();
        ranges.iterator().forEachRemaining((long value) -> actualList.add(value));
        assertArrayEquals(Arrays.toString(expectedRows) + " != " + actualList, expectedRows, actualList.toLongArray());
    }

    @Test
    public void testFiltering()
    {
        Set<ColumnPath> paths = paths("column1", "column2", "column3", "column4");

        assertAllRows(
                calculateRowRanges(FilterCompat.get(
                        userDefined(intColumn("column1"), AnyInt.class)), STORE, paths, TOTAL_ROW_COUNT),
                TOTAL_ROW_COUNT);
        assertRows(calculateRowRanges(FilterCompat.get(
                and(
                        and(
                                eq(intColumn("column1"), null),
                                eq(binaryColumn("column2"), null)),
                        and(
                                eq(doubleColumn("column3"), null),
                                eq(booleanColumn("column4"), null)))),
                STORE, paths, TOTAL_ROW_COUNT),
                6, 9);
        assertRows(calculateRowRanges(FilterCompat.get(
                and(
                        and(
                                notEq(intColumn("column1"), null),
                                notEq(binaryColumn("column2"), null)),
                        and(
                                notEq(doubleColumn("column3"), null),
                                notEq(booleanColumn("column4"), null)))),
                STORE, paths, TOTAL_ROW_COUNT),
                0, 1, 2, 3, 4, 5, 6, 7, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25);
        assertRows(calculateRowRanges(FilterCompat.get(
                or(
                        and(
                                lt(intColumn("column1"), 20),
                                gtEq(binaryColumn("column2"), fromString("Quebec"))),
                        and(
                                gt(doubleColumn("column3"), 5.32),
                                ltEq(binaryColumn("column4"), fromString("XYZ"))))),
                STORE, paths, TOTAL_ROW_COUNT),
                0, 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 23, 24, 25);
        assertRows(calculateRowRanges(FilterCompat.get(
                and(
                        and(
                                gtEq(intColumn("column1"), 7),
                                gt(binaryColumn("column2"), fromString("India"))),
                        and(
                                eq(doubleColumn("column3"), null),
                                notEq(binaryColumn("column4"), null)))),
                STORE, paths, TOTAL_ROW_COUNT),
                7, 16, 17, 18, 19, 20);
        assertRows(calculateRowRanges(FilterCompat.get(
                and(
                        or(
                                invert(userDefined(intColumn("column1"), AnyInt.class)),
                                eq(binaryColumn("column2"), fromString("Echo"))),
                        eq(doubleColumn("column3"), 6.0))),
                STORE, paths, TOTAL_ROW_COUNT),
                23, 24, 25);
        assertRows(calculateRowRanges(FilterCompat.get(
                and(
                        and(
                                gtEq(intColumn("column1"), 7),
                                lt(intColumn("column1"), 11)),
                        and(
                                gt(binaryColumn("column2"), fromString("Romeo")),
                                ltEq(binaryColumn("column2"), fromString("Tango"))))),
                STORE, paths, TOTAL_ROW_COUNT),
                7, 11, 12, 13);
    }

    @Test
    public void testFilteringOnMissingColumns()
    {
        Set<ColumnPath> paths = paths("column1", "column2", "column3", "column4");

        // Missing column filter is always true
        assertAllRows(calculateRowRanges(FilterCompat.get(
                notEq(intColumn("missing_column"), 0)),
                STORE, paths, TOTAL_ROW_COUNT),
                TOTAL_ROW_COUNT);
        assertRows(calculateRowRanges(FilterCompat.get(
                and(
                        and(
                                gtEq(intColumn("column1"), 7),
                                lt(intColumn("column1"), 11)),
                        eq(binaryColumn("missing_column"), null))),
                STORE, paths, TOTAL_ROW_COUNT),
                7, 8, 9, 10, 11, 12, 13);

        // Missing column filter is always false
        assertRows(calculateRowRanges(FilterCompat.get(
                or(
                        and(
                                gtEq(intColumn("column1"), 7),
                                lt(intColumn("column1"), 11)),
                        notEq(binaryColumn("missing_column"), null))),
                STORE, paths, TOTAL_ROW_COUNT),
                7, 8, 9, 10, 11, 12, 13);
        assertRows(calculateRowRanges(FilterCompat.get(
                gt(intColumn("missing_column"), 0)),
                STORE, paths, TOTAL_ROW_COUNT));
    }

    @Test
    public void testFilteringWithMissingOffsetIndex()
    {
        Set<ColumnPath> paths = paths("column1", "column2", "column3", "column4", "column_wo_oi");

        assertAllRows(calculateRowRanges(FilterCompat.get(
                and(
                        and(
                                gtEq(intColumn("column1"), 7),
                                lt(intColumn("column1"), 11)),
                        and(
                                gt(binaryColumn("column2"), fromString("Romeo")),
                                ltEq(binaryColumn("column_wo_oi"), fromString("Tango"))))),
                STORE, paths, TOTAL_ROW_COUNT),
                TOTAL_ROW_COUNT);
    }

    @Test
    public void testFilteringWithAllNullPages()
    {
        Set<ColumnPath> paths = paths("column1", "column5");

        assertAllRows(calculateRowRanges(FilterCompat.get(
                notEq(longColumn("column5"), 1234567L)),
                STORE, paths, TOTAL_ROW_COUNT),
                TOTAL_ROW_COUNT);
        assertAllRows(calculateRowRanges(FilterCompat.get(
                or(gtEq(intColumn("column1"), 10),
                        notEq(longColumn("column5"), 1234567L))),
                STORE, paths, TOTAL_ROW_COUNT),
                TOTAL_ROW_COUNT);
        assertRows(calculateRowRanges(FilterCompat.get(
                eq(longColumn("column5"), 1234567L)),
                STORE, paths, TOTAL_ROW_COUNT));
        assertRows(calculateRowRanges(FilterCompat.get(
                and(lt(intColumn("column1"), 20),
                        gtEq(longColumn("column5"), 1234567L))),
                STORE, paths, TOTAL_ROW_COUNT));
    }
}
