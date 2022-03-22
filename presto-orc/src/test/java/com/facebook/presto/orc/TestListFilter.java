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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.PositionalFilter;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.reader.ListFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.INT;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.LIST;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestListFilter
{
    private static final int ROW_COUNT = 1_000;
    private final Random random = new Random(0);

    @Test
    public void testArray()
    {
        Integer[][] data = new Integer[ROW_COUNT][];
        int count = 0;
        for (int i = 0; i < data.length; i++) {
            data[i] = new Integer[3 + random.nextInt(10)];
            for (int j = 0; j < data[i].length; j++) {
                if (count++ % 11 == 0) {
                    data[i][j] = null;
                }
                else {
                    data[i][j] = random.nextInt(100);
                }
            }
        }

        assertPositionalFilter(ImmutableMap.of(
                1, BigintRange.of(0, 50, false)), data);

        assertPositionalFilter(ImmutableMap.of(
                1, BigintRange.of(0, 50, false),
                2, BigintRange.of(25, 50, false)), data);

        assertPositionalFilter(ImmutableMap.of(
                2, BigintRange.of(25, 50, false)), data);
    }

    private void assertPositionalFilter(Map<Integer, TupleDomainFilter> filters, Integer[][] data)
    {
        ListFilter listFilter = buildListFilter(filters, data);

        TestFilter1 testFilter = (i, value) -> Optional.ofNullable(filters.get(i + 1))
                .map(filter -> value == null ? filter.testNull() : filter.testLong(value))
                .orElse(true);

        PositionalFilter positionalFilter = listFilter.getPositionalFilter();

        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data[i].length; j++) {
                Integer value = data[i][j];
                boolean expectedToPass = testFilter.test(j, value);
                assertEquals(value == null ? positionalFilter.testNull() : positionalFilter.testLong(value), expectedToPass);
                if (!expectedToPass) {
                    assertEquals(positionalFilter.getPrecedingPositionsToFail(), j);
                    assertEquals(positionalFilter.getSucceedingPositionsToFail(), data[i].length - j - 1);
                    break;
                }
            }
        }
    }

    private static ListFilter buildListFilter(Map<Integer, TupleDomainFilter> filters, Integer[][] data)
    {
        Map<Subfield, TupleDomainFilter> subfieldFilters = filters.entrySet().stream()
                .collect(toImmutableMap(entry -> toSubfield(entry.getKey()), Map.Entry::getValue));

        ListFilter filter = new ListFilter(makeStreamDescriptor(1), subfieldFilters);

        int[] lengths = Arrays.stream(data).mapToInt(v -> v.length).toArray();
        filter.populateElementFilters(data.length, null, lengths, Arrays.stream(lengths).sum());

        return filter;
    }

    private static Subfield toSubfield(Integer index)
    {
        return new Subfield(format("c[%s]", index));
    }

    @Test
    public void testNestedArray()
    {
        Integer[][][] data = new Integer[ROW_COUNT][][];
        int count = 0;
        for (int i = 0; i < data.length; i++) {
            data[i] = new Integer[3 + random.nextInt(10)][];
            for (int j = 0; j < data[i].length; j++) {
                data[i][j] = new Integer[3 + random.nextInt(10)];
                for (int k = 0; k < data[i][j].length; k++) {
                    if (count++ % 11 == 0) {
                        data[i][j][k] = null;
                    }
                    else {
                        data[i][j][k] = random.nextInt(100);
                    }
                }
            }
        }

        assertPositionalFilter(ImmutableMap.of(
                RowAndColumn.of(1, 1), BigintRange.of(0, 50, false)), data);

        assertPositionalFilter(ImmutableMap.of(
                RowAndColumn.of(1, 2), BigintRange.of(0, 50, false),
                RowAndColumn.of(3, 2), BigintRange.of(25, 50, false)), data);

        assertPositionalFilter(ImmutableMap.of(
                RowAndColumn.of(2, 1), BigintRange.of(0, 50, false),
                RowAndColumn.of(2, 2), BigintRange.of(10, 40, false),
                RowAndColumn.of(3, 3), BigintRange.of(20, 30, false)), data);
    }

    private void assertPositionalFilter(Map<RowAndColumn, TupleDomainFilter> filters, Integer[][][] data)
    {
        ListFilter listFilter = buildListFilter(filters, data);

        TestFilter2 testFilter = (i, j, value) -> Optional.ofNullable(filters.get(RowAndColumn.of(i + 1, j + 1)))
                .map(filter -> value == null ? filter.testNull() : filter.testLong(value))
                .orElse(true);

        PositionalFilter positionalFilter = listFilter.getChild().getPositionalFilter();

        for (int i = 0; i < data.length; i++) {
            boolean expectedToPass = true;
            int passCount = 0;
            int failCount = 0;
            for (int j = 0; j < data[i].length; j++) {
                if (!expectedToPass) {
                    failCount += data[i][j].length;
                    continue;
                }
                for (int k = 0; k < data[i][j].length; k++) {
                    Integer value = data[i][j][k];
                    expectedToPass = testFilter.test(j, k, value);
                    assertEquals(value == null ? positionalFilter.testNull() : positionalFilter.testLong(value), expectedToPass);
                    if (!expectedToPass) {
                        assertEquals(positionalFilter.getPrecedingPositionsToFail(), passCount);
                        failCount = data[i][j].length - k - 1;
                        break;
                    }
                    passCount++;
                }
            }
            assertEquals(positionalFilter.getSucceedingPositionsToFail(), failCount);
        }
    }

    private static ListFilter buildListFilter(Map<RowAndColumn, TupleDomainFilter> filters, Integer[][][] data)
    {
        Map<Subfield, TupleDomainFilter> subfieldFilters = filters.entrySet().stream()
                .collect(toImmutableMap(entry -> toSubfield(entry.getKey()), Map.Entry::getValue));

        ListFilter filter = new ListFilter(makeStreamDescriptor(2), subfieldFilters);

        int[] lengths = Arrays.stream(data).mapToInt(v -> v.length).toArray();
        filter.populateElementFilters(data.length, null, lengths, Arrays.stream(lengths).sum());

        int[] nestedLenghts = Arrays.stream(data).flatMap(Arrays::stream).mapToInt(v -> v.length).toArray();
        ((ListFilter) filter.getChild()).populateElementFilters(Arrays.stream(lengths).sum(), null, nestedLenghts, Arrays.stream(nestedLenghts).sum());

        return filter;
    }

    private static StreamDescriptor makeStreamDescriptor(int levels)
    {
        NoopOrcDataSource orcDataSource = NoopOrcDataSource.INSTANCE;

        OrcType intType = new OrcType(INT, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty());
        OrcType listType = new OrcType(LIST, ImmutableList.of(1), ImmutableList.of("item"), Optional.empty(), Optional.empty(), Optional.empty());

        StreamDescriptor streamDescriptor = new StreamDescriptor("a", 0, "a", intType, orcDataSource, ImmutableList.of());
        for (int i = 0; i < levels; i++) {
            streamDescriptor = new StreamDescriptor("a", 0, "a", listType, orcDataSource, ImmutableList.of(streamDescriptor));
        }

        return streamDescriptor;
    }

    private static Subfield toSubfield(RowAndColumn indices)
    {
        return new Subfield(format("c[%s][%s]", indices.getRow(), indices.getColumn()));
    }

    private interface TestFilter1
    {
        boolean test(int i, Integer value);
    }

    private interface TestFilter2
    {
        boolean test(int i, int j, Integer value);
    }

    private static final class RowAndColumn
    {
        private final int row;
        private final int column;

        private RowAndColumn(int row, int column)
        {
            this.row = row;
            this.column = column;
        }

        public static RowAndColumn of(int row, int column)
        {
            return new RowAndColumn(row, column);
        }

        public int getRow()
        {
            return row;
        }

        public int getColumn()
        {
            return column;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            RowAndColumn that = (RowAndColumn) o;
            return row == that.row &&
                    column == that.column;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(row, column);
        }
    }
}
