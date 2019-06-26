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

import com.facebook.presto.orc.OrcTester.Format;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class TestingOrcPredicate
{
    public static final int ORC_STRIPE_SIZE = 30_000;
    public static final int ORC_ROW_GROUP_SIZE = 10_000;

    private TestingOrcPredicate()
    {
    }

    public static OrcPredicate createOrcPredicate(List<Type> types, List<List<?>> values, Format format, boolean isHiveWriter)
    {
        List<OrcPredicate> orcPredicates = IntStream.range(0, types.size())
                .mapToObj(i -> createOrcPredicate(i, types.get(i), values.get(i), format, isHiveWriter))
                .collect(toImmutableList());

        return new MultiOrcPredicate(orcPredicates);
    }

    public static OrcPredicate createOrcPredicate(int columnIndex, Type type, Iterable<?> values, Format format, boolean isHiveWriter)
    {
        List<Object> expectedValues = newArrayList(values);
        if (BOOLEAN.equals(type)) {
            return new BooleanOrcPredicate(columnIndex, expectedValues, format == DWRF);
        }
        if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type) || BIGINT.equals(type)) {
            return new LongOrcPredicate(
                    columnIndex,
                    expectedValues.stream()
                            .map(value -> value == null ? null : ((Number) value).longValue())
                            .collect(toList()),
                    format == DWRF);
        }
        if (TIMESTAMP.equals(type)) {
            return new LongOrcPredicate(
                    columnIndex,
                    expectedValues.stream()
                            .map(value -> value == null ? null : ((SqlTimestamp) value).getMillisUtc())
                            .collect(toList()),
                    format == DWRF);
        }
        if (DATE.equals(type)) {
            return new DateOrcPredicate(
                    columnIndex,
                    expectedValues.stream()
                            .map(value -> value == null ? null : (long) ((SqlDate) value).getDays())
                            .collect(toList()),
                    format == DWRF);
        }
        if (REAL.equals(type) || DOUBLE.equals(type)) {
            return new DoubleOrcPredicate(
                    columnIndex,
                    expectedValues.stream()
                            .map(value -> value == null ? null : ((Number) value).doubleValue())
                            .collect(toList()),
                    format == DWRF);
        }
        if (type instanceof VarbinaryType) {
            // binary does not have stats
            return new BasicOrcPredicate<>(columnIndex, expectedValues, Object.class, format == DWRF);
        }
        if (type instanceof VarcharType) {
            return new StringOrcPredicate(columnIndex, expectedValues, format, isHiveWriter);
        }
        if (type instanceof CharType) {
            return new CharOrcPredicate(columnIndex, expectedValues, format == DWRF);
        }
        if (type instanceof DecimalType) {
            return new DecimalOrcPredicate(columnIndex, expectedValues, format == DWRF);
        }

        String baseType = type.getTypeSignature().getBase();
        if (ARRAY.equals(baseType) || MAP.equals(baseType) || ROW.equals(baseType)) {
            return new BasicOrcPredicate<>(columnIndex, expectedValues, Object.class, format == DWRF);
        }
        throw new IllegalArgumentException("Unsupported type " + type);
    }

    private static class MultiOrcPredicate
            implements OrcPredicate
    {
        private final List<OrcPredicate> orcPredicates;

        public MultiOrcPredicate(List<OrcPredicate> orcPredicates)
        {
            this.orcPredicates = requireNonNull(orcPredicates, "orcPredicates is null");
        }

        @Override
        public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex)
        {
            return orcPredicates.stream()
                .allMatch(predicate -> predicate.matches(numberOfRows, statisticsByColumnIndex));
        }
    }

    public static class BasicOrcPredicate<T>
            implements OrcPredicate
    {
        private final int columnIndex;
        private final List<T> expectedValues;
        private final boolean noFileStats;

        public BasicOrcPredicate(int columnIndex, Iterable<?> expectedValues, Class<T> type, boolean noFileStats)
        {
            List<T> values = new ArrayList<>();
            for (Object expectedValue : expectedValues) {
                values.add(type.cast(expectedValue));
            }
            this.columnIndex = columnIndex;
            this.expectedValues = Collections.unmodifiableList(values);
            this.noFileStats = noFileStats;
        }

        @Override
        public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex)
        {
            ColumnStatistics columnStatistics = statisticsByColumnIndex.get(columnIndex);

            // todo enable file stats when DWRF team verifies that the stats are correct
            // assertTrue(columnStatistics.hasNumberOfValues());
            if (noFileStats && numberOfRows == expectedValues.size()) {
                assertNull(columnStatistics);
                return true;
            }

            if (numberOfRows == expectedValues.size()) {
                // whole file
                assertChunkStats(expectedValues, columnStatistics);
            }
            else if (numberOfRows == ORC_ROW_GROUP_SIZE) {
                // middle section
                matchMiddleSection(columnStatistics, ORC_ROW_GROUP_SIZE);
            }
            else if (numberOfRows == ORC_STRIPE_SIZE) {
                // middle section
                matchMiddleSection(columnStatistics, ORC_STRIPE_SIZE);
            }
            else if (numberOfRows == expectedValues.size() % ORC_ROW_GROUP_SIZE || numberOfRows == expectedValues.size() % ORC_STRIPE_SIZE) {
                // tail section
                List<T> chunk = expectedValues.subList((int) (expectedValues.size() - numberOfRows), expectedValues.size());
                assertChunkStats(chunk, columnStatistics);
            }
            else if (numberOfRows == expectedValues.size() % ORC_STRIPE_SIZE) {
                // tail section
                List<T> chunk = expectedValues.subList((int) (expectedValues.size() - numberOfRows), expectedValues.size());
                assertChunkStats(chunk, columnStatistics);
            }
            else {
                fail("Unexpected number of rows: " + numberOfRows);
            }
            return true;
        }

        private void matchMiddleSection(ColumnStatistics columnStatistics, int size)
        {
            int length;
            for (int offset = 0; offset < expectedValues.size(); offset += length) {
                length = Math.min(size, expectedValues.size() - offset);
                if (chunkMatchesStats(expectedValues.subList(offset, offset + length), columnStatistics)) {
                    return;
                }
            }
            fail("match not found for middle section");
        }

        private void assertChunkStats(List<T> chunk, ColumnStatistics columnStatistics)
        {
            assertTrue(chunkMatchesStats(chunk, columnStatistics));
        }

        protected boolean chunkMatchesStats(List<T> chunk, ColumnStatistics columnStatistics)
        {
            // verify non null count
            if (columnStatistics.getNumberOfValues() != Iterables.size(filter(chunk, notNull()))) {
                return false;
            }

            return true;
        }
    }

    public static class BooleanOrcPredicate
            extends BasicOrcPredicate<Boolean>
    {
        public BooleanOrcPredicate(int columnIndex, Iterable<?> expectedValues, boolean noFileStats)
        {
            super(columnIndex, expectedValues, Boolean.class, noFileStats);
        }

        @Override
        protected boolean chunkMatchesStats(List<Boolean> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getIntegerStatistics());
            assertNull(columnStatistics.getDoubleStatistics());
            assertNull(columnStatistics.getStringStatistics());
            assertNull(columnStatistics.getDateStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            // statistics can be missing for any reason
            if (columnStatistics.getBooleanStatistics() != null) {
                if (columnStatistics.getBooleanStatistics().getTrueValueCount() != Iterables.size(filter(chunk, equalTo(Boolean.TRUE)))) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class DoubleOrcPredicate
            extends BasicOrcPredicate<Double>
    {
        public DoubleOrcPredicate(int columnIndex, Iterable<?> expectedValues, boolean noFileStats)
        {
            super(columnIndex, expectedValues, Double.class, noFileStats);
        }

        @Override
        protected boolean chunkMatchesStats(List<Double> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getBooleanStatistics());
            assertNull(columnStatistics.getIntegerStatistics());
            assertNull(columnStatistics.getStringStatistics());
            assertNull(columnStatistics.getDateStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            // statistics can be missing for any reason
            if (columnStatistics.getDoubleStatistics() != null) {
                if (chunk.stream().allMatch(Objects::isNull)) {
                    if (columnStatistics.getDoubleStatistics().getMin() != null || columnStatistics.getDoubleStatistics().getMax() != null) {
                        return false;
                    }
                }
                else {
                    // verify min
                    if (Math.abs(columnStatistics.getDoubleStatistics().getMin() - Ordering.natural().nullsLast().min(chunk)) > 0.001) {
                        return false;
                    }

                    // verify max
                    if (Math.abs(columnStatistics.getDoubleStatistics().getMax() - Ordering.natural().nullsFirst().max(chunk)) > 0.001) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private static class DecimalOrcPredicate
            extends BasicOrcPredicate<SqlDecimal>
    {
        public DecimalOrcPredicate(int columnIndex, Iterable<?> expectedValues, boolean noFileStats)
        {
            super(columnIndex, expectedValues, SqlDecimal.class, noFileStats);
        }
    }

    public static class LongOrcPredicate
            extends BasicOrcPredicate<Long>
    {
        public LongOrcPredicate(int columnIndex, Iterable<?> expectedValues, boolean noFileStats)
        {
            super(columnIndex, expectedValues, Long.class, noFileStats);
        }

        @Override
        protected boolean chunkMatchesStats(List<Long> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getBooleanStatistics());
            assertNull(columnStatistics.getDoubleStatistics());
            assertNull(columnStatistics.getStringStatistics());
            assertNull(columnStatistics.getDateStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            // statistics can be missing for any reason
            if (columnStatistics.getIntegerStatistics() != null) {
                if (chunk.stream().allMatch(Objects::isNull)) {
                    if (columnStatistics.getIntegerStatistics().getMin() != null || columnStatistics.getIntegerStatistics().getMax() != null) {
                        return false;
                    }
                }
                else {
                    // verify min
                    if (!columnStatistics.getIntegerStatistics().getMin().equals(Ordering.natural().nullsLast().min(chunk))) {
                        return false;
                    }

                    // verify max
                    if (!columnStatistics.getIntegerStatistics().getMax().equals(Ordering.natural().nullsFirst().max(chunk))) {
                        return false;
                    }
                }
                long sum = chunk.stream()
                        .filter(Objects::nonNull)
                        .mapToLong(Long::longValue)
                        .sum();
                if (columnStatistics.getIntegerStatistics().getSum() != sum) {
                    return false;
                }
            }

            return true;
        }
    }

    public static class StringOrcPredicate
            extends BasicOrcPredicate<String>
    {
        private final Format format;
        private final boolean isHiveWriter;

        public StringOrcPredicate(int columnIndex, Iterable<?> expectedValues, Format format, boolean isHiveWriter)
        {
            super(columnIndex, expectedValues, String.class, format == DWRF);
            this.format = format;
            this.isHiveWriter = isHiveWriter;
        }

        @Override
        protected boolean chunkMatchesStats(List<String> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getBooleanStatistics());
            assertNull(columnStatistics.getIntegerStatistics());
            assertNull(columnStatistics.getDoubleStatistics());
            assertNull(columnStatistics.getDateStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            List<Slice> slices = chunk.stream()
                    .filter(Objects::nonNull)
                    .map(Slices::utf8Slice)
                    .collect(toList());

            // statistics can be missing for any reason
            if (columnStatistics.getStringStatistics() != null) {
                if (slices.isEmpty()) {
                    if (columnStatistics.getStringStatistics().getMin() != null || columnStatistics.getStringStatistics().getMax() != null) {
                        return false;
                    }
                }
                else {
                    Slice chunkMin = Ordering.natural().nullsLast().min(slices);
                    Slice chunkMax = Ordering.natural().nullsFirst().max(slices);
                    if (format == DWRF && isHiveWriter) {
                        // We use the OLD open source DWRF writer for tests which uses UTF-16be for string stats. These are widened by the our reader.
                        if (columnStatistics.getStringStatistics().getMin().compareTo(chunkMin) > 0) {
                            return false;
                        }
                        if (columnStatistics.getStringStatistics().getMax().compareTo(chunkMax) < 0) {
                            return false;
                        }
                    }
                    else {
                        if (!columnStatistics.getStringStatistics().getMin().equals(chunkMin)) {
                            return false;
                        }
                        if (!columnStatistics.getStringStatistics().getMax().equals(chunkMax)) {
                            return false;
                        }
                    }
                }
            }

            return true;
        }
    }

    public static class CharOrcPredicate
            extends BasicOrcPredicate<String>
    {
        public CharOrcPredicate(int columnIndex, Iterable<?> expectedValues, boolean noFileStats)
        {
            super(columnIndex, expectedValues, String.class, noFileStats);
        }

        @Override
        protected boolean chunkMatchesStats(List<String> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getBooleanStatistics());
            assertNull(columnStatistics.getIntegerStatistics());
            assertNull(columnStatistics.getDoubleStatistics());
            assertNull(columnStatistics.getDateStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            List<String> strings = chunk.stream()
                    .filter(Objects::nonNull)
                    .map(String::trim)
                    .collect(toList());

            // statistics can be missing for any reason
            if (columnStatistics.getStringStatistics() != null) {
                if (strings.isEmpty()) {
                    if (columnStatistics.getStringStatistics().getMin() != null || columnStatistics.getStringStatistics().getMax() != null) {
                        return false;
                    }
                }
                else {
                    // verify min
                    String chunkMin = Ordering.natural().nullsLast().min(strings);
                    if (columnStatistics.getStringStatistics().getMin().toStringUtf8().trim().compareTo(chunkMin) > 0) {
                        return false;
                    }

                    // verify max
                    String chunkMax = Ordering.natural().nullsFirst().max(strings);
                    if (columnStatistics.getStringStatistics().getMax().toStringUtf8().trim().compareTo(chunkMax) < 0) {
                        return false;
                    }
                }
            }

            return true;
        }
    }

    public static class DateOrcPredicate
            extends BasicOrcPredicate<Long>
    {
        public DateOrcPredicate(int columnIndex, Iterable<?> expectedValues, boolean noFileStats)
        {
            super(columnIndex, expectedValues, Long.class, noFileStats);
        }

        @Override
        protected boolean chunkMatchesStats(List<Long> chunk, ColumnStatistics columnStatistics)
        {
            assertNull(columnStatistics.getBooleanStatistics());
            assertNull(columnStatistics.getIntegerStatistics());
            assertNull(columnStatistics.getDoubleStatistics());
            assertNull(columnStatistics.getStringStatistics());

            // check basic statistics
            if (!super.chunkMatchesStats(chunk, columnStatistics)) {
                return false;
            }

            // statistics can be missing for any reason
            if (columnStatistics.getDateStatistics() != null) {
                if (chunk.stream().allMatch(Objects::isNull)) {
                    if (columnStatistics.getDateStatistics().getMin() != null || columnStatistics.getDateStatistics().getMax() != null) {
                        return false;
                    }
                }
                else {
                    // verify min
                    Long min = columnStatistics.getDateStatistics().getMin().longValue();
                    if (!min.equals(Ordering.natural().nullsLast().min(chunk))) {
                        return false;
                    }

                    // verify max
                    Long statMax = columnStatistics.getDateStatistics().getMax().longValue();
                    Long chunkMax = Ordering.natural().nullsFirst().max(chunk);
                    if (!statMax.equals(chunkMax)) {
                        return false;
                    }
                }
            }

            return true;
        }
    }
}
