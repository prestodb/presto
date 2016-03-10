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

import com.facebook.presto.orc.metadata.ColumnStatistics;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class TestingOrcPredicate
{
    private static final int ORC_ROW_GROUP_SIZE = 10_000;

    private TestingOrcPredicate()
    {
    }

    public static OrcPredicate createOrcPredicate(ObjectInspector objectInspector, Iterable<?> values)
    {
        List<Object> expectedValues = newArrayList(values);
        if (!(objectInspector instanceof PrimitiveObjectInspector)) {
            return new BasicOrcPredicate<>(expectedValues, Object.class);
        }

        PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;
        PrimitiveCategory primitiveCategory = primitiveObjectInspector.getPrimitiveCategory();

        switch (primitiveCategory) {
            case BOOLEAN:
                return new BooleanOrcPredicate(expectedValues);
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new LongOrcPredicate(
                        expectedValues.stream()
                                .map(value -> value == null ? null : ((Number) value).longValue())
                                .collect(toList()));
            case TIMESTAMP:
                return new LongOrcPredicate(
                        expectedValues.stream()
                                .map(value -> value == null ? null : ((SqlTimestamp) value).getMillisUtc())
                                .collect(toList()));
            case DATE:
                return new DateOrcPredicate(
                        expectedValues.stream()
                                .map(value -> value == null ? null : (long) ((SqlDate) value).getDays())
                                .collect(toList()));
            case FLOAT:
            case DOUBLE:
                return new DoubleOrcPredicate(
                        expectedValues.stream()
                                .map(value -> value == null ? null : ((Number) value).doubleValue())
                                .collect(toList()));
            case BINARY:
                // binary does not have stats
                return new BasicOrcPredicate<>(expectedValues, Object.class);
            case STRING:
                return new StringOrcPredicate(expectedValues);
            case DECIMAL:
                return new DecimalOrcPredicate(expectedValues);
            default:
                throw new IllegalArgumentException("Unsupported types " + primitiveCategory);
        }
    }

    public static class BasicOrcPredicate<T>
            implements OrcPredicate
    {
        private final List<T> expectedValues;

        public BasicOrcPredicate(Iterable<?> expectedValues, Class<T> type)
        {
            List<T> values = new ArrayList<>();
            for (Object expectedValue : expectedValues) {
                values.add(type.cast(expectedValue));
            }
            this.expectedValues = Collections.unmodifiableList(values);
        }

        @Override
        public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex)
        {
            ColumnStatistics columnStatistics = statisticsByColumnIndex.get(0);
            assertTrue(columnStatistics.hasNumberOfValues());

            if (numberOfRows == expectedValues.size()) {
                // whole file
                assertChunkStats(expectedValues, columnStatistics);
            }
            else if (numberOfRows == ORC_ROW_GROUP_SIZE) {
                // middle section
                boolean foundMatch = false;

                int length;
                for (int offset = 0; offset < expectedValues.size(); offset += length) {
                    length = Math.min(ORC_ROW_GROUP_SIZE, expectedValues.size() - offset);
                    if (chunkMatchesStats(expectedValues.subList(offset, offset + length), columnStatistics)) {
                        foundMatch = true;
                        break;
                    }
                }
                assertTrue(foundMatch);
            }
            else if (numberOfRows == expectedValues.size() % ORC_ROW_GROUP_SIZE) {
                // tail section
                List<T> chunk = expectedValues.subList((int) (expectedValues.size() - numberOfRows), expectedValues.size());
                assertChunkStats(chunk, columnStatistics);
            }
            else {
                fail("Unexpected number of rows: " + numberOfRows);
            }
            return true;
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
        public BooleanOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, Boolean.class);
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
        public DoubleOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, Double.class);
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
                // verify min
                if (Math.abs(columnStatistics.getDoubleStatistics().getMin() - Ordering.natural().nullsLast().min(chunk)) > 0.001) {
                    return false;
                }

                // verify max
                if (Math.abs(columnStatistics.getDoubleStatistics().getMax() - Ordering.natural().nullsFirst().max(chunk)) > 0.001) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class DecimalOrcPredicate
            extends BasicOrcPredicate<SqlDecimal>
    {
        public DecimalOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, SqlDecimal.class);
        }
    }

    public static class LongOrcPredicate
            extends BasicOrcPredicate<Long>
    {
        public LongOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, Long.class);
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
                // verify min
                if (!columnStatistics.getIntegerStatistics().getMin().equals(Ordering.natural().nullsLast().min(chunk))) {
                    return false;
                }

                // verify max
                if (!columnStatistics.getIntegerStatistics().getMax().equals(Ordering.natural().nullsFirst().max(chunk))) {
                    return false;
                }
            }

            return true;
        }
    }

    public static class StringOrcPredicate
            extends BasicOrcPredicate<String>
    {
        public StringOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, String.class);
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
                // verify min
                Slice chunkMin = Ordering.natural().nullsLast().min(slices);
                if (columnStatistics.getStringStatistics().getMin().compareTo(chunkMin) > 0) {
                    return false;
                }

                // verify max
                Slice chunkMax = Ordering.natural().nullsFirst().max(slices);
                if (columnStatistics.getStringStatistics().getMax().compareTo(chunkMax) < 0) {
                    return false;
                }
            }

            return true;
        }
    }

    public static class DateOrcPredicate
            extends BasicOrcPredicate<Long>
    {
        public DateOrcPredicate(Iterable<?> expectedValues)
        {
            super(expectedValues, Long.class);
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

            return true;
        }
    }
}
