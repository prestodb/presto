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
package com.facebook.presto.accumulo.iterators;

import com.facebook.presto.accumulo.iterators.SingleColumnValueFilter.CompareOp;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSingleColumnValueFilter
{
    private static final LexicoderRowSerializer SERIALIZER = new LexicoderRowSerializer();

    private static byte[] encode(Type type, Object v)
    {
        return SERIALIZER.encode(type, v);
    }

    private final Random random = new Random();

    public boolean testFilter(
            String filterFamily,
            String filterQualifier,
            CompareOp filterOp,
            Object filterValue,
            Type type,
            Key testKey,
            Object testValue)
            throws IOException
    {
        Value vFilterValue = new Value(encode(type, filterValue));
        Map<String, String> opts = SingleColumnValueFilter.getProperties(filterFamily, filterQualifier, filterOp, vFilterValue.get());

        SingleColumnValueFilter filter = new SingleColumnValueFilter();
        filter.validateOptions(opts);

        Value vTestValue = new Value(encode(type, testValue));

        filter.init(new TestKeyValueIterator(testKey, vTestValue), opts, null);
        return filter.acceptRow(new TestKeyValueIterator(testKey, vTestValue));
    }

    @Test
    public void testNullField()
            throws IOException
    {
        Value vFilterValue = new Value(encode(BigintType.BIGINT, 5L));

        Map<String, String> opts = SingleColumnValueFilter.getProperties("cf", "cq1", CompareOp.EQUAL, vFilterValue.get());

        SingleColumnValueFilter filter = new SingleColumnValueFilter();
        filter.validateOptions(opts);

        Key testKey = new Key("row", "cf", "cq2");

        filter.init(new TestKeyValueIterator(testKey, vFilterValue), opts, null);
        assertFalse(filter.acceptRow(new TestKeyValueIterator(testKey, vFilterValue)));
    }

    public void testBigint(CompareOp op, BiFunction<Long, Long, Boolean> func)
            throws Exception
    {
        String filterFamily = "cf";
        String filterQualifier = "cq";

        Key matchingKey = new Key("row", filterFamily, filterQualifier);
        Key wrongFam = new Key("row", "cf2", filterQualifier);
        Key wrongQual = new Key("row", filterFamily, "cq2");

        Set<Long> values = new HashSet<>();
        for (int i = 0; i < 100; ++i) {
            values.add((long) random.nextInt());
        }

        for (long filterValue : values) {
            for (long testValue : values) {
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue, BigintType.BIGINT, wrongFam, testValue),
                        "Filter accepted key with wrong family");
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue, BigintType.BIGINT, wrongQual, testValue),
                        "Filter accepted key with wrong qual");

                boolean test = testFilter(filterFamily, filterQualifier, op, filterValue, BigintType.BIGINT, matchingKey, testValue);
                if (func.apply(testValue, filterValue)) {
                    assertTrue(test);
                }
                else {
                    assertFalse(test);
                }
            }
        }
    }

    @Test
    public void testBigintLess()
            throws Exception
    {
        testBigint(CompareOp.LESS, (x, y) -> Long.compare(x, y) < 0);
    }

    @Test
    public void testBigintLessOrEqual()
            throws Exception
    {
        testBigint(CompareOp.LESS_OR_EQUAL, (x, y) -> Long.compare(x, y) <= 0);
    }

    @Test
    public void testBigintEqual()
            throws Exception
    {
        testBigint(CompareOp.EQUAL, (x, y) -> Long.compare(x, y) == 0);
    }

    @Test
    public void testBigintNotEqual()
            throws Exception
    {
        testBigint(CompareOp.NOT_EQUAL, (x, y) -> Long.compare(x, y) != 0);
    }

    @Test
    public void testBigintGreater()
            throws Exception
    {
        testBigint(CompareOp.GREATER, (x, y) -> Long.compare(x, y) > 0);
    }

    @Test
    public void testBigintGreaterOrEqual()
            throws Exception
    {
        testBigint(CompareOp.GREATER_OR_EQUAL, (x, y) -> Long.compare(x, y) >= 0);
    }

    public void testBoolean(CompareOp op, BiFunction<Boolean, Boolean, Boolean> func)
            throws Exception
    {
        String filterFamily = "cf";
        String filterQualifier = "cq";

        Key matchingKey = new Key("row", filterFamily, filterQualifier);
        Key wrongFam = new Key("row", "cf2", filterQualifier);
        Key wrongQual = new Key("row", filterFamily, "cq2");

        Set<Boolean> values = new HashSet<>();
        values.add(true);
        values.add(false);

        for (boolean filterValue : values) {
            for (boolean testValue : values) {
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue, BooleanType.BOOLEAN, wrongFam, testValue),
                        "Filter accepted key with wrong family");
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue, BooleanType.BOOLEAN, wrongQual, testValue),
                        "Filter accepted key with wrong qual");

                boolean test = testFilter(filterFamily, filterQualifier, op, filterValue, BooleanType.BOOLEAN, matchingKey, testValue);
                if (func.apply(testValue, filterValue)) {
                    assertTrue(test);
                }
                else {
                    assertFalse(test);
                }
            }
        }
    }

    @Test
    public void testBooleanLess()
            throws Exception
    {
        testBoolean(CompareOp.LESS, (x, y) -> Boolean.compare(x, y) < 0);
    }

    @Test
    public void testBooleanLessOrEqual()
            throws Exception
    {
        testBoolean(CompareOp.LESS_OR_EQUAL, (x, y) -> Boolean.compare(x, y) <= 0);
    }

    @Test
    public void testBooleanEqual()
            throws Exception
    {
        testBoolean(CompareOp.EQUAL, (x, y) -> Boolean.compare(x, y) == 0);
    }

    @Test
    public void testBooleanNotEqual()
            throws Exception
    {
        testBoolean(CompareOp.NOT_EQUAL, (x, y) -> Boolean.compare(x, y) != 0);
    }

    @Test
    public void testBooleanGreater()
            throws Exception
    {
        testBoolean(CompareOp.GREATER, (x, y) -> Boolean.compare(x, y) > 0);
    }

    @Test
    public void testBooleanGreaterOrEqual()
            throws Exception
    {
        testBoolean(CompareOp.GREATER_OR_EQUAL, (x, y) -> Boolean.compare(x, y) >= 0);
    }

    public void testDate(CompareOp op, BiFunction<Date, Date, Boolean> func)
            throws Exception
    {
        String filterFamily = "cf";
        String filterQualifier = "cq";

        Key matchingKey = new Key("row", filterFamily, filterQualifier);
        Key wrongFam = new Key("row", "cf2", filterQualifier);
        Key wrongQual = new Key("row", filterFamily, "cq2");

        Set<Date> values = new HashSet<>();
        for (int i = 0; i < 100; ++i) {
            values.add(new Date(random.nextInt()));
        }

        for (Date filterValue : values) {
            for (Date testValue : values) {
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue.getTime(), DateType.DATE, wrongFam, testValue.getTime()),
                        "Filter accepted key with wrong family");
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue.getTime(), DateType.DATE, wrongQual, testValue.getTime()),
                        "Filter accepted key with wrong qual");

                boolean test = testFilter(filterFamily, filterQualifier, op, filterValue.getTime(), DateType.DATE, matchingKey, testValue.getTime());
                if (func.apply(testValue, filterValue)) {
                    assertTrue(test);
                }
                else {
                    assertFalse(test);
                }
            }
        }
    }

    @Test
    public void testDateLess()
            throws Exception
    {
        testDate(CompareOp.LESS, (x, y) -> x.compareTo(y) < 0);
    }

    @Test
    public void testDateLessOrEqual()
            throws Exception
    {
        testDate(CompareOp.LESS_OR_EQUAL, (x, y) -> x.compareTo(y) <= 0);
    }

    @Test
    public void testDateEqual()
            throws Exception
    {
        testDate(CompareOp.EQUAL, (x, y) -> x.compareTo(y) == 0);
    }

    @Test
    public void testDateNotEqual()
            throws Exception
    {
        testDate(CompareOp.NOT_EQUAL, (x, y) -> x.compareTo(y) != 0);
    }

    @Test
    public void testDateGreater()
            throws Exception
    {
        testDate(CompareOp.GREATER, (x, y) -> x.compareTo(y) > 0);
    }

    @Test
    public void testDateGreaterOrEqual()
            throws Exception
    {
        testDate(CompareOp.GREATER_OR_EQUAL, (x, y) -> x.compareTo(y) >= 0);
    }

    public void testDouble(CompareOp op, BiFunction<Double, Double, Boolean> func)
            throws Exception
    {
        String filterFamily = "cf";
        String filterQualifier = "cq";

        Key matchingKey = new Key("row", filterFamily, filterQualifier);
        Key wrongFam = new Key("row", "cf2", filterQualifier);
        Key wrongQual = new Key("row", filterFamily, "cq2");

        Set<Double> values = new HashSet<>();
        for (int i = 0; i < 100; ++i) {
            values.add(random.nextDouble());
        }

        for (double filterValue : values) {
            for (double testValue : values) {
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue, DoubleType.DOUBLE, wrongFam, testValue),
                        "Filter accepted key with wrong family");
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue, DoubleType.DOUBLE, wrongQual, testValue),
                        "Filter accepted key with wrong qual");

                boolean test = testFilter(filterFamily, filterQualifier, op, filterValue, DoubleType.DOUBLE, matchingKey, testValue);
                if (func.apply(testValue, filterValue)) {
                    assertTrue(test);
                }
                else {
                    assertFalse(test);
                }
            }
        }
    }

    @Test
    public void testDoubleLess()
            throws Exception
    {
        testDouble(CompareOp.LESS, (x, y) -> x.compareTo(y) < 0);
    }

    @Test
    public void testDoubleLessOrEqual()
            throws Exception
    {
        testDouble(CompareOp.LESS_OR_EQUAL, (x, y) -> x.compareTo(y) <= 0);
    }

    @Test
    public void testDoubleEqual()
            throws Exception
    {
        testDouble(CompareOp.EQUAL, (x, y) -> x.compareTo(y) == 0);
    }

    @Test
    public void testDoubleNotEqual()
            throws Exception
    {
        testDouble(CompareOp.NOT_EQUAL, (x, y) -> x.compareTo(y) != 0);
    }

    @Test
    public void testDoubleGreater()
            throws Exception
    {
        testDouble(CompareOp.GREATER, (x, y) -> x.compareTo(y) > 0);
    }

    @Test
    public void testDoubleGreaterOrEqual()
            throws Exception
    {
        testDouble(CompareOp.GREATER_OR_EQUAL, (x, y) -> x.compareTo(y) >= 0);
    }

    public void testTime(CompareOp op, BiFunction<Time, Time, Boolean> func)
            throws Exception
    {
        String filterFamily = "cf";
        String filterQualifier = "cq";

        Key matchingKey = new Key("row", filterFamily, filterQualifier);
        Key wrongFam = new Key("row", "cf2", filterQualifier);
        Key wrongQual = new Key("row", filterFamily, "cq2");

        Set<Time> values = new HashSet<>();
        for (int i = 0; i < 100; ++i) {
            values.add(new Time(random.nextInt()));
        }

        for (Time filterValue : values) {
            for (Time testValue : values) {
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue.getTime(), TimeType.TIME, wrongFam, testValue.getTime()),
                        "Filter accepted key with wrong family");
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue.getTime(), TimeType.TIME, wrongQual, testValue.getTime()),
                        "Filter accepted key with wrong qual");

                boolean test = testFilter(filterFamily, filterQualifier, op, filterValue.getTime(), TimeType.TIME, matchingKey, testValue.getTime());
                if (func.apply(testValue, filterValue)) {
                    assertTrue(test);
                }
                else {
                    assertFalse(test);
                }
            }
        }
    }

    @Test
    public void testTimeLess()
            throws Exception
    {
        testTime(CompareOp.LESS, (x, y) -> x.compareTo(y) < 0);
    }

    @Test
    public void testTimeLessOrEqual()
            throws Exception
    {
        testTime(CompareOp.LESS_OR_EQUAL, (x, y) -> x.compareTo(y) <= 0);
    }

    @Test
    public void testTimeEqual()
            throws Exception
    {
        testTime(CompareOp.EQUAL, (x, y) -> x.compareTo(y) == 0);
    }

    @Test
    public void testTimeNotEqual()
            throws Exception
    {
        testTime(CompareOp.NOT_EQUAL, (x, y) -> x.compareTo(y) != 0);
    }

    @Test
    public void testTimeGreater()
            throws Exception
    {
        testTime(CompareOp.GREATER, (x, y) -> x.compareTo(y) > 0);
    }

    @Test
    public void testTimeGreaterOrEqual()
            throws Exception
    {
        testTime(CompareOp.GREATER_OR_EQUAL, (x, y) -> x.compareTo(y) >= 0);
    }

    public void testTimestamp(CompareOp op, BiFunction<Timestamp, Timestamp, Boolean> func)
            throws Exception
    {
        String filterFamily = "cf";
        String filterQualifier = "cq";

        Key matchingKey = new Key("row", filterFamily, filterQualifier);
        Key wrongFam = new Key("row", "cf2", filterQualifier);
        Key wrongQual = new Key("row", filterFamily, "cq2");

        Set<Timestamp> values = new HashSet<>();
        for (int i = 0; i < 100; ++i) {
            values.add(new Timestamp(random.nextInt()));
        }

        for (Timestamp filterValue : values) {
            for (Timestamp testValue : values) {
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue.getTime(), TimestampType.TIMESTAMP, wrongFam, testValue.getTime()),
                        "Filter accepted key with wrong family");
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue.getTime(), TimestampType.TIMESTAMP, wrongQual, testValue.getTime()),
                        "Filter accepted key with wrong qual");

                boolean test = testFilter(filterFamily, filterQualifier, op, filterValue.getTime(), TimestampType.TIMESTAMP, matchingKey, testValue.getTime());
                if (func.apply(testValue, filterValue)) {
                    assertTrue(test);
                }
                else {
                    assertFalse(test);
                }
            }
        }
    }

    @Test
    public void testTimestampLess()
            throws Exception
    {
        testTimestamp(CompareOp.LESS, (x, y) -> x.compareTo(y) < 0);
    }

    @Test
    public void testTimestampLessOrEqual()
            throws Exception
    {
        testTimestamp(CompareOp.LESS_OR_EQUAL, (x, y) -> x.compareTo(y) <= 0);
    }

    @Test
    public void testTimestampEqual()
            throws Exception
    {
        testTimestamp(CompareOp.EQUAL, (x, y) -> x.compareTo(y) == 0);
    }

    @Test
    public void testTimestampNotEqual()
            throws Exception
    {
        testTimestamp(CompareOp.NOT_EQUAL, (x, y) -> x.compareTo(y) != 0);
    }

    @Test
    public void testTimestampGreater()
            throws Exception
    {
        testTimestamp(CompareOp.GREATER, (x, y) -> x.compareTo(y) > 0);
    }

    @Test
    public void testTimestampGreaterOrEqual()
            throws Exception
    {
        testTimestamp(CompareOp.GREATER_OR_EQUAL, (x, y) -> x.compareTo(y) >= 0);
    }

    public void testVarbinary(CompareOp op, BiFunction<byte[], byte[], Boolean> func)
            throws Exception
    {
        String filterFamily = "cf";
        String filterQualifier = "cq";

        Key matchingKey = new Key("row", filterFamily, filterQualifier);
        Key wrongFam = new Key("row", "cf2", filterQualifier);
        Key wrongQual = new Key("row", filterFamily, "cq2");

        Set<byte[]> bytes = new HashSet<>();
        for (int i = 0; i < 100; ++i) {
            bytes.add(UUID.randomUUID().toString().getBytes(UTF_8));
        }

        for (byte[] filterValue : bytes) {
            for (byte[] testValue : bytes) {
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue, VarbinaryType.VARBINARY, wrongFam, testValue),
                        "Filter accepted key with wrong family");
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue, VarbinaryType.VARBINARY, wrongQual, testValue),
                        "Filter accepted key with wrong qual");

                boolean test = testFilter(filterFamily, filterQualifier, op, filterValue, VarbinaryType.VARBINARY, matchingKey, testValue);
                if (func.apply(testValue, filterValue)) {
                    assertTrue(test);
                }
                else {
                    assertFalse(test);
                }
            }
        }
    }

    @Test
    public void testVarbinaryLess()
            throws Exception
    {
        testVarbinary(CompareOp.LESS, (x, y) -> wrap(x).compareTo(wrap(y)) < 0);
    }

    @Test
    public void testVarbinaryLessOrEqual()
            throws Exception
    {
        testVarbinary(CompareOp.LESS_OR_EQUAL, (x, y) -> wrap(x).compareTo(wrap(y)) <= 0);
    }

    @Test
    public void testVarbinaryEqual()
            throws Exception
    {
        testVarbinary(CompareOp.EQUAL, (x, y) -> wrap(x).compareTo(wrap(y)) == 0);
    }

    @Test
    public void testVarbinaryNotEqual()
            throws Exception
    {
        testVarbinary(CompareOp.NOT_EQUAL, (x, y) -> wrap(x).compareTo(wrap(y)) != 0);
    }

    @Test
    public void testVarbinaryGreater()
            throws Exception
    {
        testVarbinary(CompareOp.GREATER, (x, y) -> wrap(x).compareTo(wrap(y)) > 0);
    }

    @Test
    public void testVarbinaryGreaterOrEqual()
            throws Exception
    {
        testVarbinary(CompareOp.GREATER_OR_EQUAL, (x, y) -> wrap(x).compareTo(wrap(y)) >= 0);
    }

    public void testVarchar(CompareOp op, BiFunction<String, String, Boolean> func)
            throws Exception
    {
        String filterFamily = "cf";
        String filterQualifier = "cq";

        Key matchingKey = new Key("row", filterFamily, filterQualifier);
        Key wrongFam = new Key("row", "cf2", filterQualifier);
        Key wrongQual = new Key("row", filterFamily, "cq2");

        Set<String> bytes = new HashSet<>();
        for (int i = 0; i < 100; ++i) {
            bytes.add(UUID.randomUUID().toString());
        }

        for (String filterValue : bytes) {
            for (String testValue : bytes) {
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue, VarcharType.VARCHAR, wrongFam, testValue),
                        "Filter accepted key with wrong family");
                assertFalse(
                        testFilter(filterFamily, filterQualifier, op, filterValue, VarcharType.VARCHAR, wrongQual, testValue),
                        "Filter accepted key with wrong qual");

                boolean test = testFilter(filterFamily, filterQualifier, op, filterValue, VarcharType.VARCHAR, matchingKey, testValue);
                if (func.apply(testValue, filterValue)) {
                    assertTrue(test);
                }
                else {
                    assertFalse(test);
                }
            }
        }
    }

    @Test
    public void testVarcharLess()
            throws Exception
    {
        testVarchar(CompareOp.LESS, (x, y) -> x.compareTo(y) < 0);
    }

    @Test
    public void testVarcharLessOrEqual()
            throws Exception
    {
        testVarchar(CompareOp.LESS_OR_EQUAL, (x, y) -> x.compareTo(y) <= 0);
    }

    @Test
    public void testVarcharEqual()
            throws Exception
    {
        testVarchar(CompareOp.EQUAL, (x, y) -> x.compareTo(y) == 0);
    }

    @Test
    public void testVarcharNotEqual()
            throws Exception
    {
        testVarchar(CompareOp.NOT_EQUAL, (x, y) -> x.compareTo(y) != 0);
    }

    @Test
    public void testVarcharGreater()
            throws Exception
    {
        testVarchar(CompareOp.GREATER, (x, y) -> x.compareTo(y) > 0);
    }

    @Test
    public void testVarcharGreaterOrEqual()
            throws Exception
    {
        testVarchar(CompareOp.GREATER_OR_EQUAL, (x, y) -> x.compareTo(y) >= 0);
    }
}
