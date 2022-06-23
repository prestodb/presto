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
package com.facebook.presto.common.predicate;

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.block.TestingBlockJsonSerde;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.TestingTypeDeserializer;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestRange
{
    public static final SqlFunctionProperties PROPERTIES = SqlFunctionProperties.builder()
            .setTimeZoneKey(UTC_KEY)
            .setLegacyTimestamp(true)
            .setSessionStartTime(0)
            .setSessionLocale(ENGLISH)
            .setSessionUser("user")
            .build();

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMismatchedTypes()
    {
        // NEVER DO THIS
        new Range(Marker.exactly(BIGINT, 1L), Marker.exactly(VARCHAR, utf8Slice("a")));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvertedBounds()
    {
        new Range(Marker.exactly(BIGINT, 1L), Marker.exactly(BIGINT, 0L));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLowerUnboundedOnly()
    {
        new Range(Marker.lowerUnbounded(BIGINT), Marker.lowerUnbounded(BIGINT));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUpperUnboundedOnly()
    {
        new Range(Marker.upperUnbounded(BIGINT), Marker.upperUnbounded(BIGINT));
    }

    @Test
    public void testSingleValue()
    {
        assertTrue(Range.range(BIGINT, 1L, true, 1L, true).isSingleValue());
        assertFalse(Range.range(BIGINT, 1L, true, 2L, true).isSingleValue());
        assertTrue(Range.range(DOUBLE, 1.1, true, 1.1, true).isSingleValue());
        assertTrue(Range.range(VARCHAR, utf8Slice("a"), true, utf8Slice("a"), true).isSingleValue());
        assertTrue(Range.range(BOOLEAN, true, true, true, true).isSingleValue());
        assertFalse(Range.range(BOOLEAN, false, true, true, true).isSingleValue());
    }

    @Test
    public void testAllRange()
    {
        Range range = Range.all(BIGINT);
        assertEquals(range.getLow(), Marker.lowerUnbounded(BIGINT));
        assertEquals(range.getHigh(), Marker.upperUnbounded(BIGINT));
        assertFalse(range.isSingleValue());
        assertTrue(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertTrue(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertTrue(range.includes(Marker.below(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.above(BIGINT, 1L)));
        assertTrue(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testGreaterThanRange()
    {
        Range range = Range.greaterThan(BIGINT, 1L);
        assertEquals(range.getLow(), Marker.above(BIGINT, 1L));
        assertEquals(range.getHigh(), Marker.upperUnbounded(BIGINT));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertFalse(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 2L)));
        assertTrue(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testGreaterThanOrEqualRange()
    {
        Range range = Range.greaterThanOrEqual(BIGINT, 1L);
        assertEquals(range.getLow(), Marker.exactly(BIGINT, 1L));
        assertEquals(range.getHigh(), Marker.upperUnbounded(BIGINT));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertFalse(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 0L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 2L)));
        assertTrue(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testLessThanRange()
    {
        Range range = Range.lessThan(BIGINT, 1L);
        assertEquals(range.getLow(), Marker.lowerUnbounded(BIGINT));
        assertEquals(range.getHigh(), Marker.below(BIGINT, 1L));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertTrue(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 0L)));
        assertFalse(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testLessThanOrEqualRange()
    {
        Range range = Range.lessThanOrEqual(BIGINT, 1L);
        assertEquals(range.getLow(), Marker.lowerUnbounded(BIGINT));
        assertEquals(range.getHigh(), Marker.exactly(BIGINT, 1L));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertTrue(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 2L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 0L)));
        assertFalse(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testEqualRange()
    {
        Range range = Range.equal(BIGINT, 1L);
        assertEquals(range.getLow(), Marker.exactly(BIGINT, 1L));
        assertEquals(range.getHigh(), Marker.exactly(BIGINT, 1L));
        assertTrue(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertFalse(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 0L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 1L)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 2L)));
        assertFalse(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testRange()
    {
        Range range = Range.range(BIGINT, 0L, false, 2L, true);
        assertEquals(range.getLow(), Marker.above(BIGINT, 0L));
        assertEquals(range.getHigh(), Marker.exactly(BIGINT, 2L));
        assertFalse(range.isSingleValue());
        assertFalse(range.isAll());
        assertEquals(range.getType(), BIGINT);
        assertFalse(range.includes(Marker.lowerUnbounded(BIGINT)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 0L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 1L)));
        assertTrue(range.includes(Marker.exactly(BIGINT, 2L)));
        assertFalse(range.includes(Marker.exactly(BIGINT, 3L)));
        assertFalse(range.includes(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testGetSingleValue()
    {
        assertEquals(Range.equal(BIGINT, 0L).getSingleValue(), 0L);
        assertThrows(IllegalStateException.class, () -> Range.lessThan(BIGINT, 0L).getSingleValue());
    }

    @Test
    public void testContains()
    {
        assertTrue(Range.all(BIGINT).contains(Range.all(BIGINT)));
        assertTrue(Range.all(BIGINT).contains(Range.equal(BIGINT, 0L)));
        assertTrue(Range.all(BIGINT).contains(Range.greaterThan(BIGINT, 0L)));
        assertTrue(Range.equal(BIGINT, 0L).contains(Range.equal(BIGINT, 0L)));
        assertFalse(Range.equal(BIGINT, 0L).contains(Range.greaterThan(BIGINT, 0L)));
        assertFalse(Range.equal(BIGINT, 0L).contains(Range.greaterThanOrEqual(BIGINT, 0L)));
        assertFalse(Range.equal(BIGINT, 0L).contains(Range.all(BIGINT)));
        assertTrue(Range.greaterThanOrEqual(BIGINT, 0L).contains(Range.greaterThan(BIGINT, 0L)));
        assertTrue(Range.greaterThan(BIGINT, 0L).contains(Range.greaterThan(BIGINT, 1L)));
        assertFalse(Range.greaterThan(BIGINT, 0L).contains(Range.lessThan(BIGINT, 0L)));
        assertTrue(Range.range(BIGINT, 0L, true, 2L, true).contains(Range.range(BIGINT, 1L, true, 2L, true)));
        assertFalse(Range.range(BIGINT, 0L, true, 2L, true).contains(Range.range(BIGINT, 1L, true, 3L, false)));
    }

    @Test
    public void testSpan()
    {
        assertEquals(Range.greaterThan(BIGINT, 1L).span(Range.lessThanOrEqual(BIGINT, 2L)), Range.all(BIGINT));
        assertEquals(Range.greaterThan(BIGINT, 2L).span(Range.lessThanOrEqual(BIGINT, 0L)), Range.all(BIGINT));
        assertEquals(Range.range(BIGINT, 1L, true, 3L, false).span(Range.equal(BIGINT, 2L)), Range.range(BIGINT, 1L, true, 3L, false));
        assertEquals(Range.range(BIGINT, 1L, true, 3L, false).span(Range.range(BIGINT, 2L, false, 10L, false)), Range.range(BIGINT, 1L, true, 10L, false));
        assertEquals(Range.greaterThan(BIGINT, 1L).span(Range.equal(BIGINT, 0L)), Range.greaterThanOrEqual(BIGINT, 0L));
        assertEquals(Range.greaterThan(BIGINT, 1L).span(Range.greaterThanOrEqual(BIGINT, 10L)), Range.greaterThan(BIGINT, 1L));
        assertEquals(Range.lessThan(BIGINT, 1L).span(Range.lessThanOrEqual(BIGINT, 1L)), Range.lessThanOrEqual(BIGINT, 1L));
        assertEquals(Range.all(BIGINT).span(Range.lessThanOrEqual(BIGINT, 1L)), Range.all(BIGINT));
    }

    @Test
    public void testOverlaps()
    {
        assertTrue(Range.greaterThan(BIGINT, 1L).overlaps(Range.lessThanOrEqual(BIGINT, 2L)));
        assertFalse(Range.greaterThan(BIGINT, 2L).overlaps(Range.lessThan(BIGINT, 2L)));
        assertTrue(Range.range(BIGINT, 1L, true, 3L, false).overlaps(Range.equal(BIGINT, 2L)));
        assertTrue(Range.range(BIGINT, 1L, true, 3L, false).overlaps(Range.range(BIGINT, 2L, false, 10L, false)));
        assertFalse(Range.range(BIGINT, 1L, true, 3L, false).overlaps(Range.range(BIGINT, 3L, true, 10L, false)));
        assertTrue(Range.range(BIGINT, 1L, true, 3L, true).overlaps(Range.range(BIGINT, 3L, true, 10L, false)));
        assertTrue(Range.all(BIGINT).overlaps(Range.equal(BIGINT, Long.MAX_VALUE)));
    }

    @Test
    public void testIntersect()
    {
        assertEquals(Range.greaterThan(BIGINT, 1L).intersect(Range.lessThanOrEqual(BIGINT, 2L)), Range.range(BIGINT, 1L, false, 2L, true));
        assertEquals(Range.range(BIGINT, 1L, true, 3L, false).intersect(Range.equal(BIGINT, 2L)), Range.equal(BIGINT, 2L));
        assertEquals(Range.range(BIGINT, 1L, true, 3L, false).intersect(Range.range(BIGINT, 2L, false, 10L, false)), Range.range(BIGINT, 2L, false, 3L, false));
        assertEquals(Range.range(BIGINT, 1L, true, 3L, true).intersect(Range.range(BIGINT, 3L, true, 10L, false)), Range.equal(BIGINT, 3L));
        assertEquals(Range.all(BIGINT).intersect(Range.equal(BIGINT, Long.MAX_VALUE)), Range.equal(BIGINT, Long.MAX_VALUE));
    }

    @Test
    public void testExceptionalIntersect()
    {
        assertThrows(IllegalArgumentException.class, () -> Range.greaterThan(BIGINT, 2L).intersect(Range.lessThan(BIGINT, 2L)));
        assertThrows(IllegalArgumentException.class, () -> Range.range(BIGINT, 1L, true, 3L, false).intersect(Range.range(BIGINT, 3L, true, 10L, false)));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

        ObjectMapper mapper = new JsonObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        Range range = Range.all(BIGINT);
        assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));

        range = Range.equal(DOUBLE, 0.123);
        assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));

        range = Range.greaterThan(BIGINT, 0L);
        assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));

        range = Range.greaterThanOrEqual(VARCHAR, utf8Slice("abc"));
        assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));

        range = Range.lessThan(BIGINT, Long.MAX_VALUE);
        assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));

        range = Range.lessThanOrEqual(DOUBLE, Double.MAX_VALUE);
        assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));
    }

    @Test
    public void testToString()
    {
        Range range = Range.all(VARCHAR);
        assertEquals(range.toString(PROPERTIES), "(<min>, <max>)");

        range = Range.equal(VARCHAR, utf8Slice("some string"));
        assertEquals(range.toString(PROPERTIES), "[\"some string\"]");

        range = Range.equal(VARCHAR, utf8Slice("here's a quote: \""));
        assertEquals(range.toString(PROPERTIES), "[\"here's a quote: \\\"\"]");

        range = Range.equal(VARCHAR, utf8Slice(""));
        assertEquals(range.toString(PROPERTIES), "[\"\"]");

        range = Range.greaterThanOrEqual(VARCHAR, utf8Slice("string with \"quotes\" inside")).intersect(Range.lessThanOrEqual(VARCHAR, utf8Slice("this string's quote is here -> \"")));
        assertEquals(range.toString(PROPERTIES), "[\"string with \\\"quotes\\\" inside\", \"this string's quote is here -> \\\"\"]");

        range = Range.greaterThan(VARCHAR, utf8Slice("string with \"quotes\" inside")).intersect(Range.lessThan(VARCHAR, utf8Slice("this string's quote is here -> \"")));
        assertEquals(range.toString(PROPERTIES), "(\"string with \\\"quotes\\\" inside\", \"this string's quote is here -> \\\"\")");

        range = Range.equal(VARCHAR, utf8Slice("<min>"));
        assertEquals(range.toString(PROPERTIES), "[\"<min>\"]");

        range = Range.equal(VARCHAR, utf8Slice("<max>"));
        assertEquals(range.toString(PROPERTIES), "[\"<max>\"]");

        range = Range.equal(VARCHAR, utf8Slice("<min>, <max>"));
        assertEquals(range.toString(PROPERTIES), "[\"<min>, <max>\"]");

        range = Range.greaterThanOrEqual(VARCHAR, utf8Slice("a")).intersect(Range.lessThanOrEqual(VARCHAR, utf8Slice("b")));
        assertEquals(range.toString(PROPERTIES), "[\"a\", \"b\"]");

        range = Range.equal(VARCHAR, utf8Slice("a, b"));
        assertEquals(range.toString(PROPERTIES), "[\"a, b\"]");
    }
}
