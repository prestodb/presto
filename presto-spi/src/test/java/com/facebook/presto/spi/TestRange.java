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
package com.facebook.presto.spi;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRange
{
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMismatchedTypes()
            throws Exception
    {
        // NEVER DO THIS
        new Range(Marker.exactly(1L), Marker.exactly("a"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvertedBounds()
            throws Exception
    {
        new Range(Marker.exactly(1L), Marker.exactly(0L));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLowerUnboundedOnly()
            throws Exception
    {
        new Range(Marker.lowerUnbounded(Long.class), Marker.lowerUnbounded(Long.class));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUpperUnboundedOnly()
            throws Exception
    {
        new Range(Marker.upperUnbounded(Long.class), Marker.upperUnbounded(Long.class));
    }

    @Test
    public void testAllRange()
            throws Exception
    {
        Range range = Range.all(Long.class);
        Assert.assertEquals(range.getLow(), Marker.lowerUnbounded(Long.class));
        Assert.assertEquals(range.getHigh(), Marker.upperUnbounded(Long.class));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertTrue(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertTrue(range.includes(Marker.lowerUnbounded(Long.class)));
        Assert.assertTrue(range.includes(Marker.below(1L)));
        Assert.assertTrue(range.includes(Marker.exactly(1L)));
        Assert.assertTrue(range.includes(Marker.above(1L)));
        Assert.assertTrue(range.includes(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testGreaterThanRange()
            throws Exception
    {
        Range range = Range.greaterThan(1L);
        Assert.assertEquals(range.getLow(), Marker.above(1L));
        Assert.assertEquals(range.getHigh(), Marker.upperUnbounded(Long.class));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertFalse(range.includes(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.includes(Marker.exactly(1L)));
        Assert.assertTrue(range.includes(Marker.exactly(2L)));
        Assert.assertTrue(range.includes(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testGreaterThanOrEqualRange()
            throws Exception
    {
        Range range = Range.greaterThanOrEqual(1L);
        Assert.assertEquals(range.getLow(), Marker.exactly(1L));
        Assert.assertEquals(range.getHigh(), Marker.upperUnbounded(Long.class));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertFalse(range.includes(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.includes(Marker.exactly(0L)));
        Assert.assertTrue(range.includes(Marker.exactly(1L)));
        Assert.assertTrue(range.includes(Marker.exactly(2L)));
        Assert.assertTrue(range.includes(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testLessThanRange()
            throws Exception
    {
        Range range = Range.lessThan(1L);
        Assert.assertEquals(range.getLow(), Marker.lowerUnbounded(Long.class));
        Assert.assertEquals(range.getHigh(), Marker.below(1L));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertTrue(range.includes(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.includes(Marker.exactly(1L)));
        Assert.assertTrue(range.includes(Marker.exactly(0L)));
        Assert.assertFalse(range.includes(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testLessThanOrEqualRange()
            throws Exception
    {
        Range range = Range.lessThanOrEqual(1L);
        Assert.assertEquals(range.getLow(), Marker.lowerUnbounded(Long.class));
        Assert.assertEquals(range.getHigh(), Marker.exactly(1L));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertTrue(range.includes(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.includes(Marker.exactly(2L)));
        Assert.assertTrue(range.includes(Marker.exactly(1L)));
        Assert.assertTrue(range.includes(Marker.exactly(0L)));
        Assert.assertFalse(range.includes(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testEqualRange()
            throws Exception
    {
        Range range = Range.equal(1L);
        Assert.assertEquals(range.getLow(), Marker.exactly(1L));
        Assert.assertEquals(range.getHigh(), Marker.exactly(1L));
        Assert.assertTrue(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertFalse(range.includes(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.includes(Marker.exactly(0L)));
        Assert.assertTrue(range.includes(Marker.exactly(1L)));
        Assert.assertFalse(range.includes(Marker.exactly(2L)));
        Assert.assertFalse(range.includes(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testRange()
            throws Exception
    {
        Range range = Range.range(0L, false, 2L, true);
        Assert.assertEquals(range.getLow(), Marker.above(0L));
        Assert.assertEquals(range.getHigh(), Marker.exactly(2L));
        Assert.assertFalse(range.isSingleValue());
        Assert.assertFalse(range.isAll());
        Assert.assertEquals(range.getType(), Long.class);
        Assert.assertFalse(range.includes(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(range.includes(Marker.exactly(0L)));
        Assert.assertTrue(range.includes(Marker.exactly(1L)));
        Assert.assertTrue(range.includes(Marker.exactly(2L)));
        Assert.assertFalse(range.includes(Marker.exactly(3L)));
        Assert.assertFalse(range.includes(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testGetSingleValue()
            throws Exception
    {
        Assert.assertEquals(Range.equal(0L).getSingleValue(), 0L);
        try {
            Range.lessThan(0L).getSingleValue();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test
    public void testContains()
            throws Exception
    {
        Assert.assertTrue(Range.all(Long.class).contains(Range.all(Long.class)));
        Assert.assertTrue(Range.all(Long.class).contains(Range.equal(0L)));
        Assert.assertTrue(Range.all(Long.class).contains(Range.greaterThan(0L)));
        Assert.assertTrue(Range.equal(0L).contains(Range.equal(0L)));
        Assert.assertFalse(Range.equal(0L).contains(Range.greaterThan(0L)));
        Assert.assertFalse(Range.equal(0L).contains(Range.greaterThanOrEqual(0L)));
        Assert.assertFalse(Range.equal(0L).contains(Range.all(Long.class)));
        Assert.assertTrue(Range.greaterThanOrEqual(0L).contains(Range.greaterThan(0L)));
        Assert.assertTrue(Range.greaterThan(0L).contains(Range.greaterThan(1L)));
        Assert.assertFalse(Range.greaterThan(0L).contains(Range.lessThan(0L)));
        Assert.assertTrue(Range.range(0, true, 2, true).contains(Range.range(1, true, 2, true)));
        Assert.assertFalse(Range.range(0, true, 2, true).contains(Range.range(1, true, 3, false)));
    }

    @Test
    public void testSpan()
            throws Exception
    {
        Assert.assertEquals(Range.greaterThan(1L).span(Range.lessThanOrEqual(2L)), Range.all(Long.class));
        Assert.assertEquals(Range.greaterThan(2L).span(Range.lessThanOrEqual(0L)), Range.all(Long.class));
        Assert.assertEquals(Range.range(1L, true, 3L, false).span(Range.equal(2L)), Range.range(1L, true, 3L, false));
        Assert.assertEquals(Range.range(1L, true, 3L, false).span(Range.range(2L, false, 10L, false)), Range.range(1L, true, 10L, false));
        Assert.assertEquals(Range.greaterThan(1L).span(Range.equal(0L)), Range.greaterThanOrEqual(0L));
        Assert.assertEquals(Range.greaterThan(1L).span(Range.greaterThanOrEqual(10L)), Range.greaterThan(1L));
        Assert.assertEquals(Range.lessThan(1L).span(Range.lessThanOrEqual(1L)), Range.lessThanOrEqual(1L));
        Assert.assertEquals(Range.all(Long.class).span(Range.lessThanOrEqual(1L)), Range.all(Long.class));
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        Assert.assertTrue(Range.greaterThan(1L).overlaps(Range.lessThanOrEqual(2L)));
        Assert.assertFalse(Range.greaterThan(2L).overlaps(Range.lessThan(2L)));
        Assert.assertTrue(Range.range(1L, true, 3L, false).overlaps(Range.equal(2L)));
        Assert.assertTrue(Range.range(1L, true, 3L, false).overlaps(Range.range(2L, false, 10L, false)));
        Assert.assertFalse(Range.range(1L, true, 3L, false).overlaps(Range.range(3L, true, 10L, false)));
        Assert.assertTrue(Range.range(1L, true, 3L, true).overlaps(Range.range(3L, true, 10L, false)));
        Assert.assertTrue(Range.all(Long.class).overlaps(Range.equal(Long.MAX_VALUE)));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        Assert.assertEquals(Range.greaterThan(1L).intersect(Range.lessThanOrEqual(2L)), Range.range(1L, false, 2L, true));
        Assert.assertEquals(Range.range(1L, true, 3L, false).intersect(Range.equal(2L)), Range.equal(2L));
        Assert.assertEquals(Range.range(1L, true, 3L, false).intersect(Range.range(2L, false, 10L, false)), Range.range(2L, false, 3L, false));
        Assert.assertEquals(Range.range(1L, true, 3L, true).intersect(Range.range(3L, true, 10L, false)), Range.equal(3L));
        Assert.assertEquals(Range.all(Long.class).intersect(Range.equal(Long.MAX_VALUE)), Range.equal(Long.MAX_VALUE));
    }

    @Test
    public void testExceptionalIntersect()
            throws Exception
    {
        try {
            Range.greaterThan(2L).intersect(Range.lessThan(2L));
            Assert.fail();
        }
        catch (IllegalArgumentException e) {
        }

        try {
            Range.range(1L, true, 3L, false).intersect(Range.range(3L, true, 10L, false));
            Assert.fail();
        }
        catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();

        Range range = Range.all(Long.class);
        Assert.assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));

        range = Range.equal(0.123);
        Assert.assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));

        range = Range.greaterThan(0L);
        Assert.assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));

        range = Range.greaterThanOrEqual("abc");
        Assert.assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));

        range = Range.lessThan(Long.MAX_VALUE);
        Assert.assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));

        range = Range.lessThanOrEqual(Double.MAX_VALUE);
        Assert.assertEquals(range, mapper.readValue(mapper.writeValueAsString(range), Range.class));
    }
}
