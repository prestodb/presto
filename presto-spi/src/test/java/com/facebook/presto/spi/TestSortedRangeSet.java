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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Iterator;

public class TestSortedRangeSet
{
    @Test
    public void testEmptySet()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.none(Long.class);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertTrue(rangeSet.isNone());
        Assert.assertFalse(rangeSet.isAll());
        Assert.assertFalse(rangeSet.isSingleValue());
        Assert.assertTrue(Iterables.isEmpty(rangeSet));
        Assert.assertEquals(rangeSet.getRangeCount(), 0);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertEquals(rangeSet.complement(), SortedRangeSet.all(Long.class));
        Assert.assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(Long.class)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.exactly(0L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testEntireSet()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.all(Long.class);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertFalse(rangeSet.isNone());
        Assert.assertTrue(rangeSet.isAll());
        Assert.assertFalse(rangeSet.isSingleValue());
        Assert.assertEquals(rangeSet.getRangeCount(), 1);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertEquals(rangeSet.complement(), SortedRangeSet.none(Long.class));
        Assert.assertTrue(rangeSet.includesMarker(Marker.lowerUnbounded(Long.class)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(0L)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testSingleValue()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.singleValue(10L);

        SortedRangeSet complement = SortedRangeSet.of(Range.greaterThan(10L), Range.lessThan(10L));

        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertFalse(rangeSet.isNone());
        Assert.assertFalse(rangeSet.isAll());
        Assert.assertTrue(rangeSet.isSingleValue());
        Assert.assertTrue(Iterables.elementsEqual(rangeSet, ImmutableList.of(Range.equal(10L))));
        Assert.assertEquals(rangeSet.getRangeCount(), 1);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertEquals(rangeSet.complement(), complement);
        Assert.assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(Long.class)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(10L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.exactly(9L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testBoundedSet()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.equal(10L),
                Range.equal(0L),
                Range.range(9L, true, 11L, false),
                Range.equal(0L),
                Range.range(2L, true, 4L, true),
                Range.range(4L, false, 5L, true));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.equal(0L),
                Range.range(2L, true, 5L, true),
                Range.range(9L, true, 11L, false));

        SortedRangeSet complement = SortedRangeSet.of(
                Range.lessThan(0L),
                Range.range(0L, false, 2L, false),
                Range.range(5L, false, 9L, false),
                Range.greaterThanOrEqual(11L));

        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertFalse(rangeSet.isNone());
        Assert.assertFalse(rangeSet.isAll());
        Assert.assertFalse(rangeSet.isSingleValue());
        Assert.assertTrue(Iterables.elementsEqual(rangeSet, normalizedResult));
        Assert.assertEquals(rangeSet, SortedRangeSet.copyOf(Long.class, normalizedResult));
        Assert.assertEquals(rangeSet.getRangeCount(), 3);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertEquals(rangeSet.complement(), complement);
        Assert.assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(Long.class)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(0L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.exactly(1L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.exactly(7L)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(9L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testUnboundedSet()
            throws Exception
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.greaterThan(10L),
                Range.lessThanOrEqual(0L),
                Range.range(2L, true, 4L, false),
                Range.range(4L, true, 6L, false),
                Range.range(1L, false, 2L, false),
                Range.range(9L, false, 11L, false));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.lessThanOrEqual(0L),
                Range.range(1L, false, 6L, false),
                Range.greaterThan(9L));

        SortedRangeSet complement = SortedRangeSet.of(
                Range.range(0L, false, 1L, true),
                Range.range(6L, true, 9L, true));

        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertFalse(rangeSet.isNone());
        Assert.assertFalse(rangeSet.isAll());
        Assert.assertFalse(rangeSet.isSingleValue());
        Assert.assertTrue(Iterables.elementsEqual(rangeSet, normalizedResult));
        Assert.assertEquals(rangeSet, SortedRangeSet.copyOf(Long.class, normalizedResult));
        Assert.assertEquals(rangeSet.getRangeCount(), 3);
        Assert.assertEquals(rangeSet.getType(), Long.class);
        Assert.assertEquals(rangeSet.complement(), complement);
        Assert.assertTrue(rangeSet.includesMarker(Marker.lowerUnbounded(Long.class)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(0L)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.exactly(4L)));
        Assert.assertFalse(rangeSet.includesMarker(Marker.exactly(7L)));
        Assert.assertTrue(rangeSet.includesMarker(Marker.upperUnbounded(Long.class)));
    }

    @Test
    public void testGetSingleValue()
            throws Exception
    {
        Assert.assertEquals(SortedRangeSet.singleValue(0L).getSingleValue(), 0L);
        try {
            SortedRangeSet.all(Long.class).getSingleValue();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test
    public void testSpan()
            throws Exception
    {
        try {
            SortedRangeSet.none(Long.class).getSpan();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }

        Assert.assertEquals(SortedRangeSet.all(Long.class).getSpan(), Range.all(Long.class));
        Assert.assertEquals(SortedRangeSet.singleValue(0L).getSpan(), Range.equal(0L));
        Assert.assertEquals(SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).getSpan(), Range.range(0L, true, 1L, true));
        Assert.assertEquals(SortedRangeSet.of(Range.equal(0L), Range.greaterThan(1L)).getSpan(), Range.greaterThanOrEqual(0L));
        Assert.assertEquals(SortedRangeSet.of(Range.lessThan(0L), Range.greaterThan(1L)).getSpan(), Range.all(Long.class));
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        Assert.assertTrue(SortedRangeSet.all(Long.class).overlaps(SortedRangeSet.all(Long.class)));
        Assert.assertFalse(SortedRangeSet.all(Long.class).overlaps(SortedRangeSet.none(Long.class)));
        Assert.assertTrue(SortedRangeSet.all(Long.class).overlaps(SortedRangeSet.singleValue(0L)));
        Assert.assertTrue(SortedRangeSet.all(Long.class).overlaps(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))));
        Assert.assertTrue(SortedRangeSet.all(Long.class).overlaps(SortedRangeSet.of(Range.greaterThan(0L))));
        Assert.assertTrue(SortedRangeSet.all(Long.class).overlaps(SortedRangeSet.of(Range.greaterThan(0L), Range.lessThan(0L))));

        Assert.assertFalse(SortedRangeSet.none(Long.class).overlaps(SortedRangeSet.all(Long.class)));
        Assert.assertFalse(SortedRangeSet.none(Long.class).overlaps(SortedRangeSet.none(Long.class)));
        Assert.assertFalse(SortedRangeSet.none(Long.class).overlaps(SortedRangeSet.singleValue(0L)));
        Assert.assertFalse(SortedRangeSet.none(Long.class).overlaps(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))));
        Assert.assertFalse(SortedRangeSet.none(Long.class).overlaps(SortedRangeSet.of(Range.greaterThan(0L))));
        Assert.assertFalse(SortedRangeSet.none(Long.class).overlaps(SortedRangeSet.of(Range.greaterThan(0L), Range.lessThan(0L))));

        Assert.assertTrue(SortedRangeSet.singleValue(0L).overlaps(SortedRangeSet.all(Long.class)));
        Assert.assertFalse(SortedRangeSet.singleValue(0L).overlaps(SortedRangeSet.none(Long.class)));
        Assert.assertTrue(SortedRangeSet.singleValue(0L).overlaps(SortedRangeSet.singleValue(0L)));
        Assert.assertTrue(SortedRangeSet.singleValue(0L).overlaps(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))));
        Assert.assertFalse(SortedRangeSet.singleValue(0L).overlaps(SortedRangeSet.of(Range.greaterThan(0L))));
        Assert.assertFalse(SortedRangeSet.singleValue(0L).overlaps(SortedRangeSet.of(Range.greaterThan(0L), Range.lessThan(0L))));

        Assert.assertTrue(SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).overlaps(SortedRangeSet.of(Range.equal(1L))));
        Assert.assertFalse(SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).overlaps(SortedRangeSet.of(Range.equal(2L))));
        Assert.assertTrue(SortedRangeSet.of(Range.greaterThanOrEqual(0L)).overlaps(SortedRangeSet.of(Range.greaterThan(0L))));
        Assert.assertTrue(SortedRangeSet.of(Range.greaterThan(0L)).overlaps(SortedRangeSet.of(Range.greaterThanOrEqual(0L))));
        Assert.assertFalse(SortedRangeSet.of(Range.lessThan(0L)).overlaps(SortedRangeSet.of(Range.greaterThan(0L))));
    }

    @Test
    public void testContains()
            throws Exception
    {
        Assert.assertTrue(SortedRangeSet.all(Long.class).contains(SortedRangeSet.all(Long.class)));
        Assert.assertTrue(SortedRangeSet.all(Long.class).contains(SortedRangeSet.none(Long.class)));
        Assert.assertTrue(SortedRangeSet.all(Long.class).contains(SortedRangeSet.singleValue(0L)));
        Assert.assertTrue(SortedRangeSet.all(Long.class).contains(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))));
        Assert.assertTrue(SortedRangeSet.all(Long.class).contains(SortedRangeSet.of(Range.greaterThan(0L))));
        Assert.assertTrue(SortedRangeSet.all(Long.class).contains(SortedRangeSet.of(Range.greaterThan(0L), Range.lessThan(0L))));

        Assert.assertFalse(SortedRangeSet.none(Long.class).contains(SortedRangeSet.all(Long.class)));
        Assert.assertTrue(SortedRangeSet.none(Long.class).contains(SortedRangeSet.none(Long.class)));
        Assert.assertFalse(SortedRangeSet.none(Long.class).contains(SortedRangeSet.singleValue(0L)));
        Assert.assertFalse(SortedRangeSet.none(Long.class).contains(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))));
        Assert.assertFalse(SortedRangeSet.none(Long.class).contains(SortedRangeSet.of(Range.greaterThan(0L))));
        Assert.assertFalse(SortedRangeSet.none(Long.class).contains(SortedRangeSet.of(Range.greaterThan(0L), Range.lessThan(0L))));

        Assert.assertFalse(SortedRangeSet.singleValue(0L).contains(SortedRangeSet.all(Long.class)));
        Assert.assertTrue(SortedRangeSet.singleValue(0L).contains(SortedRangeSet.none(Long.class)));
        Assert.assertTrue(SortedRangeSet.singleValue(0L).contains(SortedRangeSet.singleValue(0L)));
        Assert.assertFalse(SortedRangeSet.singleValue(0L).contains(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))));
        Assert.assertFalse(SortedRangeSet.singleValue(0L).contains(SortedRangeSet.of(Range.greaterThan(0L))));
        Assert.assertFalse(SortedRangeSet.singleValue(0L).contains(SortedRangeSet.of(Range.greaterThan(0L), Range.lessThan(0L))));

        Assert.assertTrue(SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).contains(SortedRangeSet.of(Range.equal(1L))));
        Assert.assertFalse(SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).contains(SortedRangeSet.of(Range.equal(1L), Range.equal(2L))));
        Assert.assertTrue(SortedRangeSet.of(Range.greaterThanOrEqual(0L)).contains(SortedRangeSet.of(Range.greaterThan(0L))));
        Assert.assertFalse(SortedRangeSet.of(Range.greaterThan(0L)).contains(SortedRangeSet.of(Range.greaterThanOrEqual(0L))));
        Assert.assertFalse(SortedRangeSet.of(Range.lessThan(0L)).contains(SortedRangeSet.of(Range.greaterThan(0L))));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        Assert.assertEquals(
                SortedRangeSet.none(Long.class).intersect(
                        SortedRangeSet.none(Long.class)),
                SortedRangeSet.none(Long.class));

        Assert.assertEquals(
                SortedRangeSet.all(Long.class).intersect(
                        SortedRangeSet.all(Long.class)),
                SortedRangeSet.all(Long.class));

        Assert.assertEquals(
                SortedRangeSet.none(Long.class).intersect(
                        SortedRangeSet.all(Long.class)),
                SortedRangeSet.none(Long.class));

        Assert.assertEquals(
                SortedRangeSet.of(Range.equal(1L), Range.equal(2L), Range.equal(3L)).intersect(
                        SortedRangeSet.of(Range.equal(2L), Range.equal(4L))),
                SortedRangeSet.of(Range.equal(2L)));

        Assert.assertEquals(
                SortedRangeSet.all(Long.class).intersect(
                        SortedRangeSet.of(Range.equal(2L), Range.equal(4L))),
                SortedRangeSet.of(Range.equal(2L), Range.equal(4L)));

        Assert.assertEquals(
                SortedRangeSet.of(Range.range(0L, true, 4L, false)).intersect(
                        SortedRangeSet.of(Range.equal(2L), Range.greaterThan(3L))),
                SortedRangeSet.of(Range.equal(2L), Range.range(3L, false, 4L, false)));

        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(0L)).intersect(
                        SortedRangeSet.of(Range.lessThanOrEqual(0L))),
                SortedRangeSet.of(Range.equal(0L)));

        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(-1L)).intersect(
                        SortedRangeSet.of(Range.lessThanOrEqual(1L))),
                SortedRangeSet.of(Range.range(-1L, true, 1L, true)));
    }

    @Test
    public void testUnion()
            throws Exception
    {
        Assert.assertEquals(
                SortedRangeSet.none(Long.class).union(
                        SortedRangeSet.none(Long.class)),
                SortedRangeSet.none(Long.class));

        Assert.assertEquals(
                SortedRangeSet.all(Long.class).union(
                        SortedRangeSet.all(Long.class)),
                SortedRangeSet.all(Long.class));

        Assert.assertEquals(
                SortedRangeSet.none(Long.class).union(
                        SortedRangeSet.all(Long.class)),
                SortedRangeSet.all(Long.class));

        Assert.assertEquals(
                SortedRangeSet.of(Range.equal(1L), Range.equal(2L)).union(
                        SortedRangeSet.of(Range.equal(2L), Range.equal(3L))),
                SortedRangeSet.of(Range.equal(1L), Range.equal(2L), Range.equal(3L)));

        Assert.assertEquals(
                SortedRangeSet.all(Long.class).union(
                        SortedRangeSet.of(Range.equal(0L))),
                SortedRangeSet.all(Long.class));

        Assert.assertEquals(
                SortedRangeSet.of(Range.range(0L, true, 4L, false)).union(
                        SortedRangeSet.of(Range.greaterThan(3L))),
                SortedRangeSet.of(Range.greaterThanOrEqual(0L)));

        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(0L)).union(
                        SortedRangeSet.of(Range.lessThanOrEqual(0L))),
                SortedRangeSet.of(Range.all(Long.class)));

        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThan(0L)).union(
                        SortedRangeSet.of(Range.lessThan(0L))),
                SortedRangeSet.singleValue(0L).complement());
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        Assert.assertEquals(
                SortedRangeSet.all(Long.class).subtract(SortedRangeSet.all(Long.class)),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.all(Long.class).subtract(SortedRangeSet.none(Long.class)),
                SortedRangeSet.all(Long.class));
        Assert.assertEquals(
                SortedRangeSet.all(Long.class).subtract(SortedRangeSet.singleValue(0L)),
                SortedRangeSet.singleValue(0L).complement());
        Assert.assertEquals(
                SortedRangeSet.all(Long.class).subtract(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))),
                SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).complement());
        Assert.assertEquals(
                SortedRangeSet.all(Long.class).subtract(SortedRangeSet.of(Range.greaterThan(0L))),
                SortedRangeSet.of(Range.lessThanOrEqual(0L)));

        Assert.assertEquals(
                SortedRangeSet.none(Long.class).subtract(SortedRangeSet.all(Long.class)),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.none(Long.class).subtract(SortedRangeSet.none(Long.class)),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.none(Long.class).subtract(SortedRangeSet.singleValue(0L)),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.none(Long.class).subtract(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.none(Long.class).subtract(SortedRangeSet.of(Range.greaterThan(0L))),
                SortedRangeSet.none(Long.class));

        Assert.assertEquals(
                SortedRangeSet.singleValue(0L).subtract(SortedRangeSet.all(Long.class)),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.singleValue(0L).subtract(SortedRangeSet.none(Long.class)),
                SortedRangeSet.singleValue(0L));
        Assert.assertEquals(
                SortedRangeSet.singleValue(0L).subtract(SortedRangeSet.singleValue(0L)),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.singleValue(0L).subtract(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.singleValue(0L).subtract(SortedRangeSet.of(Range.greaterThan(0L))),
                SortedRangeSet.singleValue(0L));

        Assert.assertEquals(
                SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).subtract(SortedRangeSet.all(Long.class)),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).subtract(SortedRangeSet.none(Long.class)),
                SortedRangeSet.of(Range.equal(0L), Range.equal(1L)));
        Assert.assertEquals(
                SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).subtract(SortedRangeSet.singleValue(0L)),
                SortedRangeSet.singleValue(1L));
        Assert.assertEquals(
                SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).subtract(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.of(Range.equal(0L), Range.equal(1L)).subtract(SortedRangeSet.of(Range.greaterThan(0L))),
                SortedRangeSet.of(Range.equal(0L)));

        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThan(0L)).subtract(SortedRangeSet.all(Long.class)),
                SortedRangeSet.none(Long.class));
        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThan(0L)).subtract(SortedRangeSet.none(Long.class)),
                SortedRangeSet.of(Range.greaterThan(0L)));
        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThan(0L)).subtract(SortedRangeSet.singleValue(0L)),
                SortedRangeSet.of(Range.greaterThan(0L)));
        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThan(0L)).subtract(SortedRangeSet.of(Range.equal(0L), Range.equal(1L))),
                SortedRangeSet.of(Range.range(0L, false, 1L, false), Range.greaterThan(1L)));
        Assert.assertEquals(
                SortedRangeSet.of(Range.greaterThan(0L)).subtract(SortedRangeSet.of(Range.greaterThan(0L))),
                SortedRangeSet.none(Long.class));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnmodifiableIterator()
            throws Exception
    {
        Iterator<Range> iterator = SortedRangeSet.of(Range.equal(1L)).iterator();
        iterator.next();
        iterator.remove();
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();

        SortedRangeSet set = SortedRangeSet.all(Long.class);
        Assert.assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.none(Double.class);
        Assert.assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.singleValue("abc");
        Assert.assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.of(Range.equal(true), Range.equal(false));
        Assert.assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));
    }
}
