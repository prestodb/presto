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

public class TestDomain
{
    @Test
    public void testNone()
            throws Exception
    {
        Domain domain = Domain.none(Long.class);
        Assert.assertTrue(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getRanges(), SortedRangeSet.none(Long.class));
        Assert.assertEquals(domain.getType(), Long.class);
        Assert.assertFalse(domain.includesValue(Long.MIN_VALUE));
        Assert.assertFalse(domain.includesValue(0L));
        Assert.assertFalse(domain.includesValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.all(Long.class));
    }

    @Test
    public void testAll()
            throws Exception
    {
        Domain domain = Domain.all(Long.class);
        Assert.assertFalse(domain.isNone());
        Assert.assertTrue(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertTrue(domain.isNullAllowed());
        Assert.assertEquals(domain.getRanges(), SortedRangeSet.all(Long.class));
        Assert.assertEquals(domain.getType(), Long.class);
        Assert.assertTrue(domain.includesValue(Long.MIN_VALUE));
        Assert.assertTrue(domain.includesValue(0L));
        Assert.assertTrue(domain.includesValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.none(Long.class));
    }

    @Test
    public void testNullOnly()
            throws Exception
    {
        Domain domain = Domain.onlyNull(Long.class);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertTrue(domain.isNullAllowed());
        Assert.assertEquals(domain.getRanges(), SortedRangeSet.none(Long.class));
        Assert.assertEquals(domain.getType(), Long.class);
        Assert.assertFalse(domain.includesValue(Long.MIN_VALUE));
        Assert.assertFalse(domain.includesValue(0L));
        Assert.assertFalse(domain.includesValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.notNull(Long.class));
    }

    @Test
    public void testNotNull()
            throws Exception
    {
        Domain domain = Domain.notNull(Long.class);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertFalse(domain.isSingleValue());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getRanges(), SortedRangeSet.all(Long.class));
        Assert.assertEquals(domain.getType(), Long.class);
        Assert.assertTrue(domain.includesValue(Long.MIN_VALUE));
        Assert.assertTrue(domain.includesValue(0L));
        Assert.assertTrue(domain.includesValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.onlyNull(Long.class));
    }

    @Test
    public void testSingleValue()
            throws Exception
    {
        Domain domain = Domain.singleValue(0L);
        Assert.assertFalse(domain.isNone());
        Assert.assertFalse(domain.isAll());
        Assert.assertTrue(domain.isSingleValue());
        Assert.assertFalse(domain.isNullAllowed());
        Assert.assertEquals(domain.getRanges(), SortedRangeSet.of(Range.equal(0L)));
        Assert.assertEquals(domain.getType(), Long.class);
        Assert.assertFalse(domain.includesValue(Long.MIN_VALUE));
        Assert.assertTrue(domain.includesValue(0L));
        Assert.assertFalse(domain.includesValue(Long.MAX_VALUE));
        Assert.assertEquals(domain.complement(), Domain.create(SortedRangeSet.of(Range.lessThan(0L), Range.greaterThan(0L)), true));
        Assert.assertEquals(domain.getSingleValue(), 0L);

        try {
            Domain.create(SortedRangeSet.of(Range.range(1, true, 2, true)), false).getSingleValue();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test
    public void testOverlaps()
            throws Exception
    {
        Assert.assertTrue(Domain.all(Long.class).overlaps(Domain.all(Long.class)));
        Assert.assertFalse(Domain.all(Long.class).overlaps(Domain.none(Long.class)));
        Assert.assertTrue(Domain.all(Long.class).overlaps(Domain.notNull(Long.class)));
        Assert.assertTrue(Domain.all(Long.class).overlaps(Domain.onlyNull(Long.class)));
        Assert.assertTrue(Domain.all(Long.class).overlaps(Domain.singleValue(0L)));

        Assert.assertFalse(Domain.none(Long.class).overlaps(Domain.all(Long.class)));
        Assert.assertFalse(Domain.none(Long.class).overlaps(Domain.none(Long.class)));
        Assert.assertFalse(Domain.none(Long.class).overlaps(Domain.notNull(Long.class)));
        Assert.assertFalse(Domain.none(Long.class).overlaps(Domain.onlyNull(Long.class)));
        Assert.assertFalse(Domain.none(Long.class).overlaps(Domain.singleValue(0L)));

        Assert.assertTrue(Domain.notNull(Long.class).overlaps(Domain.all(Long.class)));
        Assert.assertFalse(Domain.notNull(Long.class).overlaps(Domain.none(Long.class)));
        Assert.assertTrue(Domain.notNull(Long.class).overlaps(Domain.notNull(Long.class)));
        Assert.assertFalse(Domain.notNull(Long.class).overlaps(Domain.onlyNull(Long.class)));
        Assert.assertTrue(Domain.notNull(Long.class).overlaps(Domain.singleValue(0L)));

        Assert.assertTrue(Domain.onlyNull(Long.class).overlaps(Domain.all(Long.class)));
        Assert.assertFalse(Domain.onlyNull(Long.class).overlaps(Domain.none(Long.class)));
        Assert.assertFalse(Domain.onlyNull(Long.class).overlaps(Domain.notNull(Long.class)));
        Assert.assertTrue(Domain.onlyNull(Long.class).overlaps(Domain.onlyNull(Long.class)));
        Assert.assertFalse(Domain.onlyNull(Long.class).overlaps(Domain.singleValue(0L)));

        Assert.assertTrue(Domain.singleValue(0L).overlaps(Domain.all(Long.class)));
        Assert.assertFalse(Domain.singleValue(0L).overlaps(Domain.none(Long.class)));
        Assert.assertTrue(Domain.singleValue(0L).overlaps(Domain.notNull(Long.class)));
        Assert.assertFalse(Domain.singleValue(0L).overlaps(Domain.onlyNull(Long.class)));
        Assert.assertTrue(Domain.singleValue(0L).overlaps(Domain.singleValue(0L)));
    }

    @Test
    public void testContains()
            throws Exception
    {
        Assert.assertTrue(Domain.all(Long.class).contains(Domain.all(Long.class)));
        Assert.assertTrue(Domain.all(Long.class).contains(Domain.none(Long.class)));
        Assert.assertTrue(Domain.all(Long.class).contains(Domain.notNull(Long.class)));
        Assert.assertTrue(Domain.all(Long.class).contains(Domain.onlyNull(Long.class)));
        Assert.assertTrue(Domain.all(Long.class).contains(Domain.singleValue(0L)));

        Assert.assertFalse(Domain.none(Long.class).contains(Domain.all(Long.class)));
        Assert.assertTrue(Domain.none(Long.class).contains(Domain.none(Long.class)));
        Assert.assertFalse(Domain.none(Long.class).contains(Domain.notNull(Long.class)));
        Assert.assertFalse(Domain.none(Long.class).contains(Domain.onlyNull(Long.class)));
        Assert.assertFalse(Domain.none(Long.class).contains(Domain.singleValue(0L)));

        Assert.assertFalse(Domain.notNull(Long.class).contains(Domain.all(Long.class)));
        Assert.assertTrue(Domain.notNull(Long.class).contains(Domain.none(Long.class)));
        Assert.assertTrue(Domain.notNull(Long.class).contains(Domain.notNull(Long.class)));
        Assert.assertFalse(Domain.notNull(Long.class).contains(Domain.onlyNull(Long.class)));
        Assert.assertTrue(Domain.notNull(Long.class).contains(Domain.singleValue(0L)));

        Assert.assertFalse(Domain.onlyNull(Long.class).contains(Domain.all(Long.class)));
        Assert.assertTrue(Domain.onlyNull(Long.class).contains(Domain.none(Long.class)));
        Assert.assertFalse(Domain.onlyNull(Long.class).contains(Domain.notNull(Long.class)));
        Assert.assertTrue(Domain.onlyNull(Long.class).contains(Domain.onlyNull(Long.class)));
        Assert.assertFalse(Domain.onlyNull(Long.class).contains(Domain.singleValue(0L)));

        Assert.assertFalse(Domain.singleValue(0L).contains(Domain.all(Long.class)));
        Assert.assertTrue(Domain.singleValue(0L).contains(Domain.none(Long.class)));
        Assert.assertFalse(Domain.singleValue(0L).contains(Domain.notNull(Long.class)));
        Assert.assertFalse(Domain.singleValue(0L).contains(Domain.onlyNull(Long.class)));
        Assert.assertTrue(Domain.singleValue(0L).contains(Domain.singleValue(0L)));
    }

    @Test
    public void testIntersect()
            throws Exception
    {
        Assert.assertEquals(
                Domain.all(Long.class).intersect(Domain.all(Long.class)),
                Domain.all(Long.class));

        Assert.assertEquals(
                Domain.none(Long.class).intersect(Domain.none(Long.class)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.all(Long.class).intersect(Domain.none(Long.class)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.notNull(Long.class).intersect(Domain.onlyNull(Long.class)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.singleValue(0L).intersect(Domain.all(Long.class)),
                Domain.singleValue(0L));

        Assert.assertEquals(
                Domain.singleValue(0L).intersect(Domain.onlyNull(Long.class)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.create(SortedRangeSet.of(Range.equal(1L)), true).intersect(Domain.create(SortedRangeSet.of(Range.equal(2L)), true)),
                Domain.onlyNull(Long.class));

        Assert.assertEquals(
                Domain.create(SortedRangeSet.of(Range.equal(1L)), true).intersect(Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false)),
                Domain.singleValue(1L));
    }

    @Test
    public void testUnion()
            throws Exception
    {
        Assert.assertEquals(
                Domain.all(Long.class).union(Domain.all(Long.class)),
                Domain.all(Long.class));

        Assert.assertEquals(
                Domain.none(Long.class).union(Domain.none(Long.class)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.all(Long.class).union(Domain.none(Long.class)),
                Domain.all(Long.class));

        Assert.assertEquals(
                Domain.notNull(Long.class).union(Domain.onlyNull(Long.class)),
                Domain.all(Long.class));

        Assert.assertEquals(
                Domain.singleValue(0L).union(Domain.all(Long.class)),
                Domain.all(Long.class));

        Assert.assertEquals(
                  Domain.singleValue(0L).union(Domain.notNull(Long.class)),
                  Domain.notNull(Long.class));

        Assert.assertEquals(
                Domain.singleValue(0L).union(Domain.onlyNull(Long.class)),
                Domain.create(SortedRangeSet.of(Range.equal(0L)), true));

        Assert.assertEquals(
                Domain.create(SortedRangeSet.of(Range.equal(1L)), true).union(Domain.create(SortedRangeSet.of(Range.equal(2L)), true)),
                Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), true));

        Assert.assertEquals(
                Domain.create(SortedRangeSet.of(Range.equal(1L)), true).union(Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false)),
                Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), true));
    }

    @Test
    public void testSubtract()
            throws Exception
    {
        Assert.assertEquals(
                Domain.all(Long.class).subtract(Domain.all(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.all(Long.class).subtract(Domain.none(Long.class)),
                Domain.all(Long.class));
        Assert.assertEquals(
                Domain.all(Long.class).subtract(Domain.notNull(Long.class)),
                Domain.onlyNull(Long.class));
        Assert.assertEquals(
                Domain.all(Long.class).subtract(Domain.onlyNull(Long.class)),
                Domain.notNull(Long.class));
        Assert.assertEquals(
                Domain.all(Long.class).subtract(Domain.singleValue(0L)),
                Domain.create(SortedRangeSet.of(Range.lessThan(0L), Range.greaterThan(0L)), true));

        Assert.assertEquals(
                Domain.none(Long.class).subtract(Domain.all(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.none(Long.class).subtract(Domain.none(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.none(Long.class).subtract(Domain.notNull(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.none(Long.class).subtract(Domain.onlyNull(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.none(Long.class).subtract(Domain.singleValue(0L)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.notNull(Long.class).subtract(Domain.all(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.notNull(Long.class).subtract(Domain.none(Long.class)),
                Domain.notNull(Long.class));
        Assert.assertEquals(
                Domain.notNull(Long.class).subtract(Domain.notNull(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.notNull(Long.class).subtract(Domain.onlyNull(Long.class)),
                Domain.notNull(Long.class));
        Assert.assertEquals(
                Domain.notNull(Long.class).subtract(Domain.singleValue(0L)),
                Domain.create(SortedRangeSet.of(Range.lessThan(0L), Range.greaterThan(0L)), false));

        Assert.assertEquals(
                Domain.onlyNull(Long.class).subtract(Domain.all(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.onlyNull(Long.class).subtract(Domain.none(Long.class)),
                Domain.onlyNull(Long.class));
        Assert.assertEquals(
                Domain.onlyNull(Long.class).subtract(Domain.notNull(Long.class)),
                Domain.onlyNull(Long.class));
        Assert.assertEquals(
                Domain.onlyNull(Long.class).subtract(Domain.onlyNull(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.onlyNull(Long.class).subtract(Domain.singleValue(0L)),
                Domain.onlyNull(Long.class));

        Assert.assertEquals(
                Domain.singleValue(0L).subtract(Domain.all(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.singleValue(0L).subtract(Domain.none(Long.class)),
                Domain.singleValue(0L));
        Assert.assertEquals(
                Domain.singleValue(0L).subtract(Domain.notNull(Long.class)),
                Domain.none(Long.class));
        Assert.assertEquals(
                Domain.singleValue(0L).subtract(Domain.onlyNull(Long.class)),
                Domain.singleValue(0L));
        Assert.assertEquals(
                Domain.singleValue(0L).subtract(Domain.singleValue(0L)),
                Domain.none(Long.class));

        Assert.assertEquals(
                Domain.create(SortedRangeSet.of(Range.equal(1L)), true).subtract(Domain.create(SortedRangeSet.of(Range.equal(2L)), true)),
                Domain.singleValue(1L));

        Assert.assertEquals(
                Domain.create(SortedRangeSet.of(Range.equal(1L)), true).subtract(Domain.create(SortedRangeSet.of(Range.equal(1L), Range.equal(2L)), false)),
                Domain.onlyNull(Long.class));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();

        Domain domain = Domain.all(Long.class);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.none(Double.class);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.notNull(Boolean.class);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.onlyNull(String.class);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.singleValue(Long.MIN_VALUE);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));

        domain = Domain.create(SortedRangeSet.of(Range.lessThan(0L), Range.equal(1L), Range.range(2L, true, 3L, true)), true);
        Assert.assertEquals(domain, mapper.readValue(mapper.writeValueAsString(domain), Domain.class));
    }
}
