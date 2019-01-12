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
package io.prestosql.spi.predicate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.TestingBlockEncodingSerde;
import io.prestosql.spi.block.TestingBlockJsonSerde;
import io.prestosql.spi.type.TestingTypeDeserializer;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSortedRangeSet
{
    @Test
    public void testEmptySet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.none(BIGINT);
        assertEquals(rangeSet.getType(), BIGINT);
        assertTrue(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertTrue(Iterables.isEmpty(rangeSet.getOrderedRanges()));
        assertEquals(rangeSet.getRangeCount(), 0);
        assertEquals(rangeSet.complement(), SortedRangeSet.all(BIGINT));
        assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(BIGINT)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(BIGINT, 0L)));
        assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testEntireSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.all(BIGINT);
        assertEquals(rangeSet.getType(), BIGINT);
        assertFalse(rangeSet.isNone());
        assertTrue(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertEquals(rangeSet.getRangeCount(), 1);
        assertEquals(rangeSet.complement(), SortedRangeSet.none(BIGINT));
        assertTrue(rangeSet.includesMarker(Marker.lowerUnbounded(BIGINT)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 0L)));
        assertTrue(rangeSet.includesMarker(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testSingleValue()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(BIGINT, 10L);

        SortedRangeSet complement = SortedRangeSet.of(Range.greaterThan(BIGINT, 10L), Range.lessThan(BIGINT, 10L));

        assertEquals(rangeSet.getType(), BIGINT);
        assertFalse(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertTrue(rangeSet.isSingleValue());
        assertTrue(Iterables.elementsEqual(rangeSet.getOrderedRanges(), ImmutableList.of(Range.equal(BIGINT, 10L))));
        assertEquals(rangeSet.getRangeCount(), 1);
        assertEquals(rangeSet.complement(), complement);
        assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(BIGINT)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 10L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(BIGINT, 9L)));
        assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testBoundedSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.equal(BIGINT, 10L),
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 9L, true, 11L, false),
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 4L, true),
                Range.range(BIGINT, 4L, false, 5L, true));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.equal(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 5L, true),
                Range.range(BIGINT, 9L, true, 11L, false));

        SortedRangeSet complement = SortedRangeSet.of(
                Range.lessThan(BIGINT, 0L),
                Range.range(BIGINT, 0L, false, 2L, false),
                Range.range(BIGINT, 5L, false, 9L, false),
                Range.greaterThanOrEqual(BIGINT, 11L));

        assertEquals(rangeSet.getType(), BIGINT);
        assertFalse(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertTrue(Iterables.elementsEqual(rangeSet.getOrderedRanges(), normalizedResult));
        assertEquals(rangeSet, SortedRangeSet.copyOf(BIGINT, normalizedResult));
        assertEquals(rangeSet.getRangeCount(), 3);
        assertEquals(rangeSet.complement(), complement);
        assertFalse(rangeSet.includesMarker(Marker.lowerUnbounded(BIGINT)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 0L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(BIGINT, 1L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(BIGINT, 7L)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 9L)));
        assertFalse(rangeSet.includesMarker(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testUnboundedSet()
    {
        SortedRangeSet rangeSet = SortedRangeSet.of(
                Range.greaterThan(BIGINT, 10L),
                Range.lessThanOrEqual(BIGINT, 0L),
                Range.range(BIGINT, 2L, true, 4L, false),
                Range.range(BIGINT, 4L, true, 6L, false),
                Range.range(BIGINT, 1L, false, 2L, false),
                Range.range(BIGINT, 9L, false, 11L, false));

        ImmutableList<Range> normalizedResult = ImmutableList.of(
                Range.lessThanOrEqual(BIGINT, 0L),
                Range.range(BIGINT, 1L, false, 6L, false),
                Range.greaterThan(BIGINT, 9L));

        SortedRangeSet complement = SortedRangeSet.of(
                Range.range(BIGINT, 0L, false, 1L, true),
                Range.range(BIGINT, 6L, true, 9L, true));

        assertEquals(rangeSet.getType(), BIGINT);
        assertFalse(rangeSet.isNone());
        assertFalse(rangeSet.isAll());
        assertFalse(rangeSet.isSingleValue());
        assertTrue(Iterables.elementsEqual(rangeSet.getOrderedRanges(), normalizedResult));
        assertEquals(rangeSet, SortedRangeSet.copyOf(BIGINT, normalizedResult));
        assertEquals(rangeSet.getRangeCount(), 3);
        assertEquals(rangeSet.complement(), complement);
        assertTrue(rangeSet.includesMarker(Marker.lowerUnbounded(BIGINT)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 0L)));
        assertTrue(rangeSet.includesMarker(Marker.exactly(BIGINT, 4L)));
        assertFalse(rangeSet.includesMarker(Marker.exactly(BIGINT, 7L)));
        assertTrue(rangeSet.includesMarker(Marker.upperUnbounded(BIGINT)));
    }

    @Test
    public void testGetSingleValue()
    {
        assertEquals(SortedRangeSet.of(BIGINT, 0L).getSingleValue(), 0L);
        try {
            SortedRangeSet.all(BIGINT).getSingleValue();
            fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test
    public void testSpan()
    {
        try {
            SortedRangeSet.none(BIGINT).getSpan();
            fail();
        }
        catch (IllegalStateException e) {
        }

        assertEquals(SortedRangeSet.all(BIGINT).getSpan(), Range.all(BIGINT));
        assertEquals(SortedRangeSet.of(BIGINT, 0L).getSpan(), Range.equal(BIGINT, 0L));
        assertEquals(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).getSpan(), Range.range(BIGINT, 0L, true, 1L, true));
        assertEquals(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.greaterThan(BIGINT, 1L)).getSpan(), Range.greaterThanOrEqual(BIGINT, 0L));
        assertEquals(SortedRangeSet.of(Range.lessThan(BIGINT, 0L), Range.greaterThan(BIGINT, 1L)).getSpan(), Range.all(BIGINT));
    }

    @Test
    public void testOverlaps()
    {
        assertTrue(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.all(BIGINT)));
        assertFalse(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.none(BIGINT)));
        assertTrue(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(BIGINT, 0L)));
        assertTrue(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertTrue(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertTrue(SortedRangeSet.all(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.all(BIGINT)));
        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.none(BIGINT)));
        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(BIGINT, 0L)));
        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertFalse(SortedRangeSet.none(BIGINT).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertTrue(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.all(BIGINT)));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.none(BIGINT)));
        assertTrue(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(BIGINT, 0L)));
        assertTrue(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertTrue(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).overlaps(SortedRangeSet.of(Range.equal(BIGINT, 2L))));
        assertTrue(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertTrue(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).overlaps(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L))));
        assertFalse(SortedRangeSet.of(Range.lessThan(BIGINT, 0L)).overlaps(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
    }

    @Test
    public void testContains()
    {
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.all(BIGINT)));
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.none(BIGINT)));
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(BIGINT, 0L)));
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertTrue(SortedRangeSet.all(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertFalse(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.all(BIGINT)));
        assertTrue(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.none(BIGINT)));
        assertFalse(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(BIGINT, 0L)));
        assertFalse(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertFalse(SortedRangeSet.none(BIGINT).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertFalse(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.all(BIGINT)));
        assertTrue(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.none(BIGINT)));
        assertTrue(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(BIGINT, 0L)));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertFalse(SortedRangeSet.of(BIGINT, 0L).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L), Range.lessThan(BIGINT, 0L))));

        assertTrue(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).contains(SortedRangeSet.of(Range.equal(BIGINT, 1L))));
        assertFalse(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).contains(SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L))));
        assertTrue(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
        assertFalse(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L))));
        assertFalse(SortedRangeSet.of(Range.lessThan(BIGINT, 0L)).contains(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))));
    }

    @Test
    public void testIntersect()
    {
        assertEquals(
                SortedRangeSet.none(BIGINT).intersect(
                        SortedRangeSet.none(BIGINT)),
                SortedRangeSet.none(BIGINT));

        assertEquals(
                SortedRangeSet.all(BIGINT).intersect(
                        SortedRangeSet.all(BIGINT)),
                SortedRangeSet.all(BIGINT));

        assertEquals(
                SortedRangeSet.none(BIGINT).intersect(
                        SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));

        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)).intersect(
                        SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L))),
                SortedRangeSet.of(Range.equal(BIGINT, 2L)));

        assertEquals(
                SortedRangeSet.all(BIGINT).intersect(
                        SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L))),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 4L)));

        assertEquals(
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 4L, false)).intersect(
                        SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.greaterThan(BIGINT, 3L))),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.range(BIGINT, 3L, false, 4L, false)));

        assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)).intersect(
                        SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L))),
                SortedRangeSet.of(Range.equal(BIGINT, 0L)));

        assertEquals(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, -1L)).intersect(
                        SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 1L))),
                SortedRangeSet.of(Range.range(BIGINT, -1L, true, 1L, true)));
    }

    @Test
    public void testUnion()
    {
        assertUnion(SortedRangeSet.none(BIGINT), SortedRangeSet.none(BIGINT), SortedRangeSet.none(BIGINT));
        assertUnion(SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT));
        assertUnion(SortedRangeSet.none(BIGINT), SortedRangeSet.all(BIGINT), SortedRangeSet.all(BIGINT));

        assertUnion(
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)),
                SortedRangeSet.of(Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)),
                SortedRangeSet.of(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)));

        assertUnion(SortedRangeSet.all(BIGINT), SortedRangeSet.of(Range.equal(BIGINT, 0L)), SortedRangeSet.all(BIGINT));

        assertUnion(
                SortedRangeSet.of(Range.range(BIGINT, 0L, true, 4L, false)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 3L)),
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)));

        assertUnion(
                SortedRangeSet.of(Range.greaterThanOrEqual(BIGINT, 0L)),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L)),
                SortedRangeSet.of(Range.all(BIGINT)));

        assertUnion(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)),
                SortedRangeSet.of(Range.lessThan(BIGINT, 0L)),
                SortedRangeSet.of(BIGINT, 0L).complement());
    }

    @Test
    public void testSubtract()
    {
        assertEquals(
                SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.none(BIGINT)),
                SortedRangeSet.all(BIGINT));
        assertEquals(
                SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(BIGINT, 0L)),
                SortedRangeSet.of(BIGINT, 0L).complement());
        assertEquals(
                SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))),
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).complement());
        assertEquals(
                SortedRangeSet.all(BIGINT).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))),
                SortedRangeSet.of(Range.lessThanOrEqual(BIGINT, 0L)));

        assertEquals(
                SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.none(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(BIGINT, 0L)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.none(BIGINT).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))),
                SortedRangeSet.none(BIGINT));

        assertEquals(
                SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.none(BIGINT)),
                SortedRangeSet.of(BIGINT, 0L));
        assertEquals(
                SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(BIGINT, 0L)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(BIGINT, 0L).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))),
                SortedRangeSet.of(BIGINT, 0L));

        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.none(BIGINT)),
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)));
        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(BIGINT, 0L)),
                SortedRangeSet.of(BIGINT, 1L));
        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L)).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))),
                SortedRangeSet.of(Range.equal(BIGINT, 0L)));

        assertEquals(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.all(BIGINT)),
                SortedRangeSet.none(BIGINT));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.none(BIGINT)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(BIGINT, 0L)),
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(Range.equal(BIGINT, 0L), Range.equal(BIGINT, 1L))),
                SortedRangeSet.of(Range.range(BIGINT, 0L, false, 1L, false), Range.greaterThan(BIGINT, 1L)));
        assertEquals(
                SortedRangeSet.of(Range.greaterThan(BIGINT, 0L)).subtract(SortedRangeSet.of(Range.greaterThan(BIGINT, 0L))),
                SortedRangeSet.none(BIGINT));
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        TestingTypeManager typeManager = new TestingTypeManager();
        TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde(typeManager);

        ObjectMapper mapper = new ObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(typeManager))
                        .addSerializer(Block.class, new TestingBlockJsonSerde.Serializer(blockEncodingSerde))
                        .addDeserializer(Block.class, new TestingBlockJsonSerde.Deserializer(blockEncodingSerde)));

        SortedRangeSet set = SortedRangeSet.all(BIGINT);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.none(DOUBLE);
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.of(VARCHAR, utf8Slice("abc"));
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));

        set = SortedRangeSet.of(Range.equal(BOOLEAN, true), Range.equal(BOOLEAN, false));
        assertEquals(set, mapper.readValue(mapper.writeValueAsString(set), SortedRangeSet.class));
    }

    private void assertUnion(SortedRangeSet first, SortedRangeSet second, SortedRangeSet expected)
    {
        assertEquals(first.union(second), expected);
        assertEquals(first.union(ImmutableList.of(first, second)), expected);
    }
}
