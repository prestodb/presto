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
package com.facebook.presto.thrift.api.valuesets;

import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.thrift.api.datatypes.PrestoThriftBigint;
import com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock;
import com.facebook.presto.thrift.api.valuesets.PrestoThriftRangeValueSet.PrestoThriftMarker;
import com.facebook.presto.thrift.api.valuesets.PrestoThriftRangeValueSet.PrestoThriftRange;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock.bigintData;
import static com.facebook.presto.thrift.api.valuesets.PrestoThriftRangeValueSet.PrestoThriftBound.ABOVE;
import static com.facebook.presto.thrift.api.valuesets.PrestoThriftRangeValueSet.PrestoThriftBound.BELOW;
import static com.facebook.presto.thrift.api.valuesets.PrestoThriftRangeValueSet.PrestoThriftBound.EXACTLY;
import static com.facebook.presto.thrift.api.valuesets.PrestoThriftValueSet.fromValueSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestPrestoThriftRangeValueSet
{
    @Test
    public void testFromValueSetAll()
    {
        PrestoThriftValueSet thriftValueSet = fromValueSet(ValueSet.all(BIGINT));
        assertNotNull(thriftValueSet.getRangeValueSet());
        assertEquals(thriftValueSet.getRangeValueSet().getRanges(), ImmutableList.of(
                new PrestoThriftRange(new PrestoThriftMarker(null, ABOVE), new PrestoThriftMarker(null, BELOW))));
    }

    @Test
    public void testFromValueSetNone()
    {
        PrestoThriftValueSet thriftValueSet = fromValueSet(ValueSet.none(BIGINT));
        assertNotNull(thriftValueSet.getRangeValueSet());
        assertEquals(thriftValueSet.getRangeValueSet().getRanges(), ImmutableList.of());
    }

    @Test
    public void testFromValueSetOf()
    {
        PrestoThriftValueSet thriftValueSet = fromValueSet(ValueSet.of(BIGINT, 1L, 2L, 3L));
        assertNotNull(thriftValueSet.getRangeValueSet());
        assertEquals(thriftValueSet.getRangeValueSet().getRanges(), ImmutableList.of(
                new PrestoThriftRange(new PrestoThriftMarker(longValue(1), EXACTLY), new PrestoThriftMarker(longValue(1), EXACTLY)),
                new PrestoThriftRange(new PrestoThriftMarker(longValue(2), EXACTLY), new PrestoThriftMarker(longValue(2), EXACTLY)),
                new PrestoThriftRange(new PrestoThriftMarker(longValue(3), EXACTLY), new PrestoThriftMarker(longValue(3), EXACTLY))));
    }

    @Test
    public void testFromValueSetOfRangesUnbounded()
    {
        PrestoThriftValueSet thriftValueSet = fromValueSet(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 0L)));
        assertNotNull(thriftValueSet.getRangeValueSet());
        assertEquals(thriftValueSet.getRangeValueSet().getRanges(), ImmutableList.of(
                new PrestoThriftRange(new PrestoThriftMarker(longValue(0), EXACTLY), new PrestoThriftMarker(null, BELOW))));
    }

    @Test
    public void testFromValueSetOfRangesBounded()
    {
        PrestoThriftValueSet thriftValueSet = fromValueSet(ValueSet.ofRanges(
                range(BIGINT, -10L, true, -1L, false),
                range(BIGINT, -1L, false, 100L, true)));
        assertNotNull(thriftValueSet.getRangeValueSet());
        assertEquals(thriftValueSet.getRangeValueSet().getRanges(), ImmutableList.of(
                new PrestoThriftRange(new PrestoThriftMarker(longValue(-10), EXACTLY), new PrestoThriftMarker(longValue(-1), BELOW)),
                new PrestoThriftRange(new PrestoThriftMarker(longValue(-1), ABOVE), new PrestoThriftMarker(longValue(100), EXACTLY))));
    }

    private static PrestoThriftBlock longValue(long value)
    {
        return bigintData(new PrestoThriftBigint(null, new long[] {value}));
    }
}
