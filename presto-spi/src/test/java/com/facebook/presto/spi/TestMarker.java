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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

public class TestMarker
{
    @Test
    public void testTypes()
            throws Exception
    {
        Assert.assertEquals(Marker.lowerUnbounded(Long.class).getType(), Long.class);
        Assert.assertEquals(Marker.below(1L).getType(), Long.class);
        Assert.assertEquals(Marker.exactly(1L).getType(), Long.class);
        Assert.assertEquals(Marker.above(1L).getType(), Long.class);
        Assert.assertEquals(Marker.upperUnbounded(Long.class).getType(), Long.class);
    }

    @Test
    public void testUnbounded()
            throws Exception
    {
        Assert.assertTrue(Marker.lowerUnbounded(Long.class).isLowerUnbounded());
        Assert.assertFalse(Marker.lowerUnbounded(Long.class).isUpperUnbounded());
        Assert.assertTrue(Marker.upperUnbounded(Long.class).isUpperUnbounded());
        Assert.assertFalse(Marker.upperUnbounded(Long.class).isLowerUnbounded());

        Assert.assertFalse(Marker.below(1L).isLowerUnbounded());
        Assert.assertFalse(Marker.below(1L).isUpperUnbounded());
        Assert.assertFalse(Marker.exactly(1L).isLowerUnbounded());
        Assert.assertFalse(Marker.exactly(1L).isUpperUnbounded());
        Assert.assertFalse(Marker.above(1L).isLowerUnbounded());
        Assert.assertFalse(Marker.above(1L).isUpperUnbounded());
    }

    @Test
    public void testComparisons()
            throws Exception
    {
        ImmutableList<Marker> markers = ImmutableList.of(
                Marker.lowerUnbounded(Long.class),
                Marker.above(0L),
                Marker.below(1L),
                Marker.exactly(1L),
                Marker.above(1L),
                Marker.below(2L),
                Marker.upperUnbounded(Long.class));

        Assert.assertTrue(Ordering.natural().isStrictlyOrdered(markers));

        // Compare every marker with every other marker
        // Since the markers are strictly ordered, the value of the comparisons should be equivalent to the comparisons
        // of their indexes.
        for (int i = 0; i < markers.size(); i++) {
            for (int j = 0; j < markers.size(); j++) {
                Assert.assertTrue(markers.get(i).compareTo(markers.get(j)) == Integer.compare(i, j));
            }
        }
    }

    @Test
    public void testAdjacency()
            throws Exception
    {
        ImmutableMap<Marker, Integer> markers = ImmutableMap.<Marker, Integer>builder()
                .put(Marker.lowerUnbounded(Long.class), -1000)
                .put(Marker.above(0L), -100)
                .put(Marker.below(1L), -1)
                .put(Marker.exactly(1L), 0)
                .put(Marker.above(1L), 1)
                .put(Marker.below(2L), 100)
                .put(Marker.upperUnbounded(Long.class), 1000)
                .build();

        // Compare every marker with every other marker
        // Map values of distance 1 indicate expected adjacency
        for (Map.Entry<Marker, Integer> entry1 : markers.entrySet()) {
            for (Map.Entry<Marker, Integer> entry2 : markers.entrySet()) {
                boolean adjacent = entry1.getKey().isAdjacent(entry2.getKey());
                boolean distanceIsOne = Math.abs(entry1.getValue() - entry2.getValue()) == 1;
                Assert.assertEquals(adjacent, distanceIsOne);
            }
        }

        Assert.assertEquals(Marker.below(1L).greaterAdjacent(), Marker.exactly(1L));
        Assert.assertEquals(Marker.exactly(1L).greaterAdjacent(), Marker.above(1L));
        Assert.assertEquals(Marker.above(1L).lesserAdjacent(), Marker.exactly(1L));
        Assert.assertEquals(Marker.exactly(1L).lesserAdjacent(), Marker.below(1L));

        try {
            Marker.below(1L).lesserAdjacent();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }

        try {
            Marker.above(1L).greaterAdjacent();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }

        try {
            Marker.lowerUnbounded(Long.class).lesserAdjacent();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }

        try {
            Marker.lowerUnbounded(Long.class).greaterAdjacent();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }

        try {
            Marker.upperUnbounded(Long.class).lesserAdjacent();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }

        try {
            Marker.upperUnbounded(Long.class).greaterAdjacent();
            Assert.fail();
        }
        catch (IllegalStateException e) {
        }
    }

    @Test
    public void testJsonSerialization()
            throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();

        Marker marker = Marker.above(0L);
        Assert.assertEquals(marker, mapper.readValue(mapper.writeValueAsString(marker), Marker.class));

        marker = Marker.exactly("abc");
        Assert.assertEquals(marker, mapper.readValue(mapper.writeValueAsString(marker), Marker.class));

        marker = Marker.below(0.123);
        Assert.assertEquals(marker, mapper.readValue(mapper.writeValueAsString(marker), Marker.class));

        marker = Marker.exactly(true);
        Assert.assertEquals(marker, mapper.readValue(mapper.writeValueAsString(marker), Marker.class));

        marker = Marker.upperUnbounded(Long.class);
        Assert.assertEquals(marker, mapper.readValue(mapper.writeValueAsString(marker), Marker.class));

        marker = Marker.lowerUnbounded(Long.class);
        Assert.assertEquals(marker, mapper.readValue(mapper.writeValueAsString(marker), Marker.class));
    }
}
