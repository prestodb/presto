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
package com.facebook.presto.hive.clustering;

import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestIntervalExtractor
{
    @Test
    public void testExtractIntervals()
    {
        List<Object> distribution = new ArrayList<>(Arrays.asList(
                1L, 3L, 5L,
                Slices.wrappedBuffer("ab".getBytes(StandardCharsets.UTF_8)),
                Slices.wrappedBuffer("de".getBytes(StandardCharsets.UTF_8)),
                Slices.wrappedBuffer("st".getBytes(StandardCharsets.UTF_8)),
                0.1, 0.3, 0.5));
        List<String> clusteredBy = new ArrayList<>(Arrays.asList(
                "c1", "c2", "c3"));
        List<Type> types = new ArrayList<>(Arrays.asList(
                IntegerType.INTEGER, VarcharType.VARCHAR, DoubleType.DOUBLE));
        List<Integer> clusterCount = new ArrayList<>(Arrays.asList(4, 4, 4));

        Map<String, List<Marker>> intervals = IntervalExtractor.extractIntervals(
                distribution, clusteredBy, types, clusterCount);

        assertEquals(intervals.size(), 3);
        Set<String> keys = new HashSet<>(Arrays.asList("c1", "c2", "c3"));
        assertEquals(intervals.keySet(), keys);

        List<Marker> integerIntervals = intervals.get("c1");
        assertEquals(integerIntervals.size(), 3);
        assertEquals((long) integerIntervals.get(0).getValue(), 1L);
        assertEquals((long) integerIntervals.get(1).getValue(), 3L);
        assertEquals((long) integerIntervals.get(2).getValue(), 5L);

        integerIntervals = intervals.get("c2");
        assertEquals(integerIntervals.size(), 3);
        assertEquals(((Slice) integerIntervals.get(0).getValue()).compareTo(
                Slices.wrappedBuffer("ab".getBytes(StandardCharsets.UTF_8))), 0);
        assertEquals(((Slice) integerIntervals.get(1).getValue()).compareTo(
                Slices.wrappedBuffer("de".getBytes(StandardCharsets.UTF_8))), 0);
        assertEquals(((Slice) integerIntervals.get(2).getValue()).compareTo(
                Slices.wrappedBuffer("st".getBytes(StandardCharsets.UTF_8))), 0);
    }
}
