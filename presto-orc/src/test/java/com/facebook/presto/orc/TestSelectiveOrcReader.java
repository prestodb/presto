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

import com.facebook.presto.orc.TupleDomainFilter.BooleanValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcTester.quickSelectiveOrcTester;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Lists.newArrayList;
import static org.testng.Assert.assertEquals;

public class TestSelectiveOrcReader
{
    private final OrcTester tester = quickSelectiveOrcTester();

    @BeforeClass
    public void setUp()
    {
        assertEquals(DateTimeZone.getDefault(), HIVE_STORAGE_TIME_ZONE);
    }

    @Test
    public void testBooleanSequence()
            throws Exception
    {
        List<Map<Integer, TupleDomainFilter>> filters = ImmutableList.of(
                ImmutableMap.of(0, BooleanValue.of(true, false)),
                ImmutableMap.of(0, TupleDomainFilter.IS_NULL));
        tester.testRoundTrip(BOOLEAN, newArrayList(limit(cycle(ImmutableList.of(true, false, false)), 30_000)), filters);

        filters = ImmutableList.of(
                ImmutableMap.of(0, BooleanValue.of(true, false)),
                ImmutableMap.of(0, TupleDomainFilter.IS_NULL),
                ImmutableMap.of(1, BooleanValue.of(true, false)),
                ImmutableMap.of(0, BooleanValue.of(false, false), 1, BooleanValue.of(true, false)));
        tester.testRoundTripTypes(ImmutableList.of(BOOLEAN, BOOLEAN),
                ImmutableList.of(
                        newArrayList(limit(cycle(ImmutableList.of(true, false, false)), 30_000)),
                        newArrayList(limit(cycle(ImmutableList.of(true, true, false)), 30_000))),
                filters);
    }
}
