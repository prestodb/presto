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
package com.facebook.presto.kafka;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.common.predicate.Domain.multipleValues;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKafkaFilterManager
{
    @Test
    public void testFilterValuesByDomain()
    {
        Set<Long> source = Sets.newHashSet(1L, 2L, 3L, 4L, 5L, 6L);

        Domain testDomain = Domain.singleValue(BIGINT, 1L);
        assertEquals(KafkaFilterManager.filterValuesByDomain(testDomain, source), Sets.newHashSet(1L));

        testDomain = multipleValues(BIGINT, ImmutableList.of(3L, 8L));
        assertEquals(KafkaFilterManager.filterValuesByDomain(testDomain, source), Sets.newHashSet(3L));

        testDomain = Domain.create(SortedRangeSet.copyOf(BIGINT,
                ImmutableList.of(
                        Range.range(BIGINT, 2L, true, 4L, true))),
                false);

        assertEquals(KafkaFilterManager.filterValuesByDomain(testDomain, source), Sets.newHashSet(2L, 3L, 4L));
    }

    @Test
    public void testFilterRangeByDomain()
    {
        Domain testDomain = Domain.singleValue(BIGINT, 1L);
        assertTrue(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent());
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin(), 1L);
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd(), 2L);

        testDomain = multipleValues(BIGINT, ImmutableList.of(3L, 8L));
        assertTrue(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent());
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin(), 3L);
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd(), 9L);

        testDomain = Domain.create(SortedRangeSet.copyOf(BIGINT,
                ImmutableList.of(
                        Range.range(BIGINT, 2L, true, 4L, true))),
                false);

        assertTrue(KafkaFilterManager.filterRangeByDomain(testDomain).isPresent());
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getBegin(), 2L);
        assertEquals(KafkaFilterManager.filterRangeByDomain(testDomain).get().getEnd(), 5L);
    }
}
