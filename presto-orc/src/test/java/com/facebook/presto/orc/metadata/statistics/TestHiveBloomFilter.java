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
package com.facebook.presto.orc.metadata.statistics;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

public class TestHiveBloomFilter
{
    @Test
    public void testHashCode()
    {
        List<Long> bitset1 = ImmutableList.of(2L);
        List<Long> bitset2 = ImmutableList.of(2L);
        HiveBloomFilter filter1 = new HiveBloomFilter(bitset1, 1, 1);
        HiveBloomFilter filter2 = new HiveBloomFilter(bitset2, 1, 1);
        assertEquals(filter1, filter2);
        assertEquals(filter1.hashCode(), filter2.hashCode());
    }
}
