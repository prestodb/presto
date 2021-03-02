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
package com.facebook.presto.bloomfilter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestBloomFilterImpl
{
    @Test
    public void testBloomFilterContain() throws IOException
    {
        BloomFilterForDynamicFilter bloomFilter = new BloomFilterForDynamicFilterImpl(BloomFilter.create(1024, 0.03), null, 0);
        bloomFilter.put(123);
        bloomFilter.put(456);

        assertTrue(bloomFilter.contain(123));
        assertTrue(bloomFilter.contain(456));
        assertFalse(bloomFilter.contain(234));
    }

    @Test
    public void testBloomFilterMerge() throws IOException
    {
        BloomFilterForDynamicFilter alice = new BloomFilterForDynamicFilterImpl(BloomFilter.create(1024, 0.03), null, 0);
        BloomFilterForDynamicFilter bob = new BloomFilterForDynamicFilterImpl(BloomFilter.create(1024, 0.03), null, 0);

        alice.put("123");
        bob.put("234");

        alice.merge(bob);

        assertTrue(alice.contain("123"));
        assertFalse(alice.contain("456"));
        assertTrue(alice.contain("234"));

        BloomFilterForDynamicFilter bloomFilter3 = new BloomFilterForDynamicFilterImpl(null, null, 0);
        bob.merge(bloomFilter3);
        assertNull(((BloomFilterForDynamicFilterImpl) bob).getBloomFilter());

        bloomFilter3.merge(alice);
        assertNull(((BloomFilterForDynamicFilterImpl) bloomFilter3).getBloomFilter());
    }

    @Test
    public void testJson() throws IOException
    {
        BloomFilterForDynamicFilter alice = new BloomFilterForDynamicFilterImpl(BloomFilter.create(1024, 0.03), null, 0);
        alice.put(123);
        alice.put(456);

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(alice);
        assertEquals(((BloomFilterForDynamicFilterImpl) alice).getSizeAfterCompressed(), 68);

        BloomFilterForDynamicFilter bob = objectMapper.readValue(json, BloomFilterForDynamicFilterImpl.class);
        assertTrue(bob.contain(123));
        assertTrue(bob.contain(456));
        assertFalse(bob.contain(234));
    }
}
