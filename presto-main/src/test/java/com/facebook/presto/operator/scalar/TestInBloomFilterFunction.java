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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.bloomfilter.BloomFilter;
import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilter;
import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilterImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestInBloomFilterFunction
        extends AbstractTestFunctions
{
    @Test
    public void bloomFilterContains() throws IOException
    {
        BloomFilterForDynamicFilter myBloomFilter = new BloomFilterForDynamicFilterImpl(BloomFilter.create(1024, 0.03), null, 0);
        myBloomFilter.put("123");
        myBloomFilter.put("159");

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(myBloomFilter);
        assertFunction("BLOOM_FILTER_CONTAINS(123, '" + json + "', '123')", BOOLEAN, true);
        assertFunction("BLOOM_FILTER_CONTAINS(123, '" + json + "', '158')", BOOLEAN, false);
    }
}
