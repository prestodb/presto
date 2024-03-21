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

import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilter;
import com.facebook.presto.bloomfilter.BloomFilterForDynamicFilterImpl;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

@Description("Determines if this element is in the bloom filter")
@ScalarFunction(value = "bloom_filter_contains")
public class InBloomFilterFunction
{
    private static BloomFilterForDynamicFilter[] bloomFilterForDynamicFilters = new BloomFilterForDynamicFilter[10007];
    private static Long[] autoGraphs = new Long[10007];

    private InBloomFilterFunction() {}

    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean contains(@SqlType(StandardTypes.BIGINT) long autoGraph, @SqlType(StandardTypes.VARCHAR) Slice bloomFilter, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        try {
            int index = (int) (autoGraph % 10007);
            BloomFilterForDynamicFilter myBloomFilter = bloomFilterForDynamicFilters[index];
            if (myBloomFilter == null) {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bloomFilter.getBytes());
                InputStreamReader inputStreamReader = new InputStreamReader(byteArrayInputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                ObjectMapper objectMapper = new ObjectMapper();
                myBloomFilter = objectMapper.readValue(bufferedReader.readLine(), BloomFilterForDynamicFilterImpl.class);
                bloomFilterForDynamicFilters[index] = myBloomFilter;
                autoGraphs[index] = autoGraph;

                return myBloomFilter.contain(new String(value.getBytes()));
            }

            return myBloomFilter.contain(new String(value.getBytes()));
        }
        catch (IOException e) {
            return true;
        }
    }
}
