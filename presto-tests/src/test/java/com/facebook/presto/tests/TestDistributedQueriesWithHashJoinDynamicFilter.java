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
package com.facebook.presto.tests;

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;

public class TestDistributedQueriesWithHashJoinDynamicFilter
        extends AbstractTestQueries
{
    public TestDistributedQueriesWithHashJoinDynamicFilter()
    {
        super(() -> TpchQueryRunnerBuilder.builder()
                .amendSession(builder -> builder.setSystemProperty(SystemSessionProperties.ENABLE_HASH_JOIN_DYNAMIC_FILTERING, "true")
                        .setSystemProperty(SystemSessionProperties.BLOOM_FILTER_FOR_DYNAMIC_FILTERING_SIZE, "100kB")
                        .setSystemProperty(SystemSessionProperties.BLOOM_FILTER_FOR_DYNAMIC_FILTERING_FALSE_POSITIVE_PROBABILITY, "0.03"))
                .build());
    }
}
