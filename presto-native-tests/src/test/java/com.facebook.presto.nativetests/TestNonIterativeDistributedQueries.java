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
package com.facebook.presto.nativetests;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;

/**
 * Test that Presto works with {@link com.facebook.presto.sql.planner.iterative.IterativeOptimizer} disabled.
 */
public class TestNonIterativeDistributedQueries
        extends AbstractTestQueriesNative
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        /// Presto uses a rule-based iterative optimizer which is enabled by default and can be controlled with config
        /// 'experimental.iterative-optimizer-enabled'. Tests Presto C++ worker by disabling the iterative optimizer,
        /// resulting in possibly different query plans.
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(
                ImmutableMap.of("experimental.iterative-optimizer-enabled", "false"),
                ImmutableMap.of(), "PARQUET");
    }
}
