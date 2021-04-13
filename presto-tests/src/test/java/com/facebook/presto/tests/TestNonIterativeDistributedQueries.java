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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;

/**
 * Test that Presto works with {@link com.facebook.presto.sql.planner.iterative.IterativeOptimizer} disabled.
 */
public class TestNonIterativeDistributedQueries
        extends AbstractTestQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().setSingleExtraProperty("experimental.iterative-optimizer-enabled", "false").build();
    }
}
