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
package com.facebook.presto.raptor.integration;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.raptor.RaptorQueryRunner.createRaptorQueryRunner;

public class TestRaptorDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createRaptorQueryRunner(ImmutableMap.of(), true, false, false, ImmutableMap.of("storage.orc.optimized-writer-stage", "ENABLED_AND_VALIDATED"));
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }
}
