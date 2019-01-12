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
package io.prestosql.plugin.thrift.integration;

import com.google.common.collect.ImmutableMap;
import io.prestosql.tests.AbstractTestQueries;

import static io.prestosql.plugin.thrift.integration.ThriftQueryRunner.createThriftQueryRunner;

public class TestThriftDistributedQueries
        extends AbstractTestQueries
{
    public TestThriftDistributedQueries()
    {
        super(() -> createThriftQueryRunner(3, 3, false, ImmutableMap.of()));
    }

    @Override
    public void testAssignUniqueId()
    {
        // this test can take a long time
    }
}
