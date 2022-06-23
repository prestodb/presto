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
package com.facebook.presto.connector.thrift.integration;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIndexedQueries;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Paths;

import static com.facebook.presto.connector.thrift.integration.ThriftQueryRunner.createThriftQueryRunner;

public class TestThriftDistributedQueriesIndexed
        extends AbstractTestIndexedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createThriftQueryRunner(
                2,
                2,
                true,
                // set spill path so we can enable spill by session property
                ImmutableMap.of("experimental.spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills").toString()));
    }

    @Override
    public void testExampleSystemTable()
    {
        // system tables are not supported
    }
}
