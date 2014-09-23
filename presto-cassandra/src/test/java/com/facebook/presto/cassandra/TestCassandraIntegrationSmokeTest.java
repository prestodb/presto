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
package com.facebook.presto.cassandra;

import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import org.testng.annotations.Test;

import static com.facebook.presto.cassandra.CassandraQueryRunner.createCassandraQueryRunner;
import static com.facebook.presto.cassandra.CassandraQueryRunner.createCassandraSession;
import static com.facebook.presto.cassandra.CassandraQueryRunner.createSampledSession;
import static io.airlift.tpch.TpchTable.ORDERS;

@Test(singleThreaded = true)
public class TestCassandraIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    public TestCassandraIntegrationSmokeTest()
            throws Exception
    {
        super(createCassandraQueryRunner(ORDERS), createSampledSession());
    }

    @Test
    public void testStringPartitionKey()
    {
        TestCassandraConnector.createOrReplaceKeyspace("example");
        TestCassandraConnector.createTestData("example", "test_key_text");
        queryRunner.execute(createCassandraSession("example"), "select * from example.test_key_text where key='key 0001'");

    }
}
