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
package com.facebook.presto.raptorx;

import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.mysql.TestingMySqlServer;
import org.testng.annotations.AfterClass;

import static com.facebook.presto.raptorx.RaptorQueryRunner.createRaptorQueryRunner;

public class TestRaptorSmokeMySql
        extends TestRaptorSmoke
{
    private final TestingMySqlServer mysqlServer;

    public TestRaptorSmokeMySql()
            throws Exception
    {
        this(new TestingMySqlServer("testuser", "testpass", "testdb"));
    }

    private TestRaptorSmokeMySql(TestingMySqlServer mysqlServer)
    {
        super(() -> createRaptorMySqlQueryRunner(mysqlServer.getJdbcUrl("testdb")));
        this.mysqlServer = mysqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        mysqlServer.close();
    }

    private static DistributedQueryRunner createRaptorMySqlQueryRunner(String mysqlUrl)
            throws Exception
    {
        return createRaptorQueryRunner(
                queryRunner -> ImmutableMap.<String, String>builder()
                        .put("metadata.db.type", "mysql")
                        .put("metadata.db.url", mysqlUrl)
                        .build(),
                ImmutableMap.of(),
                true,
                false);
    }
}
