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
import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import org.testng.annotations.AfterClass;

import static com.facebook.presto.raptorx.RaptorQueryRunner.createRaptorQueryRunner;

public class TestRaptorSmokePostgreSql
        extends TestRaptorSmoke
{
    private final TestingPostgreSqlServer postgresqlServer;

    public TestRaptorSmokePostgreSql()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "testdb"));
    }

    private TestRaptorSmokePostgreSql(TestingPostgreSqlServer postgresqlServer)
    {
        super(() -> createRaptorPostgreSqlQueryRunner(postgresqlServer.getJdbcUrl()));
        this.postgresqlServer = postgresqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws Exception
    {
        postgresqlServer.close();
    }

    private static DistributedQueryRunner createRaptorPostgreSqlQueryRunner(String postgresqlUrl)
            throws Exception
    {
        return createRaptorQueryRunner(
                queryRunner -> ImmutableMap.<String, String>builder()
                        .put("metadata.db.type", "postgresql")
                        .put("metadata.db.url", postgresqlUrl)
                        .build(),
                ImmutableMap.of(),
                true,
                false);
    }
}
