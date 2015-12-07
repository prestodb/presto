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
package com.facebook.presto.plugin.mysql;

import com.facebook.presto.Session;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import io.airlift.testing.mysql.TestingMySqlServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test
public class TestMySqlIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingMySqlServer mysqlServer;

    public TestMySqlIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingMySqlServer("testuser", "testpass", "tpch", "test_database"));
    }

    public TestMySqlIntegrationSmokeTest(TestingMySqlServer mysqlServer)
            throws Exception
    {
        super(createMySqlQueryRunner(mysqlServer, ORDERS));
        this.mysqlServer = mysqlServer;
    }

    @Override
    public void testViewAccessControl()
    {
        // jdbc connector does not support views
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        closeAllRuntimeException(mysqlServer);
    }

    @Test
    public void testNameEscaping()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("mysql")
                .setSchema("test_database")
                .build();

        assertFalse(queryRunner.tableExists(session, "test_table"));

        assertUpdate(session, "CREATE TABLE test_table AS SELECT 123 x", 1);
        assertTrue(queryRunner.tableExists(session, "test_table"));

        assertQuery(session, "SELECT * FROM test_table", "SELECT 123");

        assertUpdate(session, "DROP TABLE test_table");
        assertFalse(queryRunner.tableExists(session, "test_table"));
    }
}
