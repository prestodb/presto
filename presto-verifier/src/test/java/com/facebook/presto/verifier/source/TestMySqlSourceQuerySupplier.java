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
package com.facebook.presto.verifier.source;

import com.facebook.presto.testing.mysql.TestingMySqlServer;
import org.jdbi.v3.core.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.verifier.VerifierTestUtil.VERIFIER_QUERIES_TABLE;
import static com.facebook.presto.verifier.VerifierTestUtil.XDB;
import static com.facebook.presto.verifier.VerifierTestUtil.getHandle;
import static com.facebook.presto.verifier.VerifierTestUtil.insertSourceQuery;
import static com.facebook.presto.verifier.VerifierTestUtil.setupMySql;
import static com.facebook.presto.verifier.VerifierTestUtil.truncateVerifierQueries;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMySqlSourceQuerySupplier
{
    private static final String SUITE = "test";
    private static TestingMySqlServer mySqlServer;
    private static Handle handle;
    private static MySqlSourceQueryConfig config;

    @BeforeClass
    public void setup()
            throws Exception
    {
        mySqlServer = setupMySql();
        handle = getHandle(mySqlServer);
        config = new MySqlSourceQueryConfig()
                .setDatabase(mySqlServer.getJdbcUrl(XDB))
                .setTableName(VERIFIER_QUERIES_TABLE);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        closeQuietly(mySqlServer, handle);
    }

    @AfterMethod
    public void cleanup()
    {
        truncateVerifierQueries(handle);
    }

    @Test
    public void testSupplyQueries()
    {
        insertSourceQuery(handle, SUITE, "query_1", "SELECT 1");
        insertSourceQuery(handle, SUITE, "query_2", "SELECT 2");

        assertTrue(new MySqlSourceQuerySupplier(config.setSuites("other")).get().isEmpty());
        assertEquals(new MySqlSourceQuerySupplier(config.setSuites(SUITE).setMaxQueriesPerSuite(1)).get().size(), 1);
        assertEquals(new MySqlSourceQuerySupplier(config.setSuites(SUITE).setMaxQueriesPerSuite(100)).get().size(), 2);
    }
}
