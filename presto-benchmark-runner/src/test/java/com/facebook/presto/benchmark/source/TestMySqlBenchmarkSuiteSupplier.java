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
package com.facebook.presto.benchmark.source;

import com.facebook.presto.testing.mysql.TestingMySqlServer;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.XDB;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.getBenchmarkSuiteObject;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.getBenchmarkSuitePhases;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.getBenchmarkSuiteSessionProperties;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.getJdbi;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.insertBenchmarkQuery;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.insertBenchmarkSuite;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.setupMySql;
import static com.facebook.presto.benchmark.source.PhaseSpecificationsColumnMapper.PHASE_SPECIFICATION_LIST_CODEC;
import static com.facebook.presto.benchmark.source.StringToStringMapColumnMapper.MAP_CODEC;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestMySqlBenchmarkSuiteSupplier
{
    private static final String SUITE = "test_suite";
    private static final String QUERY_SET = "test_set";
    private static TestingMySqlServer mySqlServer;
    private static Handle handle;
    private static Jdbi jdbi;

    @BeforeClass
    public void setup()
            throws Exception
    {
        mySqlServer = setupMySql();
        jdbi = getJdbi(mySqlServer);
        handle = jdbi.open();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        closeQuietly(mySqlServer, handle);
    }

    @AfterMethod
    public void cleanup()
    {
        handle.execute("DELETE FROM benchmark_suites");
        handle.execute("DELETE FROM benchmark_queries");
    }

    @Test
    public void testSupplySuite()
    {
        insertBenchmarkQuery(handle, QUERY_SET, "Q1", "SELECT 1");
        insertBenchmarkQuery(handle, QUERY_SET, "Q2", "SELECT 2");
        insertBenchmarkQuery(handle, QUERY_SET, "Q3", "SELECT 3");

        insertBenchmarkSuite(handle, SUITE, QUERY_SET, PHASE_SPECIFICATION_LIST_CODEC.toJson(getBenchmarkSuitePhases()), MAP_CODEC.toJson(getBenchmarkSuiteSessionProperties()));

        assertEquals(new MySqlBenchmarkSuiteSupplier(
                new MySqlBenchmarkSuiteConfig().setDatabaseUrl(mySqlServer.getJdbcUrl(XDB)),
                new BenchmarkSuiteConfig().setSuite(SUITE)).get(), getBenchmarkSuiteObject(SUITE, QUERY_SET));
    }
}
