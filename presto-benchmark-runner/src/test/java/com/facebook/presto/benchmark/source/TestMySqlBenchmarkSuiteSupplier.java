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

import com.facebook.presto.benchmark.framework.BenchmarkQuery;
import com.facebook.presto.benchmark.framework.BenchmarkSuite;
import com.facebook.presto.benchmark.framework.ConcurrentExecutionPhase;
import com.facebook.presto.benchmark.framework.PhaseSpecification;
import com.facebook.presto.benchmark.framework.StreamExecutionPhase;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jdbi.v3.core.Handle;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.CATALOG;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.SCHEMA;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.XDB;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.getJdbi;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.insertBenchmarkQuery;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.insertBenchmarkSuite;
import static com.facebook.presto.benchmark.BenchmarkTestUtil.setupMySql;
import static com.facebook.presto.benchmark.source.PhaseSpecificationsColumnMapper.PHASE_SPECIFICATIONS_CODEC;
import static com.facebook.presto.benchmark.source.StringToStringMapColumnMapper.MAP_CODEC;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestMySqlBenchmarkSuiteSupplier
{
    private static final String SUITE = "test_suite";
    private static final String QUERY_SET = "test_set";

    private TestingMySqlServer mySqlServer;
    private Handle handle;

    @BeforeClass
    public void setup()
            throws Exception
    {
        mySqlServer = setupMySql();
        handle = getJdbi(mySqlServer).open();
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
        Map<String, String> suiteSessionProperties = ImmutableMap.of("a", "1");
        Map<String, String> phaseSessionProperties = ImmutableMap.of("b", "2");
        List<PhaseSpecification> phases = ImmutableList.of(
                new StreamExecutionPhase("Phase-1", ImmutableList.of(ImmutableList.of("Q1", "Q2"), ImmutableList.of("Q2", "Q3"))),
                new ConcurrentExecutionPhase("Phase-2", ImmutableList.of("Q1", "Q2", "Q3"), Optional.of(50)));

        insertBenchmarkQuery(handle, QUERY_SET, "Q1", "SELECT 1", Optional.empty());
        insertBenchmarkQuery(handle, QUERY_SET, "Q2", "SELECT 2", Optional.empty());
        insertBenchmarkQuery(handle, QUERY_SET, "Q3", "SELECT 3", Optional.of(phaseSessionProperties));
        insertBenchmarkSuite(handle, SUITE, QUERY_SET, PHASE_SPECIFICATIONS_CODEC.toJson(phases), MAP_CODEC.toJson(suiteSessionProperties));

        BenchmarkSuite actual = new MySqlBenchmarkSuiteSupplier(
                new MySqlBenchmarkSuiteConfig().setDatabaseUrl(mySqlServer.getJdbcUrl(XDB)),
                new BenchmarkSuiteConfig().setSuite(SUITE)).get();
        BenchmarkSuite expected = new BenchmarkSuite(
                SUITE,
                QUERY_SET,
                phases,
                suiteSessionProperties,
                ImmutableList.of(
                        new BenchmarkQuery("Q1", "SELECT 1", CATALOG, SCHEMA, Optional.empty()),
                        new BenchmarkQuery("Q2", "SELECT 2", CATALOG, SCHEMA, Optional.empty()),
                        new BenchmarkQuery("Q3", "SELECT 3", CATALOG, SCHEMA, Optional.of(phaseSessionProperties))));

        assertEquals(expected, actual);
    }
}
