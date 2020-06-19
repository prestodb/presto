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
package com.facebook.presto.benchmark;

import com.facebook.presto.Session;
import com.facebook.presto.benchmark.source.BenchmarkSuiteDao;
import com.facebook.presto.plugin.memory.MemoryPlugin;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableList;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.benchmark.source.StringToStringMapColumnMapper.MAP_CODEC;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class BenchmarkTestUtil
{
    public static final String CATALOG = "benchmark";
    public static final String SCHEMA = "default";
    public static final String XDB = "presto";

    private BenchmarkTestUtil()
    {
    }

    public static StandaloneQueryRunner setupPresto()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();
        StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(session);
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog(CATALOG, "memory");
        return queryRunner;
    }

    public static TestingMySqlServer setupMySql()
            throws Exception
    {
        TestingMySqlServer mySqlServer = new TestingMySqlServer("testuser", "testpass", ImmutableList.of(XDB));
        Handle handle = getJdbi(mySqlServer).open();
        BenchmarkSuiteDao benchmarkDao = handle.attach(BenchmarkSuiteDao.class);
        benchmarkDao.createBenchmarkSuitesTable("benchmark_suites");
        benchmarkDao.createBenchmarkQueriesTable("benchmark_queries");
        return mySqlServer;
    }

    public static Jdbi getJdbi(TestingMySqlServer mySqlServer)
    {
        return Jdbi.create(mySqlServer.getJdbcUrl(XDB)).installPlugin(new SqlObjectPlugin());
    }

    public static void insertBenchmarkQuery(Handle handle, String querySet, String name, String query, Optional<Map<String, String>> sessionProperties)
    {
        handle.execute(
                "INSERT INTO benchmark_queries(\n" +
                        "    `query_set`, `name`, `catalog`, `schema`, `query`, `session_properties`)\n" +
                        "SELECT\n" +
                        "    ?,\n" +
                        "    ?,\n" +
                        "    'benchmark',\n" +
                        "    'default',\n" +
                        "    ?\n," +
                        "    ?\n",
                querySet,
                name,
                query,
                sessionProperties.map(MAP_CODEC::toJson).orElse(null));
    }

    public static void insertBenchmarkSuite(Handle handle, String suite, String querySet, String phases, String sessionProperties)
    {
        handle.execute(
                "INSERT INTO benchmark_suites(\n" +
                        "    `suite`, `query_set`, `phases`, `session_properties`, `created_by`)\n" +
                        "SELECT\n" +
                        "    ?,\n" +
                        "    ?,\n" +
                        "    ?,\n" +
                        "    ?,\n" +
                        "    'benchmark'\n",
                suite,
                querySet,
                phases,
                sessionProperties);
    }
}
