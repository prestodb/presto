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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.facebook.presto.verifier.framework.QueryConfiguration;
import com.facebook.presto.verifier.framework.SourceQuery;
import com.facebook.presto.verifier.framework.VerifierConfig;
import com.facebook.presto.verifier.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.verifier.prestoaction.QueryActionsModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.verifier.VerifierTestUtil.CATALOG;
import static com.facebook.presto.verifier.VerifierTestUtil.SCHEMA;
import static com.facebook.presto.verifier.VerifierTestUtil.setupPresto;
import static com.google.inject.Scopes.SINGLETON;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class TestPrestoQuerySourceQuerySupplier
{
    private static final Logger log = Logger.get(TestPrestoQuerySourceQuerySupplier.class);
    private static final String SOURCE_FETCHING_QUERY = "SELECT\n" +
            "    'test' suite,\n" +
            "    name,\n" +
            "    query control_query,\n" +
            "    'catalog' control_catalog,\n" +
            "    'schema' control_schema,\n" +
            "    'user' control_username,\n" +
            "    '{\"a\": \"b\"}' control_session_properties,\n" +
            "    NULL control_password,\n" +
            "    query test_query,\n" +
            "    'catalog' test_catalog,\n" +
            "    'schema' test_schema,\n" +
            "    'user' test_username,\n" +
            "    NULL test_password,\n" +
            "    '{\"c\": \"d\"}' test_session_properties\n" +
            "FROM (\n" +
            "    VALUES\n" +
            "        ('Q1', 'SELECT 1'),\n" +
            "        ('Q2', 'INSERT INTO test_table SELECT 1')\n" +
            ") queries(name, query)";
    private static final QueryConfiguration CONTROL_CONFIGURATION = new QueryConfiguration(
            "catalog", "schema", Optional.of("user"), Optional.empty(), Optional.of(ImmutableMap.of("a", "b")));
    private static final QueryConfiguration TEST_CONFIGURATION = new QueryConfiguration(
            "catalog", "schema", Optional.of("user"), Optional.empty(), Optional.of(ImmutableMap.of("c", "d")));
    private static final List<SourceQuery> SOURCE_QUERIES = ImmutableList.of(
            new SourceQuery("test", "Q1", "SELECT 1", "SELECT 1", CONTROL_CONFIGURATION, TEST_CONFIGURATION),
            new SourceQuery("test", "Q2", "INSERT INTO test_table SELECT 1", "INSERT INTO test_table SELECT 1", CONTROL_CONFIGURATION, TEST_CONFIGURATION));

    private static StandaloneQueryRunner queryRunner;
    private static Injector injector;

    @BeforeClass
    public void setup()
            throws Exception
    {
        queryRunner = setupPresto();

        String host = queryRunner.getServer().getAddress().getHost();
        int port = queryRunner.getServer().getAddress().getPort();

        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                        .add(new SourceQueryModule(ImmutableSet.of()))
                        .add(new QueryActionsModule(PrestoExceptionClassifier.defaultBuilder().build(), ImmutableSet.of()))
                        .add(binder -> {
                            configBinder(binder).bindConfig(VerifierConfig.class);
                            binder.bind(SqlParserOptions.class).toInstance(new SqlParserOptions());
                            binder.bind(SqlParser.class).in(SINGLETON);
                        })
                        .build());
        injector = app
                .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                        .put("test-id", "10000")
                        .put("control.hosts", format("%s,%s", host, host))
                        .put("control.jdbc-port", String.valueOf(port))
                        .put("source-query.supplier", "presto-query")
                        .put("source-query.query", SOURCE_FETCHING_QUERY)
                        .put("source-query.catalog", CATALOG)
                        .put("source-query.schema", SCHEMA)
                        .put("source-query.username", "test_user")
                        .build())
                .initialize();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        closeQuietly(queryRunner);
        if (injector != null) {
            try {
                injector.getInstance(LifeCycleManager.class).stop();
            }
            catch (Throwable t) {
                log.error(t);
            }
        }
    }

    @Test
    public void testSupplyQueries()
    {
        SourceQuerySupplier sourceQuerySupplier = injector.getInstance(SourceQuerySupplier.class);
        assertEquals(sourceQuerySupplier.get(), SOURCE_QUERIES);
    }
}
