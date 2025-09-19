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
package com.facebook.presto.functions;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.scalar.MapFilterFunction;
import com.facebook.presto.scalar.sql.MapNormalizeFunction;
import com.facebook.presto.scalar.sql.StringSqlFunctions;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingPageSinkProvider;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Key;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestBuiltInConnectorFunctions
{
    protected TestingPrestoServer server;
    protected TestingPrestoClient client;
    protected TypeManager typeManager;

    private static final String PLUGIN_NAME = "test-function";
    private static final String CATALOG_NAME = "test_function_catalog";
    private static final String SCHEMA_NAME = "system";
    private static final String FUNCTION_NAMESPACE = CATALOG_NAME + "." + SCHEMA_NAME;

    @BeforeClass
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        server.installPlugin(new TestConnectorWithBuiltinFunctions());
        server.createCatalog(CATALOG_NAME, PLUGIN_NAME);
        client = new TestingPrestoClient(server, testSessionBuilder()
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Bahia_Banderas"))
                .build());
        typeManager = server.getInstance(Key.get(TypeManager.class));
    }

    public void assertInvalidFunction(String expr, String exceptionPattern)
    {
        try {
            client.execute("SELECT " + expr);
            fail("Function expected to fail but not");
        }
        catch (Exception e) {
            if (!(e.getMessage().matches(exceptionPattern))) {
                fail(format("Expected exception message '%s' to match '%s' but not",
                        e.getMessage(), exceptionPattern));
            }
        }
    }

    private class TestConnectorWithBuiltinFunctions
            implements Plugin
    {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new ConnectorFactory()
            {
                @Override
                public String getName()
                {
                    return PLUGIN_NAME;
                }

                @Override
                public ConnectorHandleResolver getHandleResolver()
                {
                    return new TestingHandleResolver();
                }

                @Override
                public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                {
                    return new Connector()
                    {
                        private final ConnectorMetadata metadata = new TestingMetadata();

                        @Override
                        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                        {
                            return new ConnectorTransactionHandle() {};
                        }

                        @Override
                        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
                        {
                            return metadata;
                        }

                        @Override
                        public Set<Class<?>> getSystemFunctions()
                        {
                            return ImmutableSet.of(TestConnectorSystemFunctions.class,
                                    TestConnectorSystemFunctions.SumFunction.class,
                                    CountAggregation.class,
                                    StringSqlFunctions.class,
                                    MapNormalizeFunction.class,
                                    MapFilterFunction.class);
                        }

                        @Override
                        public ConnectorSplitManager getSplitManager()
                        {
                            return (transactionHandle, session, layout, splitSchedulingContext) -> {
                                throw new UnsupportedOperationException();
                            };
                        }

                        @Override
                        public ConnectorPageSourceProvider getPageSourceProvider()
                        {
                            return new ConnectorPageSourceProvider() {
                                @Override
                                public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, ConnectorTableLayoutHandle layout, List<ColumnHandle> columns, SplitContext splitContext, RuntimeStats runtimeStats)
                                {
                                    return new FixedPageSource(ImmutableList.of());
                                }
                            };
                        }

                        @Override
                        public ConnectorPageSinkProvider getPageSinkProvider()
                        {
                            return new TestingPageSinkProvider();
                        }
                    };
                }
            });
        }
    }

    public void check(@Language("SQL") String query, Type expectedType, Object expectedValue)
    {
        MaterializedResult result = client.execute(query).getResult();
        assertEquals(result.getRowCount(), 1);
        assertEquals(result.getTypes().get(0), expectedType);
        Object actual = result.getMaterializedRows().get(0).getField(0);
        assertEquals(actual, expectedValue);
    }

    @Test
    public void testNewFunctionNamespaceFunctions()
    {
        // Scalar functions with methods annotated with @ScalarFunction
        check("SELECT " + FUNCTION_NAMESPACE + ".modulo(10,3)", BIGINT, 1L);
        check("SELECT " + FUNCTION_NAMESPACE + ".identity('test-functions')", VARCHAR, "test-functions");

        // Scalar function with class annotated with @ScalarFunction
        check("SELECT " + FUNCTION_NAMESPACE + ".sum(10,3)", BIGINT, 13L);

        // Aggregation function
        check("SELECT " + FUNCTION_NAMESPACE + ".count(*) from (values (0), (1), (2)) T(i)", BIGINT, 3L);

        // Sql invoked scalar function with class annotated with @SqlInvokedScalarFunction
        check("SELECT " + FUNCTION_NAMESPACE + ".map_normalize(map(array['w', 'x', 'y', 'z'], array[1, 1, 1, 1]))",
                mapType(VARCHAR, DOUBLE),
                ImmutableMap.of("w", 0.25, "x", 0.25, "y", 0.25, "z", 0.25));

        // Sql invoked scalar function with method annotated with @SqlInvokedScalarFunction
        check("SELECT " + FUNCTION_NAMESPACE + ".trail('random_string_test', 4)", VARCHAR, "test");

        // Code gen scalar function
        check("SELECT " + FUNCTION_NAMESPACE + ".map_filter(map(ARRAY [5, 6, 7, 8], ARRAY [5, 6, 6, 5]), (x, y) -> x <= 6 OR y = 5)",
                mapType(INTEGER, INTEGER),
                ImmutableMap.of(5, 5, 6, 6, 8, 5));
    }

    @Test
    public void testInvalidFunctionAndNamespace()
    {
        assertInvalidFunction(CATALOG_NAME + ".namespace.modulo(10,3)", "line 1:8: Function test_function_catalog.namespace.modulo not registered");
        assertInvalidFunction(CATALOG_NAME + ".system.some_func(10)", "line 1:8: Function test_function_catalog.system.some_func not registered");
    }
}
