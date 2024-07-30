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
package com.facebook.presto.functionNamespace;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutorsModule;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManagerModule;
import com.facebook.presto.spi.function.AggregationFunctionMetadata;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;

public class TestRestBasedFunctionNamespaceManager
{
    private RestBasedFunctionNamespaceManager functionNamespaceManager;
    private RestFunctionNamespaceServer server;
    public static final String TEST_CATALOG = "unittest";

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        // Start the server
        server = new RestFunctionNamespaceServer();
        server.startServer();

        functionNamespaceManager = createFunctionNamespaceManager();
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        server.stopServer();
    }

    private RestBasedFunctionNamespaceManager createFunctionNamespaceManager()
    {
        Bootstrap app = new Bootstrap(
                new RestBasedFunctionNamespaceManagerModule(TEST_CATALOG),
                new NoopSqlFunctionExecutorsModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(
                        ImmutableMap.of(
                                "rest-based-function-manager.rest.url", "http://127.0.0.1:7777",
                                "supported-function-languages", "REST",
                                "function-implementation-type", "REST")
                ).initialize();
        return injector.getInstance(RestBasedFunctionNamespaceManager.class);
    }

    @Test
    public void testLoadFunctions()
    {
        Collection<SqlInvokedFunction> functionList = functionNamespaceManager.listFunctions(Optional.empty(), Optional.empty());
        assertEquals(functionList.size(), 9);
    }

    public static class RestFunctionNamespaceServer
    {
        private Server server;

        public void startServer()
                throws Exception
        {
            server = new Server(7777);
            ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.SESSIONS);
            handler.setContextPath("/");
            server.setHandler(handler);
            handler.addServlet(new ServletHolder(new FunctionNamespaceServlet()), "/v1/info/functionSignatures");
            server.start();
        }

        public void stopServer()
                throws Exception
        {
            if (server != null && server.isRunning()) {
                server.stop();
            }
        }

        public static Map<String, List<JsonBasedUdfFunctionMetadata>> createUdfSignatureMap()
        {
            Map<String, List<JsonBasedUdfFunctionMetadata>> udfSignatureMap = new HashMap<>();

            // square function
            List<JsonBasedUdfFunctionMetadata> squareFunctions = new ArrayList<>();
            squareFunctions.add(new JsonBasedUdfFunctionMetadata(
                    "square an integer",
                    FunctionKind.SCALAR,
                    new TypeSignature("integer"),
                    Collections.singletonList(new TypeSignature("integer")),
                    "test_schema",
                    new RoutineCharacteristics(RoutineCharacteristics.Language.REST, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.empty()));
            squareFunctions.add(new JsonBasedUdfFunctionMetadata(
                    "square a double",
                    FunctionKind.SCALAR,
                    new TypeSignature("double"),
                    Collections.singletonList(new TypeSignature("double")),
                    "test_schema",
                    new RoutineCharacteristics(RoutineCharacteristics.Language.REST, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.empty()));
            udfSignatureMap.put("square", squareFunctions);

            // array_function_1
            List<JsonBasedUdfFunctionMetadata> arrayFunction1 = new ArrayList<>();
            arrayFunction1.add(new JsonBasedUdfFunctionMetadata(
                    "combines two string arrays into one",
                    FunctionKind.SCALAR,
                    parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>"),
                    Arrays.asList(parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>"), parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>")),
                    "test_schema",
                    new RoutineCharacteristics(RoutineCharacteristics.Language.REST, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.empty()));
            arrayFunction1.add(new JsonBasedUdfFunctionMetadata(
                    "combines two float arrays into one",
                    FunctionKind.SCALAR,
                    parseTypeSignature("ARRAY<ARRAY<BIGINT>>"),
                    Arrays.asList(parseTypeSignature("ARRAY<ARRAY<BIGINT>>"), parseTypeSignature("ARRAY<ARRAY<BIGINT>>")),
                    "test_schema",
                    new RoutineCharacteristics(RoutineCharacteristics.Language.REST, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.empty()));
            arrayFunction1.add(new JsonBasedUdfFunctionMetadata(
                    "combines two double arrays into one",
                    FunctionKind.SCALAR,
                    parseTypeSignature("ARRAY<DOUBLE>"),
                    Arrays.asList(parseTypeSignature("ARRAY<DOUBLE>"), TypeSignature.parseTypeSignature("ARRAY<DOUBLE>")),
                    "test_schema",
                    new RoutineCharacteristics(RoutineCharacteristics.Language.REST, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.empty()));
            udfSignatureMap.put("array_function_1", arrayFunction1);

            // array_function_2
            List<JsonBasedUdfFunctionMetadata> arrayFunction2 = new ArrayList<>();
            arrayFunction2.add(new JsonBasedUdfFunctionMetadata(
                    "transforms inputs into the output",
                    FunctionKind.SCALAR,
                    TypeSignature.parseTypeSignature("ARRAY<map<BIGINT, DOUBLE>>"),
                    Arrays.asList(TypeSignature.parseTypeSignature("ARRAY<map<BIGINT, DOUBLE>>"), TypeSignature.parseTypeSignature("ARRAY<varchar>")),
                    "test_schema",
                    new RoutineCharacteristics(RoutineCharacteristics.Language.REST, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.empty()));
            arrayFunction2.add(new JsonBasedUdfFunctionMetadata(
                    "transforms inputs into the output",
                    FunctionKind.SCALAR,
                    TypeSignature.parseTypeSignature("ARRAY<map<BIGINT, DOUBLE>>"),
                    Arrays.asList(TypeSignature.parseTypeSignature("ARRAY<map<BIGINT, DOUBLE>>"), TypeSignature.parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>"), TypeSignature.parseTypeSignature("ARRAY<varchar>")),
                    "test_schema",
                    new RoutineCharacteristics(RoutineCharacteristics.Language.REST, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.empty()));
            udfSignatureMap.put("array_function_2", arrayFunction2);

            // avg function
            List<JsonBasedUdfFunctionMetadata> avgFunctions = new ArrayList<>();
            avgFunctions.add(new JsonBasedUdfFunctionMetadata(
                    "Returns mean of integers",
                    FunctionKind.AGGREGATE,
                    new TypeSignature("int"),
                    Collections.singletonList(new TypeSignature("int")),
                    "test_schema",
                    new RoutineCharacteristics(RoutineCharacteristics.Language.REST, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.of(new AggregationFunctionMetadata(parseTypeSignature("ROW(bigint, int)"), false))));
            avgFunctions.add(new JsonBasedUdfFunctionMetadata(
                    "Returns mean of doubles",
                    FunctionKind.AGGREGATE,
                    new TypeSignature("double"),
                    Collections.singletonList(new TypeSignature("double")),
                    "test_schema",
                    new RoutineCharacteristics(RoutineCharacteristics.Language.REST, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                    Optional.of(new AggregationFunctionMetadata(parseTypeSignature("ROW(double, int)"), false))));
            udfSignatureMap.put("avg", avgFunctions);

            return udfSignatureMap;
        }

        public static String toJson(Map<String, List<JsonBasedUdfFunctionMetadata>> udfFunctionSignatureMap)
                throws IOException
        {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new Jdk8Module());
            mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
            return mapper.writeValueAsString(udfFunctionSignatureMap);
        }

        public static class FunctionNamespaceServlet
                extends HttpServlet
        {
            private final Map<String, List<JsonBasedUdfFunctionMetadata>> udfFunctionSignatureMap = createUdfSignatureMap();

            @Override
            protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                    throws IOException
            {
                resp.setContentType("application/json");
                resp.setStatus(HttpServletResponse.SC_OK);
                resp.getWriter().write(toJson(udfFunctionSignatureMap));
            }
        }
    }
}
