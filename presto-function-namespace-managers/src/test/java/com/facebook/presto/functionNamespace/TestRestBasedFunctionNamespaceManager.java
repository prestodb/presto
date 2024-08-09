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
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutorsModule;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManagerModule;
import com.facebook.presto.spi.function.AggregationFunctionMetadata;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

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
    public static final String TEST_CATALOG = "unittest";

    @Path("/v1/info/functionSignatures")
    public static class TestingFunctionResource
    {
        private final Map<String, List<JsonBasedUdfFunctionMetadata>> functions;

        public TestingFunctionResource(Map<String, List<JsonBasedUdfFunctionMetadata>> functions)
        {
            this.functions = functions;
        }

        @GET
        @Produces(MediaType.APPLICATION_JSON)
        public Map<String, List<JsonBasedUdfFunctionMetadata>> getFunctions()
        {
            return functions;
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

    @BeforeMethod
    public void setUp() throws Exception
    {
        TestingFunctionResource resource = new TestingFunctionResource(createUdfSignatureMap());
        JaxrsTestingHttpProcessor httpProcessor = new JaxrsTestingHttpProcessor(
                UriBuilder.fromUri("http://127.0.0.1:7777").path("/v1/info/functionSignatures").build(),
                resource);
        TestingHttpClient testingHttpClient = new TestingHttpClient(httpProcessor);

        functionNamespaceManager = createFunctionNamespaceManager(testingHttpClient);
    }

    private RestBasedFunctionNamespaceManager createFunctionNamespaceManager(HttpClient httpClient)
    {
        Bootstrap app = new Bootstrap(
                // Specially use a testing HTTP client instead of a real one
                binder -> binder.bind(HttpClient.class).annotatedWith(ForRestServer.class).toInstance(httpClient),
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
        // Call the method and assert the results
        Collection<SqlInvokedFunction> functionList = functionNamespaceManager.listFunctions(Optional.empty(), Optional.empty());
        assertEquals(functionList.size(), 9);
    }
}
