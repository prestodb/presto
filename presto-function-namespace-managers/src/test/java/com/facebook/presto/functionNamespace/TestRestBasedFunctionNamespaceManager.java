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
import com.facebook.airlift.jaxrs.JsonMapper;
import com.facebook.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.functionNamespace.execution.NoopSqlFunctionExecutorsModule;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManager;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManagerModule;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestRestBasedFunctionNamespaceManager
{
    private RestBasedFunctionNamespaceManager functionNamespaceManager;
    private TestingFunctionResource resource;
    public static final String TEST_CATALOG = "unittest";
    public static final URI REST_SERVER_URI = URI.create("http://127.0.0.1:1122");

    @Path("/v1/functions")
    public static class TestingFunctionResource
    {
        private Map<String, List<JsonBasedUdfFunctionMetadata>> functions;
        private String etag;

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

        @GET
        @Path("/{schema}/{functionName}")
        @Produces(MediaType.APPLICATION_JSON)
        public Map<String, List<JsonBasedUdfFunctionMetadata>> getFunctionsBySchemaAndName(@PathParam("schema") String schema, @PathParam("functionName") String functionName)
        {
            Map<String, List<JsonBasedUdfFunctionMetadata>> result = new HashMap<>();
            List<JsonBasedUdfFunctionMetadata> functionList = functions.get(functionName);

            if (functionList != null) {
                List<JsonBasedUdfFunctionMetadata> filteredList = new ArrayList<>();
                for (JsonBasedUdfFunctionMetadata metadata : functionList) {
                    if (metadata.getSchema().equals(schema)) {
                        filteredList.add(metadata);
                    }
                }
                if (!filteredList.isEmpty()) {
                    result.put(functionName, filteredList);
                }
            }
            return result;
        }

        @HEAD
        public Response getFunctionHeaders()
        {
            return Response.ok()
                    .header("ETag", etag)
                    .build();
        }

        public void updateFunctions(Map<String, List<JsonBasedUdfFunctionMetadata>> newFunctions)
        {
            this.functions = newFunctions;
        }

        public void updateETag(String newETag)
        {
            this.etag = newETag;
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
                "default",
                false,
                new RoutineCharacteristics(RoutineCharacteristics.Language.CPP, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(new SqlFunctionId(QualifiedObjectName.valueOf("unittest.default.square"), ImmutableList.of(parseTypeSignature("integer")))),
                Optional.of("1"),
                Optional.of(emptyList()),
                Optional.of(emptyList())));
        squareFunctions.add(new JsonBasedUdfFunctionMetadata(
                "square a double",
                FunctionKind.SCALAR,
                new TypeSignature("double"),
                Collections.singletonList(new TypeSignature("double")),
                "test_schema",
                false,
                new RoutineCharacteristics(RoutineCharacteristics.Language.CPP, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(new SqlFunctionId(QualifiedObjectName.valueOf("unittest.test_schema.square"), ImmutableList.of(parseTypeSignature("double")))),
                Optional.of("1"),
                Optional.of(emptyList()),
                Optional.of(emptyList())));
        udfSignatureMap.put("square", squareFunctions);

        // array_function_1
        List<JsonBasedUdfFunctionMetadata> arrayFunction1 = new ArrayList<>();
        arrayFunction1.add(new JsonBasedUdfFunctionMetadata(
                "combines two string arrays into one",
                FunctionKind.SCALAR,
                parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>"),
                Arrays.asList(parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>"), parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>")),
                "default",
                false,
                new RoutineCharacteristics(RoutineCharacteristics.Language.CPP, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(new SqlFunctionId(QualifiedObjectName.valueOf("unittest.default.array_function_1"), ImmutableList.of(parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>"), parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>")))),
                Optional.of("1"),
                Optional.of(emptyList()),
                Optional.of(emptyList())));
        arrayFunction1.add(new JsonBasedUdfFunctionMetadata(
                "combines two float arrays into one",
                FunctionKind.SCALAR,
                parseTypeSignature("ARRAY<ARRAY<BIGINT>>"),
                Arrays.asList(parseTypeSignature("ARRAY<ARRAY<BIGINT>>"), parseTypeSignature("ARRAY<ARRAY<BIGINT>>")),
                "test_schema",
                false,
                new RoutineCharacteristics(RoutineCharacteristics.Language.CPP, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(new SqlFunctionId(QualifiedObjectName.valueOf("unittest.test_schema.array_function_1"), ImmutableList.of(parseTypeSignature("ARRAY<ARRAY<BIGINT>>"), parseTypeSignature("ARRAY<ARRAY<BIGINT>>")))),
                Optional.of("1"),
                Optional.of(emptyList()),
                Optional.of(emptyList())));
        arrayFunction1.add(new JsonBasedUdfFunctionMetadata(
                "combines two double arrays into one",
                FunctionKind.SCALAR,
                parseTypeSignature("ARRAY<DOUBLE>"),
                Arrays.asList(parseTypeSignature("ARRAY<DOUBLE>"), TypeSignature.parseTypeSignature("ARRAY<DOUBLE>")),
                "test_schema",
                false,
                new RoutineCharacteristics(RoutineCharacteristics.Language.CPP, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(new SqlFunctionId(QualifiedObjectName.valueOf("unittest.test_schema.array_function_1"), ImmutableList.of(parseTypeSignature("ARRAY<DOUBLE>"), parseTypeSignature("ARRAY<DOUBLE>")))),
                Optional.of("1"),
                Optional.of(emptyList()),
                Optional.of(emptyList())));
        udfSignatureMap.put("array_function_1", arrayFunction1);

        // array_function_2
        List<JsonBasedUdfFunctionMetadata> arrayFunction2 = new ArrayList<>();
        arrayFunction2.add(new JsonBasedUdfFunctionMetadata(
                "transforms inputs into the output",
                FunctionKind.SCALAR,
                TypeSignature.parseTypeSignature("ARRAY<map<BIGINT, DOUBLE>>"),
                Arrays.asList(TypeSignature.parseTypeSignature("ARRAY<map<BIGINT, DOUBLE>>"), TypeSignature.parseTypeSignature("ARRAY<varchar>")),
                "default",
                false,
                new RoutineCharacteristics(RoutineCharacteristics.Language.CPP, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(new SqlFunctionId(QualifiedObjectName.valueOf("unittest.default.array_function_2"), ImmutableList.of(parseTypeSignature("ARRAY<map<BIGINT, DOUBLE>>"), parseTypeSignature("ARRAY<varchar>")))),
                Optional.of("1"),
                Optional.of(emptyList()),
                Optional.of(emptyList())));
        arrayFunction2.add(new JsonBasedUdfFunctionMetadata(
                "transforms inputs into the output",
                FunctionKind.SCALAR,
                TypeSignature.parseTypeSignature("ARRAY<map<BIGINT, DOUBLE>>"),
                Arrays.asList(TypeSignature.parseTypeSignature("ARRAY<map<BIGINT, DOUBLE>>"), TypeSignature.parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>"), TypeSignature.parseTypeSignature("ARRAY<varchar>")),
                "test_schema",
                false,
                new RoutineCharacteristics(RoutineCharacteristics.Language.CPP, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(new SqlFunctionId(QualifiedObjectName.valueOf("unittest.test_schema.array_function_2"), ImmutableList.of(parseTypeSignature("ARRAY<map<BIGINT, DOUBLE>>"), parseTypeSignature("ARRAY<ARRAY<BOOLEAN>>"), parseTypeSignature("ARRAY<varchar>")))),
                Optional.of("1"),
                Optional.of(emptyList()),
                Optional.of(emptyList())));
        udfSignatureMap.put("array_function_2", arrayFunction2);

        return udfSignatureMap;
    }

    public static Map<String, List<JsonBasedUdfFunctionMetadata>> createUpdatedUdfSignatureMap()
    {
        Map<String, List<JsonBasedUdfFunctionMetadata>> udfSignatureMap = new HashMap<>();

        // square function
        List<JsonBasedUdfFunctionMetadata> squareFunctions = new ArrayList<>();
        squareFunctions.add(new JsonBasedUdfFunctionMetadata(
                "square an integer",
                FunctionKind.SCALAR,
                new TypeSignature("integer"),
                Collections.singletonList(new TypeSignature("integer")),
                "default",
                false,
                new RoutineCharacteristics(RoutineCharacteristics.Language.CPP, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(new SqlFunctionId(QualifiedObjectName.valueOf("unittest.default.square"), ImmutableList.of(parseTypeSignature("integer")))),
                Optional.of("1"),
                Optional.of(emptyList()),
                Optional.of(emptyList())));
        squareFunctions.add(new JsonBasedUdfFunctionMetadata(
                "square a double",
                FunctionKind.SCALAR,
                new TypeSignature("double"),
                Collections.singletonList(new TypeSignature("double")),
                "test_schema",
                false,
                new RoutineCharacteristics(RoutineCharacteristics.Language.CPP, RoutineCharacteristics.Determinism.DETERMINISTIC, RoutineCharacteristics.NullCallClause.CALLED_ON_NULL_INPUT),
                Optional.empty(),
                Optional.of(new SqlFunctionId(QualifiedObjectName.valueOf("unittest.test_schema.square"), ImmutableList.of(parseTypeSignature("double")))),
                Optional.of("1"),
                Optional.of(emptyList()),
                Optional.of(emptyList())));
        udfSignatureMap.put("square", squareFunctions);

        return udfSignatureMap;
    }

    @BeforeMethod
    public void setUp() throws Exception
    {
        resource = new TestingFunctionResource(createUdfSignatureMap());
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        JaxrsTestingHttpProcessor httpProcessor = new JaxrsTestingHttpProcessor(
                UriBuilder.fromUri(REST_SERVER_URI).path("/").build(),
                resource,
                new JsonMapper(mapper));
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
                                "rest-based-function-manager.rest.url", REST_SERVER_URI.toString(),
                                "supported-function-languages", "CPP",
                                "function-implementation-type", "REST")
                ).initialize();
        return injector.getInstance(RestBasedFunctionNamespaceManager.class);
    }

    @Test
    public void testListFunctions()
    {
        // Verify list of all functions
        Collection<SqlInvokedFunction> functionList = functionNamespaceManager.listFunctions(Optional.empty(), Optional.empty());
        assertEquals(functionList.size(), 7);

        resource.updateETag("\"initial-etag\"");
        functionList = functionNamespaceManager.listFunctions(Optional.empty(), Optional.empty());
        assertEquals(functionList.size(), 7);

        // Invalidate the cached list and verify the list of all functions.
        resource.updateFunctions(createUpdatedUdfSignatureMap());
        resource.updateETag("\"updated-etag\"");
        functionList = functionNamespaceManager.listFunctions(Optional.empty(), Optional.empty());
        assertEquals(functionList.size(), 2);
    }

    @Test
    public void testFetchFunctionsDirect()
    {
        QualifiedObjectName functionName = QualifiedObjectName.valueOf("unittest.test_schema.square");
        Collection<SqlInvokedFunction> functions = functionNamespaceManager.getFunctions(Optional.empty(), functionName);
        assertEquals(functions.size(), 1);
        SqlInvokedFunction function = functions.iterator().next();
        assertEquals(function.getSignature().getName(), functionName);
        assertEquals(function.getSignature().getReturnType().getBase(), "double");
    }

    @Test
    public void testFetchFunctionImplementationDirect()
    {
        QualifiedObjectName functionName = QualifiedObjectName.valueOf("unittest.default.square");
        List<TypeSignature> argumentTypes = ImmutableList.of(parseTypeSignature("integer"));
        SqlFunctionId functionId = new SqlFunctionId(functionName, argumentTypes);
        SqlFunctionHandle functionHandle = new SqlFunctionHandle(functionId, "1");
        ScalarFunctionImplementation implementation = functionNamespaceManager.getScalarFunctionImplementation(functionHandle);
        assertNotNull(implementation);
    }
}
