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
package com.facebook.presto.tests;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.jaxrs.JsonMapper;
import com.facebook.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.IntArrayBlockBuilder;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.functionNamespace.ForRestServer;
import com.facebook.presto.functionNamespace.rest.RestSqlFunctionExecutor;
import com.facebook.presto.functionNamespace.rest.RestSqlFunctionExecutorsModule;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.RemoteScalarFunctionImplementation;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.testing.TestingNodeManager;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_ABS_INT;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_ARRAY_SUM;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_BOOL_AND;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_POWER_TOWER_DOUBLE_UPDATED;
import static com.facebook.presto.functionNamespace.testing.SqlInvokedFunctionTestUtils.FUNCTION_REV_STRING;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPage;
import static com.facebook.presto.spi.page.PagesSerdeUtil.writeSerializedPage;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Double.longBitsToDouble;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRestSqlFunctionExecutor
{
    @DataProvider(name = "testExecuteFunctionAbs")
    public Object[][] valuesForAbs()
    {
        return new Object[][] {{0L, 0L}, {-3L, 3L}, {3L, 3L}};
    }
    @DataProvider(name = "testExecuteFunctionPow")
    public Object[][] valuesForPow()
    {
        return new Object[][] {{3.0, 27.0}, {2.0, 4.0}, {-2.0, 0.25}, {0.0, 1.0}};
    }
    @DataProvider(name = "testExecuteFunctionReverseString")
    public Object[][] valuesForReverseString()
    {
        return new Object[][] {{"inputValue", "eulaVtupni"}, {"", ""}, {"abc", "cba"}, {"#$123([]}Aa", "aA}][(321$#"}};
    }
    @DataProvider(name = "testExecuteFunctionArraySum")
    public Object[][] valuesForArraySum()
    {
        return new Object[][] {{2, 3, 5L}, {1, 1, 2L}, {-4, 0, -4L}, {-3, -2, -5L}};
    }
    public static final URI REST_SERVER_URI = URI.create("http://127.0.0.1:7777");
    private RestSqlFunctionExecutor restSqlFunctionExecutor;

    @BeforeMethod
    public void setUp() throws Exception
    {
        // Set up the mock HTTP endpoint that delegates to the Java base Presto Page
        TestingFunctionResource testingFunctionResource = new TestingFunctionResource(createPagesSerde());
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        JaxrsTestingHttpProcessor jaxrsTestingHttpProcessor = new JaxrsTestingHttpProcessor(
                UriBuilder.fromUri(REST_SERVER_URI).path("/").build(),
                testingFunctionResource,
                new JsonMapper(mapper));
        TestingHttpClient testingHttpClient = new TestingHttpClient(jaxrsTestingHttpProcessor);

        restSqlFunctionExecutor = createRestSqlFunctionExecutor(testingHttpClient);
    }

    private RestSqlFunctionExecutor createRestSqlFunctionExecutor(HttpClient testingHttpClient)
    {
        Bootstrap app = new Bootstrap(
                // Specially use a testing HTTP client instead of a real one
                binder -> binder.bind(HttpClient.class).annotatedWith(ForRestServer.class).toInstance(testingHttpClient),
                binder -> binder.bind(NodeManager.class).toInstance(createNodeManager()),
                // Otherwise use the exact same module as the rest sql function executor service
                new RestSqlFunctionExecutorsModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(
                        ImmutableMap.of(
                                "rest-based-function-manager.rest.url", REST_SERVER_URI.toString())
                ).initialize();
        return injector.getInstance(RestSqlFunctionExecutor.class);
    }

    @Path("/v1/functions/{schema}/{functionName}/{functionId}/{version}")
    //@PATH("http://127.0.0.1:7777/v1/functions/memory/abs/unittest.memory.abs%5Binteger%5D/123")
    public static class TestingFunctionResource
    {
        private final PagesSerde pagesSerde;
        public TestingFunctionResource(PagesSerde pagesSerde)
        {
            this.pagesSerde = requireNonNull(pagesSerde);
        }

        @POST
        @Consumes(MediaType.WILDCARD)
        @Produces(MediaType.WILDCARD)
        public byte[] post(
                @PathParam("schema") String schema,
                @PathParam("functionName") String functionName,
                @PathParam("functionId") String functionId,
                @PathParam("version") String version,
                byte[] serializedPageByteArray)
        {
            Slice slice = wrappedBuffer(serializedPageByteArray);
            SerializedPage serializedPage = readSerializedPage(new BasicSliceInput((slice)));
            Page inputPage = pagesSerde.deserialize(serializedPage);
            List<Type> types = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            switch (functionName) {
                case "power_tower":
                    double inputValue = longBitsToDouble(inputPage.getBlock(0).toLong(0));
                    types.add(DoubleType.DOUBLE);
                    values.add(Math.pow(inputValue, inputValue));
                    break;
                case "abs":
                    types.add(IntegerType.INTEGER);
                    values.add(Math.abs(inputPage.getBlock(0).toLong(0)));
                    break;
                case "bool_and":
                    types.add(BooleanType.BOOLEAN);
                    long result = 1;
                    for (int i = 0; i < inputPage.getChannelCount(); i++) {
                        result *= inputPage.getBlock(i).toLong(0);
                    }
                    values.add(result == 1L);
                    break;
                case "reverse":
                    types.add(VarcharType.VARCHAR);
                    int length = inputPage.getBlock(0).getSliceLength(0);
                    String inputString = inputPage.getBlock(0).getSlice(0, 0, length).toStringUtf8();
                    String reverse = "";
                    for (int i = inputString.length() - 1; i >= 0; i--) {
                        reverse += inputString.charAt(i);
                    }
                    values.add(reverse);
                    break;
                case "array_sum":
                    types.add(IntegerType.INTEGER);
                    long val1 = inputPage.getBlock(0).getBlock(0).getInt(0);
                    long val2 = inputPage.getBlock(0).getBlock(0).getInt(1);
                    values.add(val1 + val2);
                    break;
            }
            Page outputPage = createPage(types, values);
            DynamicSliceOutput sliceOutput = new DynamicSliceOutput((int) outputPage.getRetainedSizeInBytes());
            writeSerializedPage(sliceOutput, pagesSerde.serialize(outputPage));
            return sliceOutput.slice().byteArray();
        }
    }

    @Test(dataProvider = "testExecuteFunctionAbs")
    public void testRestSqlFunctionExecutor(Object inputValue, Object expected)
            throws ExecutionException, InterruptedException
    {
        restSqlFunctionExecutor.setBlockEncodingSerde(new BlockEncodingManager());
        ArrayList<Type> types = new ArrayList<>();
        ArrayList<Object> arguments = new ArrayList<>();
        types.add(IntegerType.INTEGER);
        arguments.add(inputValue);
        Type returnType = IntegerType.INTEGER;

        CompletableFuture<SqlFunctionResult> output = restSqlFunctionExecutor.executeFunction(
                "",
                createRemoteScalarFunctionImplementation(FUNCTION_ABS_INT),
                createPage(types, arguments),
                new ArrayList<>(),
                types,
                returnType);
        assertEquals(returnType.getLong(output.get().getResult(), 0), expected);
    }

    @Test(dataProvider = "testExecuteFunctionPow")
    public void testRestSqlFunctionExecutorPOW(Object inputValue, Object expectedValue)
            throws ExecutionException, InterruptedException
    {
        restSqlFunctionExecutor.setBlockEncodingSerde(new BlockEncodingManager());
        ArrayList<Type> types = new ArrayList<>();
        ArrayList<Object> arguments = new ArrayList<>();
        types.add(DoubleType.DOUBLE);
        arguments.add(inputValue);
        Type returnType = DoubleType.DOUBLE;

        CompletableFuture<SqlFunctionResult> output = restSqlFunctionExecutor.executeFunction(
                "",
                createRemoteScalarFunctionImplementation(FUNCTION_POWER_TOWER_DOUBLE_UPDATED),
                createPage(types, arguments),
                new ArrayList<>(),
                types,
                returnType);
        assertEquals(returnType.getDouble(output.get().getResult(), 0), expectedValue);
    }

    @Test
    public void testRestSqlFunctionExecutorBoolAnd()
            throws ExecutionException, InterruptedException
    {
        restSqlFunctionExecutor.setBlockEncodingSerde(new BlockEncodingManager());
        ArrayList<Type> types = new ArrayList<>();
        ArrayList<Object> arguments = new ArrayList<>();
        types.add(BooleanType.BOOLEAN);
        arguments.add(true);
        Type returnType = BooleanType.BOOLEAN;

        CompletableFuture<SqlFunctionResult> output = restSqlFunctionExecutor.executeFunction(
                "",
                createRemoteScalarFunctionImplementation(FUNCTION_BOOL_AND),
                createPage(types, arguments),
                new ArrayList<>(),
                types,
                returnType);

        types.add(BooleanType.BOOLEAN);
        arguments.add(false);

        CompletableFuture<SqlFunctionResult> output2 = restSqlFunctionExecutor.executeFunction(
                "",
                createRemoteScalarFunctionImplementation(FUNCTION_BOOL_AND),
                createPage(types, arguments),
                new ArrayList<>(),
                types,
                returnType);

        assertTrue(returnType.getBoolean(output.get().getResult(), 0));
        assertFalse(returnType.getBoolean(output2.get().getResult(), 0));
    }

    @Test(dataProvider = "testExecuteFunctionReverseString")
    public void testRestSqlFunctionExecutorReverseString(Object input, Object expected)
            throws ExecutionException, InterruptedException
    {
        restSqlFunctionExecutor.setBlockEncodingSerde(new BlockEncodingManager());
        ArrayList<Type> types = new ArrayList<>();
        ArrayList<Object> arguments = new ArrayList<>();
        types.add(VarcharType.VARCHAR);
        arguments.add(input);
        Type returnType = VarcharType.VARCHAR;

        CompletableFuture<SqlFunctionResult> output = restSqlFunctionExecutor.executeFunction(
                "",
                createRemoteScalarFunctionImplementation(FUNCTION_REV_STRING),
                createPage(types, arguments),
                new ArrayList<>(),
                types,
                returnType);
        assertEquals(returnType.getSlice(output.get().getResult(), 0).toStringUtf8(), expected);
    }

    @Test(dataProvider = "testExecuteFunctionArraySum")
    public void testRestSqlFunctionExecutorArray(Object val1, Object val2, Object expected)
            throws ExecutionException, InterruptedException
    {
        restSqlFunctionExecutor.setBlockEncodingSerde(new BlockEncodingManager());
        IntArrayBlockBuilder intArrayBlockBuilder = new IntArrayBlockBuilder(null, 2);
        intArrayBlockBuilder.writeInt((int) val1);
        intArrayBlockBuilder.writeInt((int) val2);
        ArrayList<Type> types = new ArrayList<>();
        ArrayList<Object> arguments = new ArrayList<>();
        types.add(new ArrayType(IntegerType.INTEGER));
        arguments.add(intArrayBlockBuilder);
        Type returnType = IntegerType.INTEGER;

        CompletableFuture<SqlFunctionResult> output = restSqlFunctionExecutor.executeFunction(
                "",
                createRemoteScalarFunctionImplementation(FUNCTION_ARRAY_SUM),
                createPage(types, arguments),
                new ArrayList<>(),
                types,
                returnType);
        assertEquals(returnType.getLong(output.get().getResult(), 0), expected);
    }

    private NodeManager createNodeManager()
    {
        InternalNode internalNode = new InternalNode("test", REST_SERVER_URI, NodeVersion.UNKNOWN, false, false, false, true);
        return new TestingNodeManager("testenv", internalNode, ImmutableSet.of());
    }

    private RemoteScalarFunctionImplementation createRemoteScalarFunctionImplementation(SqlInvokedFunction sqlInvokedFunction)
    {
        SqlFunctionHandle sqlFunctionHandle = new SqlFunctionHandle(sqlInvokedFunction.getFunctionId(), "123");
        return new RemoteScalarFunctionImplementation(sqlFunctionHandle, RoutineCharacteristics.Language.CPP, FunctionImplementationType.CPP);
    }

    private static PagesSerde createPagesSerde()
    {
        return new PagesSerde(new BlockEncodingManager(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static Page createPage(List<Type> types, List<Object> arguments)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        pageBuilder.declarePosition();
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            BlockBuilder output = pageBuilder.getBlockBuilder(i);
            switch (type.getTypeSignature().getBase()) {
                case "integer":
                case "bigint":
                case "smallint":
                case "tinyint":
                case "real":
                case "interval day to second":
                case "interval year to month":
                case "timestamp":
                case "time":
                    type.writeLong(output, (Long) arguments.get(i));
                    break;
                case "double":
                    type.writeDouble(output, (Double) arguments.get(i));
                    break;
                case "boolean":
                    type.writeBoolean(output, (Boolean) arguments.get(i));
                    break;
                case "varchar":
                case "char":
                case "varbinary":
                case "json":
                    type.writeSlice(output, Slices.utf8Slice(arguments.get(i).toString()));
                    break;
                case "decimal":
                    BigDecimal bd = (BigDecimal) arguments.get(i);
                    type.writeSlice(output, Decimals.encodeScaledValue(bd));
                    break;
                case "object":
                case "array":
                case "row":
                case "map":
                    type.writeObject(output, arguments.get(i));
                    break;
            }
        }
        return pageBuilder.build();
    }
}
