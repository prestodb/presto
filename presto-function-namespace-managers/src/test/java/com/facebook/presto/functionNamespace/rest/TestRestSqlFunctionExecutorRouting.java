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
package com.facebook.presto.functionNamespace.rest;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.RemoteScalarFunctionImplementation;
import com.facebook.presto.spi.function.RestFunctionHandle;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.page.PagesSerde;
import com.google.common.collect.ImmutableList;
import com.google.common.net.MediaType;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.testing.TestingResponse.mockResponse;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.function.FunctionImplementationType.REST;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Language.CPP;
import static com.facebook.presto.spi.page.PagesSerdeUtil.writeSerializedPage;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRestSqlFunctionExecutorRouting
{
    private static final MediaType PRESTO_PAGES = MediaType.create("application", "X-presto-pages");

    private RestSqlFunctionExecutor executor;
    private AtomicReference<Request> capturedRequest;
    private RestBasedFunctionNamespaceManagerConfig config;

    @BeforeMethod
    public void setup()
    {
        capturedRequest = new AtomicReference<>();
        config = new RestBasedFunctionNamespaceManagerConfig()
                .setRestUrl("http://default-server.example.com");

        HttpClient httpClient = new TestingHttpClient(request -> {
            capturedRequest.set(request);
            // Return a valid response
            PagesSerde pagesSerde = new PagesSerde(new BlockEncodingManager(), Optional.empty(), Optional.empty(), Optional.empty());
            PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 42);
            Page resultPage = pageBuilder.build();

            DynamicSliceOutput sliceOutput = new DynamicSliceOutput(resultPage.getPositionCount());
            writeSerializedPage(sliceOutput, pagesSerde.serialize(resultPage));
            return mockResponse(OK, PRESTO_PAGES, sliceOutput.slice().toStringUtf8());
        });

        executor = new RestSqlFunctionExecutor(config, httpClient);
        executor.setBlockEncodingSerde(new BlockEncodingManager());
    }

    @Test
    public void testRoutesToDefaultServerWhenNoExecutionEndpoint()
            throws Exception
    {
        SqlFunctionId functionId = new SqlFunctionId(
                QualifiedObjectName.valueOf("test.schema.function"),
                ImmutableList.of(parseTypeSignature("bigint")));

        Signature signature = new Signature(
                QualifiedObjectName.valueOf("test.schema.function"),
                FunctionKind.SCALAR,
                parseTypeSignature("bigint"),
                ImmutableList.of(parseTypeSignature("bigint")));

        RestFunctionHandle handle = new RestFunctionHandle(
                functionId,
                "1.0",
                signature);  // No execution endpoint

        RemoteScalarFunctionImplementation implementation = new RemoteScalarFunctionImplementation(
                handle,
                CPP,
                REST);

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));
        pageBuilder.declarePosition();
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 10);
        Page input = pageBuilder.build();

        SqlFunctionResult result = executor.executeFunction(
                "test-source",
                implementation,
                input,
                ImmutableList.of(0),
                ImmutableList.of(BIGINT),
                BIGINT).get();

        assertTrue(result.getResult().getPositionCount() == 1);
        assertEquals(BIGINT.getLong(result.getResult(), 0), 42L);
        assertNotNull(capturedRequest.get());
        URI uri = capturedRequest.get().getUri();
        assertEquals(uri.getHost(), "default-server.example.com");
        assertTrue(uri.getPath().contains("/v1/functions/schema/function/"));
    }

    @Test
    public void testRoutesToCustomExecutionEndpoint()
            throws Exception
    {
        SqlFunctionId functionId = new SqlFunctionId(
                QualifiedObjectName.valueOf("test.schema.function"),
                ImmutableList.of(parseTypeSignature("bigint")));

        Signature signature = new Signature(
                QualifiedObjectName.valueOf("test.schema.function"),
                FunctionKind.SCALAR,
                parseTypeSignature("bigint"),
                ImmutableList.of(parseTypeSignature("bigint")));

        RestFunctionHandle handle = new RestFunctionHandle(
                functionId,
                "1.0",
                signature,
                Optional.of(URI.create("https://compute-cluster-1.example.com")));

        RemoteScalarFunctionImplementation implementation = new RemoteScalarFunctionImplementation(
                handle,
                CPP,
                REST);

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));
        pageBuilder.declarePosition();
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 10);
        Page input = pageBuilder.build();

        SqlFunctionResult result = executor.executeFunction(
                "test-source",
                implementation,
                input,
                ImmutableList.of(0),
                ImmutableList.of(BIGINT),
                BIGINT).get();

        assertTrue(result.getResult().getPositionCount() == 1);
        assertEquals(BIGINT.getLong(result.getResult(), 0), 42L);
        assertNotNull(capturedRequest.get());
        URI uri = capturedRequest.get().getUri();
        assertEquals(uri.getScheme(), "https");
        assertEquals(uri.getHost(), "compute-cluster-1.example.com");
        assertTrue(uri.getPath().contains("/v1/functions/schema/function/"));
    }

    @Test
    public void testMultipleFunctionsRouteToDifferentServers()
            throws Exception
    {
        // Test that different functions can route to different execution servers
        SqlFunctionId functionId1 = new SqlFunctionId(
                QualifiedObjectName.valueOf("test.schema.function1"),
                ImmutableList.of(parseTypeSignature("bigint")));

        SqlFunctionId functionId2 = new SqlFunctionId(
                QualifiedObjectName.valueOf("test.schema.function2"),
                ImmutableList.of(parseTypeSignature("bigint")));

        Signature signature1 = new Signature(
                QualifiedObjectName.valueOf("test.schema.function1"),
                FunctionKind.SCALAR,
                parseTypeSignature("bigint"),
                ImmutableList.of(parseTypeSignature("bigint")));

        Signature signature2 = new Signature(
                QualifiedObjectName.valueOf("test.schema.function2"),
                FunctionKind.SCALAR,
                parseTypeSignature("bigint"),
                ImmutableList.of(parseTypeSignature("bigint")));

        RestFunctionHandle handle1 = new RestFunctionHandle(
                functionId1,
                "1.0",
                signature1,
                Optional.of(URI.create("https://server1.example.com")));

        RestFunctionHandle handle2 = new RestFunctionHandle(
                functionId2,
                "1.0",
                signature2,
                Optional.of(URI.create("https://server2.example.com")));

        RemoteScalarFunctionImplementation implementation1 = new RemoteScalarFunctionImplementation(
                handle1,
                CPP,
                REST);

        RemoteScalarFunctionImplementation implementation2 = new RemoteScalarFunctionImplementation(
                handle2,
                CPP,
                REST);

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));
        pageBuilder.declarePosition();
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 10);
        Page input = pageBuilder.build();

        // Execute function 1
        executor.executeFunction("test-source", implementation1, input, ImmutableList.of(0), ImmutableList.of(BIGINT), BIGINT).get();
        assertEquals(capturedRequest.get().getUri().getHost(), "server1.example.com");

        // Execute function 2
        executor.executeFunction("test-source", implementation2, input, ImmutableList.of(0), ImmutableList.of(BIGINT), BIGINT).get();
        assertEquals(capturedRequest.get().getUri().getHost(), "server2.example.com");
    }
}
