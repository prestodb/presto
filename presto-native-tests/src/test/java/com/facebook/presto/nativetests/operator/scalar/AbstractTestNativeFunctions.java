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
package com.facebook.presto.nativetests.operator.scalar;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logger;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.sidecar.NativeSidecarFailureInfo;
import com.facebook.presto.sidecar.expressions.TestNativeExpressionInterpreter;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.tests.operator.scalar.TestFunctions;
import com.facebook.presto.type.TypeDeserializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeQueryRunnerParameters;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertExceptionMessage;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.String.format;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestNativeFunctions
        implements TestFunctions
{
    private static final Logger log = Logger.get(TestNativeExpressionInterpreter.class);
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    public static final TypeProvider SYMBOL_TYPES = TypeProvider.viewOf(ImmutableMap.<String, Type>builder().build());
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);

    private JsonCodec<RowExpression> codec;
    private Process sidecar;
    private URI expressionUri;

    @BeforeClass
    public void init()
            throws Exception
    {
        codec = getJsonCodec();
        int port = findRandomPort();
        HttpUriBuilder sidecarUri = HttpUriBuilder.uriBuilder()
                .scheme("http")
                .host("127.0.0.1")
                .port(port);
        expressionUri = sidecarUri.appendPath("/v1/expressions").build();
        sidecar = getSidecarProcess(sidecarUri.build(), port);

        try {
            OkHttpClient client = new OkHttpClient();
            URI infoUri = sidecarUri.appendPath("/v1/info").build();
            Request request = new Request.Builder()
                    .url(infoUri.toString())
                    .header(ACCEPT, JSON_UTF_8.toString())
                    .get()
                    .build();

            long timeoutMs = 15000;
            long pollIntervalMs = 1000;
            long deadline = System.currentTimeMillis() + timeoutMs;
            boolean sidecarProcessStarted = false;

            while (System.currentTimeMillis() < deadline) {
                try {
                    Response response = client.newCall(request).execute();
                    if (response.code() != 500) {
                        sidecarProcessStarted = true;
                        break;
                    }
                }
                catch (IOException e) {
                    // ignore and retry until deadline
                }

                try {
                    Thread.sleep(pollIntervalMs);
                }
                catch (InterruptedException e) {
                    // ignore and retry until deadline
                }
            }

            assertTrue(sidecarProcessStarted, format("Sidecar did not start properly within %d ms", timeoutMs));
        }
        catch (Exception e) {
            log.error(e, "Failed while waiting for sidecar startup");
            throw new Exception(e);
        }
    }

    @AfterClass(alwaysRun = true)
    public void stopSidecar()
    {
        if (sidecar != null) {
            sidecar.destroyForcibly();
            sidecar = null;
        }
    }

    @Override
    public void assertFunction(@Language("SQL") String projection, Type expectedType, Object expected)
    {
        ConstantExpression result = evaluate(projection);
        assertTrue(typesEqualIgnoringVarcharParameters(result.getType(), expectedType), format("Expected type %s but got %s", expectedType, result.getType()));

        Object expectedValue = expected;
        if (expectedValue instanceof io.airlift.slice.Slice) {
            expectedValue = ((io.airlift.slice.Slice) expectedValue).toStringUtf8();
        }

        Object actual = toNativeValue(expectedType, result);
        assertEquals(actual, expectedValue);
    }

    private boolean typesEqualIgnoringVarcharParameters(Type actual, Type expected)
    {
        if (actual == expected) {
            return true;
        }
        if (actual instanceof com.facebook.presto.common.type.VarcharType && expected instanceof com.facebook.presto.common.type.VarcharType) {
            return true;
        }
        if (!actual.getTypeSignature().getBase().equals(expected.getTypeSignature().getBase())) {
            return false;
        }
        List<Type> actualParams = actual.getTypeParameters();
        List<Type> expectedParams = expected.getTypeParameters();
        if (actualParams.size() != expectedParams.size()) {
            return false;
        }
        for (int i = 0; i < actualParams.size(); i++) {
            if (!typesEqualIgnoringVarcharParameters(actualParams.get(i), expectedParams.get(i))) {
                return false;
            }
        }
        return true;
    }

    private Object toNativeValue(Type expectedType, ConstantExpression constant)
    {
        Object value = constant.getValue();
        if (value == null) {
            return null;
        }

        if (expectedType.getJavaType() == Block.class) {
            if (expectedType instanceof ArrayType) {
                ArrayType arrayType = (ArrayType) expectedType;
                Block elementsBlock = (Block) value;
                List<Object> values = new ArrayList<>(elementsBlock.getPositionCount());
                for (int i = 0; i < elementsBlock.getPositionCount(); i++) {
                    values.add(arrayType.getElementType().getObjectValue(TEST_SESSION.getSqlFunctionProperties(), elementsBlock, i));
                }
                return values;
            }
            return BlockAssertions.toValues(expectedType, (Block) value);
        }

        return value;
    }

    @Override
    public void assertNotSupported(String projection, @Language("RegExp") String message)
    {
        assertEvaluateFails(projection, message);
    }

    private ConstantExpression evaluate(@Language("SQL") String expression)
    {
        RowExpression parsedExpression = sqlToRowExpression(expression);
        return evaluateExpression(parsedExpression);
    }

    private RowExpression sqlToRowExpression(String expression)
    {
        Expression parsedExpression = FunctionAssertions.createExpression(expression, METADATA, SYMBOL_TYPES);
        return TRANSLATOR.translate(parsedExpression, SYMBOL_TYPES);
    }

    private ConstantExpression evaluateExpression(RowExpression expression)
    {
        JsonNode expressionOptimizationResult = fetchExpressionOptimizationResult(expression);
        assertTrue(expressionOptimizationResult.get("expressionFailureInfo").get("message").isEmpty());
        JsonNode optimizedExpression = expressionOptimizationResult.get("optimizedExpression");
        RowExpression result = codec.fromJson(optimizedExpression.toString());
        assertTrue(result instanceof ConstantExpression);
        return (ConstantExpression) result;
    }

    private void assertEvaluateFails(@Language("SQL") String expression, @Language("SQL") String errorMessage)
    {
        RowExpression rowExpression = sqlToRowExpression(expression);
        JsonNode expressionOptimizationResult = fetchExpressionOptimizationResult(rowExpression);
        assertNull(expressionOptimizationResult.get("optimizedExpression"));
        JsonNode failureInfo = expressionOptimizationResult.get("expressionFailureInfo");
        JsonCodec<NativeSidecarFailureInfo> errorCodec = jsonCodec(NativeSidecarFailureInfo.class);
        NativeSidecarFailureInfo result = errorCodec.fromJson(failureInfo.toString());

        assertNotNull(result.getMessage());
        assertTrue(result.getMessage().contains(errorMessage), format("Sidecar response: %s did not contain expected error message: %s.", failureInfo, errorMessage));
    }

    private JsonNode fetchExpressionOptimizationResult(RowExpression expression)
    {
        Response response;
        try {
            response = getSidecarResponse(expression);
        }
        catch (Exception e) {
            log.error(e, "Failed to get sidecar response: %s.", e.getMessage());
            throw new RuntimeException(e);
        }

        assertEquals(response.code(), 200, "Sidecar returned error.");
        String responseBody;
        try {
            responseBody = response.body().string();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            response.close();
        }

        try {
            JsonNode expressionOptimizationResultList = new ObjectMapper().readTree(responseBody);
            assertTrue(expressionOptimizationResultList.isArray());
            return expressionOptimizationResultList.get(0);
        }
        catch (JsonProcessingException e) {
            log.error(e, "Failed to decode RowExpression from sidecar response: %s.", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private Response getSidecarResponse(RowExpression expression)
            throws IOException
    {
        String json = String.format("[%s]", codec.toJson(expression));
        OkHttpClient client = new OkHttpClient();
        RequestBody body = RequestBody.create(MediaType.parse(JSON_UTF_8.toString()), json);
        Request request = new Request.Builder()
                .url(expressionUri.toString())
                .header(CONTENT_TYPE, JSON_UTF_8.toString())
                .header(ACCEPT, JSON_UTF_8.toString())
                .header("X-Presto-Time-Zone", TEST_SESSION.getSqlFunctionProperties().getTimeZoneKey().getId())
                .header("X-Presto-Expression-Optimizer-Level", ExpressionOptimizer.Level.EVALUATED.name())
                .post(body)
                .build();

        return client.newCall(request).execute();
    }

    private JsonCodec<RowExpression> getJsonCodec()
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            binder.bind(ConnectorManager.class).toProvider(() -> null).in(Scopes.SINGLETON);
            binder.install(new ThriftCodecModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);

            FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            jsonCodecBinder(binder).bindJsonCodec(RowExpression.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(new Key<JsonCodec<RowExpression>>() {});
    }

    private static Process getSidecarProcess(URI discoveryUri, int port)
            throws IOException
    {
        Path tempDirectoryPath = Files.createTempDirectory(PrestoNativeQueryRunnerUtils.class.getSimpleName());
        log.info("Temp directory for Sidecar: %s", tempDirectoryPath.toString());

        String configProperties = format("discovery.uri=%s%n" +
                "presto.version=testversion%n" +
                "system-memory-gb=4%n" +
                "native-sidecar=true%n" +
                "http-server.http.port=%d", discoveryUri, port);

        Files.write(tempDirectoryPath.resolve("config.properties"), configProperties.getBytes());
        Files.write(tempDirectoryPath.resolve("node.properties"),
                format("node.id=%s%n" +
                        "node.internal-address=127.0.0.1%n" +
                        "node.environment=testing%n" +
                        "node.location=test-location", UUID.randomUUID()).getBytes());

        Path catalogDirectoryPath = tempDirectoryPath.resolve("catalog");
        Files.createDirectory(catalogDirectoryPath);
        PrestoNativeQueryRunnerUtils.NativeQueryRunnerParameters nativeQueryRunnerParameters = getNativeQueryRunnerParameters();
        String prestoServerPath = nativeQueryRunnerParameters.serverBinary.toString();

        return new ProcessBuilder(prestoServerPath, "--logtostderr=1", "--v=1")
                .directory(tempDirectoryPath.toFile())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("sidecar.out").toFile()))
                .redirectError(ProcessBuilder.Redirect.to(tempDirectoryPath.resolve("sidecar.out").toFile()))
                .start();
    }

    public static int findRandomPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
