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
package com.facebook.presto.nativeworker;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.expressions.AbstractTestExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.type.TypeDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.getNativeQueryRunnerParameters;
import static com.facebook.presto.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestNativeExpressionInterpreter
        extends AbstractTestExpressionInterpreter
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger log = Logger.get(TestNativeExpressionInterpreter.class);

    private JsonCodec<RowExpression> codec;
    private TestVisitor visitor;
    private Process sidecar;
    private URI expressionUri;

    public TestNativeExpressionInterpreter()
    {
        METADATA.getFunctionAndTypeManager().registerBuiltInFunctions(ImmutableList.of(APPLY_FUNCTION));
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        codec = getJsonCodec();
        visitor = new TestVisitor();
        int port = findRandomPort();
        HttpUriBuilder sidecarUri = HttpUriBuilder.uriBuilder()
                .scheme("http")
                .host("127.0.0.1")
                .port(port);
        expressionUri = sidecarUri.appendPath("/v1/expressions").build();
        sidecar = getSidecarProcess(sidecarUri.build(), port);

        try {
            HttpClient client = HttpClient.newHttpClient();
            URI infoUri = sidecarUri.appendPath("/v1/info").build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(infoUri)
                    .header(ACCEPT, JSON_UTF_8.toString())
                    .GET()
                    .build();

            long timeoutMs = 15000;
            long pollIntervalMs = 1000;
            long deadline = System.currentTimeMillis() + timeoutMs;
            boolean sidecarProcessStarted = false;

            while (System.currentTimeMillis() < deadline) {
                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                    if (response.statusCode() != 500) {
                        sidecarProcessStarted = true;
                        break;
                    }
                }
                catch (IOException e) {
                    // ignore and retry until deadline
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
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

    @AfterClass
    public void tearDown()
    {
        sidecar.destroyForcibly();
    }

    /// Velox permits Bigint to Varchar cast but Presto does not.
    @Override
    @Test
    public void testCastBigintToBoundedVarchar()
    {
        assertEvaluatedEquals("CAST(12300000000 AS varchar(11))", "'12300000000'");
        assertEvaluatedEquals("CAST(12300000000 AS varchar(50))", "'12300000000'");

        evaluate("CAST(12300000000 AS varchar(3))");
        evaluate("CAST(-12300000000 AS varchar(3))");
    }

    /// TODO: Optimization of `'a' LIKE unbound_string ESCAPE null` to `null` is not supported in Velox.
    @Override
    @Test
    public void testLikeNullOptimization()
    {
        assertOptimizedEquals("null LIKE '%'", "null");
        assertOptimizedEquals("'a' LIKE null", "null");
        assertOptimizedEquals("'a' LIKE '%' ESCAPE null", "null");
        // assertOptimizedEquals("'a' LIKE unbound_string ESCAPE null", "null");
    }

    /// TODO: Fix result mismatch between Presto and Velox for VARBINARY literals.
    /// java.lang.AssertionError:
    /// Expected :Slice{base=[B@771ede0d, address=16, length=2}
    /// Actual   :Slice{base=[B@1fd73dcb, address=47, length=2}
    @Test(enabled = false)
    public void testVarbinaryLiteral() {}

    /// Optimizer level SERIALIZABLE is not supported in Presto C++.
    @Test(enabled = false)
    public void testMassiveArray() {}

    // TODO: current timestamp returns the session timestamp, which is not supported by this test.
    @Override
    @Test(enabled = false)
    public void testCurrentTimestamp() {}

    /// TODO: current_user should be evaluated in the sidecar plugin and not in the sidecar.
    @Override
    @Test(enabled = false)
    public void testCurrentUser() {}

    /// This test is disabled because these optimizations for LIKE function are not yet supported in Velox.
    /// TODO: LIKE function with variables will not be constant folded in Velox.
    @Override
    @Test(enabled = false)
    public void testLikeOptimization() {}

    /// TODO: These tests are disabled as they throw a Velox exception when constant folded, protocol changes along
    /// with Velox to Presto exception conversion is needed to support this.
    /// Another limitation is that LIKE function with unbound_string will not be constant folded in Velox.
    @Override
    @Test(enabled = false)
    public void testInvalidLike() {}

    /// TODO: NULL_IF special form is unsupported in Presto native.
    @Override
    @Test(enabled = false)
    public void testNullIf() {}

    /// TODO: This test is disabled as the expressions throw a Velox exception when constant folded, protocol changes
    /// are needed to convert the error code and construct the FailFunction CallExpression.
    @Override
    @Test(enabled = false)
    public void testFailedExpressionOptimization() {}

    /// TODO: Json based UDFs are not supported by the native expression optimizer.
    @Override
    @Test(enabled = false)
    public void testCppFunctionCall() {}

    /// TODO: Json based UDFs are not supported by the native expression optimizer.
    @Override
    @Test(enabled = false)
    public void testCppAggregateFunctionCall() {}

    /// Non-deterministic functions are evaluated by default in sidecar.
    @Override
    @Test(enabled = false)
    public void testNonDeterministicFunctionCall() {}

    /// Velox only supports legacy map subscript and returns NULL for missing keys instead of an exception.
    @Override
    @Test(enabled = false)
    public void testMapSubscriptMissingKey() {}

    /// Failing expressions throw PrestoException when evaluated and not during optimization in Velox.
    /// This test does not support validation of expressions that throw a PrestoException.
    @Override
    @Test(enabled = false, expectedExceptions = PrestoException.class)
    public void testOptimizeDivideByZero()
    {
        evaluate("0 / 0");
    }

    /// Failing expressions throw PrestoException when evaluated and not during optimization in Velox.
    /// This test does not support validation of expressions that throw a PrestoException.
    @Test(enabled = false, expectedExceptions = PrestoException.class)
    public void testArraySubscriptConstantNegativeIndex()
    {
        evaluate("ARRAY [1, 2, 3][-1]");
    }

    /// Failing expressions throw PrestoException when evaluated and not during optimization in Velox.
    /// This test does not support validation of expressions that throw a PrestoException.
    @Test(enabled = false, expectedExceptions = PrestoException.class)
    public void testArraySubscriptConstantZeroIndex()
    {
        evaluate("ARRAY [1, 2, 3][0]");
    }

    /// Pending on special form expression rewrites in Velox.
    @Override
    @Test(enabled = false)
    public void testSpecialFormOptimizations() {}

    /// Pending on special form expression rewrites in Velox.
    @Override
    @Test(enabled = false)
    public void testCoalesce() {}

    /// TODO: Nested row subscript is not supported by Presto to Velox expression conversion.
    @Override
    @Test(enabled = false)
    public void testRowSubscript() {}

    @Override
    public Object evaluate(@Language("SQL") String expression, boolean deterministic)
    {
        return evaluate(expression);
    }

    private RowExpression evaluate(@Language("SQL") String expression)
    {
        return optimize(expression, ExpressionOptimizer.Level.EVALUATED);
    }

    @Override
    public RowExpression optimize(String expression) {
        return optimize(expression, ExpressionOptimizer.Level.OPTIMIZED);
    }

    private RowExpression optimize(@Language("SQL") String expression, ExpressionOptimizer.Level level)
    {
        assertRoundTrip(expression);
        RowExpression parsedExpression = sqlToRowExpression(expression);
        return optimizeRowExpression(parsedExpression, level);
    }

    @Override
    public void assertOptimizedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        RowExpression optimizedActual = optimize(actual, ExpressionOptimizer.Level.OPTIMIZED);
        RowExpression optimizedExpected = optimize(expected, ExpressionOptimizer.Level.OPTIMIZED);
        assertRowExpressionEvaluationEquals(optimizedActual, optimizedExpected);
    }

    @Override
    public void assertOptimizedMatches(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertOptimizedEquals(actual, expected);
    }

    @Override
    public void assertDoNotOptimize(@Language("SQL") String expression, ExpressionOptimizer.Level optimizationLevel)
    {
        assertRoundTrip(expression);
        RowExpression rowExpression = sqlToRowExpression(expression);
        RowExpression rowExpressionResult = optimizeRowExpression(rowExpression, ExpressionOptimizer.Level.OPTIMIZED);
        assertRowExpressionEvaluationEquals(rowExpressionResult, rowExpression);
    }

    private RowExpression sqlToRowExpression(String expression)
    {
        Expression parsedExpression = FunctionAssertions.createExpression(expression, METADATA, SYMBOL_TYPES);
        return TRANSLATOR.translate(parsedExpression, SYMBOL_TYPES);
    }

    @Override
    public void assertEvaluatedEquals(@Language("SQL") String actual, @Language("SQL") String expected)
    {
        assertRowExpressionEvaluationEquals(evaluate(actual), evaluate(expected));
    }

    private RowExpression optimizeRowExpression(RowExpression expression, ExpressionOptimizer.Level level)
    {
        try {
            expression = expression.accept(visitor, null);
            String json = String.format("[%s]", codec.toJson(expression));

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(expressionUri)
                    .header(CONTENT_TYPE, JSON_UTF_8.toString())
                    .header(ACCEPT, JSON_UTF_8.toString())
                    .header("X-Presto-Time-Zone", TEST_SESSION.getSqlFunctionProperties().getTimeZoneKey().getId())
                    .header("X-Presto-Expression-Optimizer-Level", level.name())
                    .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                String body = response.body();
                try {
                    // Response should be a JSON array consisting of a single RowExpression.
                    if (body != null) {
                        String trimmed = body.trim();
                        if (trimmed.startsWith("[")) {
                            String expressionString = getOnlyExpression(trimmed);
                            if (expressionString != null && !expressionString.isEmpty()) {
                                RowExpression result = codec.fromJson(expressionString);
                                if (result != null) {
                                    return result;
                                }
                            }
                        }
                    }
                }
                catch (Exception e) {
                    log.error(e, "Failed to decode RowExpression from sidecar response");
                    throw new RuntimeException(e);
                }
            }
            else {
                log.error("Sidecar returned an error with status code: %s", response.statusCode());
            }
        }
        catch (Exception e) {
            log.error(e, "Failed to serialize RowExpression from sidecar");
        }

        return expression;
    }

    public static String getOnlyExpression(String jsonArrayString)
    {
        try {
            JsonNode arrayNode = objectMapper.readTree(jsonArrayString);
            if (arrayNode.isArray() && arrayNode.size() > 0) {
                JsonNode firstElement = arrayNode.get(0);
                return objectMapper.writeValueAsString(firstElement);
            }
        }
        catch (Exception e) {
            log.error("Error parsing JSON array: " + e.getMessage());
        }
        return null;
    }

    private JsonCodec<RowExpression> getJsonCodec()
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
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

    private static class TestVisitor
            implements RowExpressionVisitor<RowExpression, Object>
    {
        @Override
        public RowExpression visitInputReference(InputReferenceExpression node, Object context)
        {
            return node;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression node, Object context)
        {
            return node;
        }

        /**
         * Convert a variable reference to a RowExpression.
         * If the symbol has a constant value, return a ConstantExpression of the appropriate type.
         * Otherwise return a VariableReferenceExpression as before.
         */
        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression node, Object context)
        {
            Symbol symbol = new Symbol(node.getName());
            Object value = symbolConstant(symbol);
            if (value == null) {
                return new VariableReferenceExpression(Optional.empty(), symbol.getName(), SYMBOL_TYPES.get(symbol.toSymbolReference()));
            }
            Type type = SYMBOL_TYPES.get(symbol.toSymbolReference());
            return new ConstantExpression(value, type);
        }

        @Override
        public RowExpression visitCall(CallExpression call, Object context)
        {
            CallExpression callExpression;
            List<RowExpression> newArguments = new ArrayList<>();
            for (RowExpression argument : call.getArguments()) {
                RowExpression newArgument = argument.accept(this, context);
                newArguments.add(newArgument);
            }
            callExpression = new CallExpression(
                    call.getSourceLocation(),
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    newArguments);
            return callExpression;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Object context)
        {
            return lambda;
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Object context)
        {
            SpecialFormExpression result;
            List<RowExpression> newArguments = new ArrayList<>();
            for (RowExpression argument : specialForm.getArguments()) {
                RowExpression newArgument = argument.accept(this, context);
                newArguments.add(newArgument);
            }
            result = new SpecialFormExpression(
                    specialForm.getSourceLocation(),
                    specialForm.getForm(),
                    specialForm.getType(),
                    newArguments);
            return result;
        }
    }
}
