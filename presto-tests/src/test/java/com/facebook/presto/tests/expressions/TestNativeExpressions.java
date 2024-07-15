package com.facebook.presto.tests.expressions;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.jaxrs.JsonMapper;
import com.facebook.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncoding;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.nodeManager.PluginNodeManager;
import com.facebook.presto.session.sql.expressions.ForSidecarInfo;
import com.facebook.presto.session.sql.expressions.NativeBatchRowExpressionInterpreterProvider;
import com.facebook.presto.session.sql.expressions.NativeExpressionsModule;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.sql.planner.RowExpressionInterpreterService;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.expressions.JsonCodecRowExpressionSerde;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import org.testng.annotations.Test;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestNativeExpressions
{
    public static final URI SIDECAR_URI = URI.create("http://127.0.0.1:1122");
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final TestingRowExpressionTranslator TRANSLATOR = new TestingRowExpressionTranslator(METADATA);
    @Test
    public void testLoadPlugin()
    {
        RowExpressionInterpreterService interpreterService = getRowExpressionInterpreterService();

        // Test the native row expression interpreter service with some simple expressions
        RowExpression simpleAddition = compileExpression("1+1");
        RowExpression unnecessaryCoalesce = compileExpression("coalesce(1, 2)");
        Map<RowExpression, Object> optimized = interpreterService.optimizeBatch(
                TEST_SESSION.toConnectorSession(),
                ImmutableMap.of(
                        simpleAddition, simpleAddition,
                        unnecessaryCoalesce, unnecessaryCoalesce),
                ExpressionOptimizer.Level.OPTIMIZED);

        // Assert simple optimizations are performed
        assertEquals(optimized.size(), 2);
        assertEquals(optimized.get(simpleAddition), toRowExpression(2L, simpleAddition.getType()));
        assertEquals(optimized.get(unnecessaryCoalesce), toRowExpression(1L, unnecessaryCoalesce.getType()));
    }

    private static RowExpression compileExpression(String expression)
    {
        return TRANSLATOR.translate(expression, ImmutableMap.of());
    }

    private RowExpressionInterpreterService getRowExpressionInterpreterService()
    {
        // Set up dependencies in main for this module
        InMemoryNodeManager nodeManager = getNodeManagerWithSidecar(SIDECAR_URI);
        Injector prestoMainInjector = getPrestoMainInjector();
        JsonMapper jsonMapper = getJsonMapper(prestoMainInjector);
        RowExpressionSerde rowExpressionSerde = getRowExpressionSerde(prestoMainInjector);

        // Set up the mock HTTP endpoint that delegates to the Java based row expression interpreter
        TestingExpressionOptimizerResource resource = new TestingExpressionOptimizerResource(
                METADATA.getFunctionAndTypeManager(),
                testSessionBuilder().build().toConnectorSession(),
                ExpressionOptimizer.Level.OPTIMIZED);
        JaxrsTestingHttpProcessor jaxrsTestingHttpProcessor = new JaxrsTestingHttpProcessor(
                UriBuilder.fromUri(SIDECAR_URI).path("/").build(),
                resource,
                jsonMapper);
        TestingHttpClient testingHttpClient = new TestingHttpClient(jaxrsTestingHttpProcessor);

        // Create the native row expression interpreter service
        return createRowExpressionInterpreterService(nodeManager, rowExpressionSerde, testingHttpClient);
    }

    private static InMemoryNodeManager getNodeManagerWithSidecar(URI sidecarUri)
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(new ConnectorId("test"), new InternalNode("test", sidecarUri, NodeVersion.UNKNOWN, false, false, false, true));
        return nodeManager;
    }

    private RowExpressionInterpreterService createRowExpressionInterpreterService(InMemoryNodeManager nodeManager, RowExpressionSerde rowExpressionSerde, HttpClient httpClient)
    {
        requireNonNull(nodeManager, "nodeManager is null");

        Bootstrap app = new Bootstrap(
                binder -> {
                    // Preconditions from main
                    binder.bind(NodeManager.class).toInstance(new PluginNodeManager(nodeManager));
                    binder.bind(RowExpressionSerde.class).toInstance(rowExpressionSerde);
                },
                // Specially use a testing HTTP client instead of a real one
                binder -> binder.bind(HttpClient.class).annotatedWith(ForSidecarInfo.class).toInstance(httpClient),
                // Otherwise use the exact same module as the native row expression interpreter service
                new NativeExpressionsModule());

        Injector injector = app
                .noStrictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(ImmutableMap.of())
                .quiet()
                .initialize();
        return injector.getInstance(NativeBatchRowExpressionInterpreterProvider.class).createInterpreter();
    }

    private JsonMapper getJsonMapper(Injector injector)
    {
        return injector.getInstance(JsonMapper.class);
    }

    private RowExpressionSerde getRowExpressionSerde(Injector injector)
    {
        return injector.getInstance(RowExpressionSerde.class);
    }

    private static Injector getPrestoMainInjector()
    {
        Module module = binder -> {
            // Installs the JSON codec
            binder.install(new JsonModule());
            // Required to deserialize function handles
            binder.install(new HandleJsonModule());
            // Required for this test in the JaxrsTestingHttpProcessor because the underlying object mapper
            // must be the same as all other object mappers
            binder.bind(JsonMapper.class);

            // These dependencies are needed to serialize and deserialize types (found in expressions)
            FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
            binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            // These dependencies are needed to serialize and deserialize blocks (found in constant values of expressions)
            binder.bind(BlockEncodingSerde.class).to(BlockEncodingManager.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);

            // Create the serde which is used by the plugin to serialize and deserialize expressions
            jsonCodecBinder(binder).bindJsonCodec(RowExpression.class);
            binder.bind(RowExpressionSerde.class).to(JsonCodecRowExpressionSerde.class).in(Scopes.SINGLETON);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector;
    }

    @Path("/v1/expressions")
    public static class TestingExpressionOptimizerResource
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final ConnectorSession connectorSession;
        private final ExpressionOptimizer.Level level;

        public TestingExpressionOptimizerResource(FunctionAndTypeManager functionAndTypeManager, ConnectorSession connectorSession, ExpressionOptimizer.Level level)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");
            this.level = requireNonNull(level, "level is null");
        }

        @POST
        @Consumes(MediaType.APPLICATION_JSON)
        @Produces(MediaType.APPLICATION_JSON)
        public List<RowExpression> post(List<RowExpression> rowExpressions)
        {
            Map<RowExpression, RowExpression> input = rowExpressions.stream().collect(toImmutableMap(i -> i, i -> i));
            Map<RowExpression, Object> optimizedExpressions = new RowExpressionInterpreter(input, functionAndTypeManager, connectorSession, level).optimizeBatch();
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
            for (RowExpression inputExpression : rowExpressions) {
                builder.add(toRowExpression(optimizedExpressions.get(inputExpression), inputExpression.getType()));
            }
            return builder.build();
        }
    }
}
