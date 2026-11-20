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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.TestingBlockEncodingSerde;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.FunctionType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.TestingTypeDeserializer;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.server.SliceDeserializer;
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.facebook.presto.sql.planner.sanity.ValidateDependenciesChecker;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.type.TypeDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestRpcFunctionOptimizer
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestMetadataManager().getFunctionAndTypeManager();

    private static CallExpression createRpcCall(RowExpression optionsArg)
    {
        FunctionHandle handle = createFunctionHandle("test_rpc_function");
        return new CallExpression(
                "test_rpc_function",
                handle,
                VARCHAR,
                ImmutableList.of(
                        new ConstantExpression(Slices.utf8Slice("prompt"), VARCHAR),
                        new ConstantExpression(Slices.utf8Slice("model"), VARCHAR),
                        new ConstantExpression(Slices.utf8Slice("system"), VARCHAR),
                        optionsArg));
    }

    private static CallExpression createConcat(RowExpression... args)
    {
        FunctionHandle handle = createFunctionHandle("concat");
        return new CallExpression("concat", handle, VARCHAR, ImmutableList.copyOf(args));
    }

    private static ConstantExpression varchar(String value)
    {
        return new ConstantExpression(Slices.utf8Slice(value), VARCHAR);
    }

    @DataProvider
    public Object[][] legacyInferenceOptions()
    {
        List<Object[]> cases = new ArrayList<>();
        for (String name : new String[] {"test_inference", "test_inference_alias", "test_inference_alt", "test_inference_with_error"}) {
            for (String sessionMode : new String[] {"PER_ROW", "BATCH"}) {
                for (String mode : new String[] {"chat", "BATCH_DEFERRED"}) {
                    for (boolean enabled : new boolean[] {false, true}) {
                        cases.add(new Object[] {name, mode, enabled, sessionMode});
                    }
                }
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "legacyInferenceOptions")
    public void testLegacyInferenceOptionsRespectRequestedMode(String functionName, String optionsMode, boolean enabled, String sessionMode)
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", sessionMode)
                .setSystemProperty("rpc_function_optimizer_enabled", Boolean.toString(enabled))
                .build();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableAllocator variables = new VariableAllocator();
        VariableReferenceExpression input = variables.newVariable("input", VARCHAR);
        Type resultType = functionName.endsWith("_with_error")
                ? RowType.from(ImmutableList.of(new RowType.Field(Optional.of("result"), VARCHAR), new RowType.Field(Optional.of("error"), VARCHAR)))
                : VARCHAR;
        VariableReferenceExpression result = variables.newVariable("result", resultType);
        ConstantExpression options = varchar("{\"streaming_mode\":\"" + optionsMode + "\"}");
        CallExpression call = new CallExpression(functionName, createFunctionHandle(functionName), resultType,
                ImmutableList.of(input, varchar("model"), options));
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(input), ImmutableList.of(), Optional.empty());
        ProjectNode project = new ProjectNode(ids.getNextId(), source, Assignments.of(result, call));
        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(() -> ImmutableSet.of(functionName), null,
                new DefaultRpcExecutionPolicy(), true);
        if (optionsMode.equals("BATCH_DEFERRED")) {
            PrestoException error = expectThrows(PrestoException.class, () ->
                    optimizer.optimize(project, session, TypeProvider.viewOf(variables.getVariables()), variables, ids,
                            WarningCollector.NOOP, false));
            assertEquals(error.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertTrue(error.getMessage().contains(enabled ? "No BATCH_DEFERRED" : "rpc_function_optimizer_enabled"));
            return;
        }
        PlanNode planned = optimizer.optimize(project, session, TypeProvider.viewOf(variables.getVariables()), variables, ids,
                WarningCollector.NOOP, false).getPlanNode();
        if (enabled) {
            RPCNode rpc = (RPCNode) findPlanNode(planned, RPCNode.class);
            assertTrue(rpc != null);
            assertEquals(rpc.getExecutionPhase(), RPCNode.ExecutionPhase.INLINE);
            assertEquals(rpc.getStreamingMode(), RPCNode.StreamingMode.valueOf(sessionMode));
            assertEquals(rpc.getArguments().get(2), options);
        }
        else {
            assertEquals(planned, project);
        }
    }

    @DataProvider
    public Object[][] optimizerEnablement()
    {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "optimizerEnablement")
    public void testOptimizerDoesNotLoadRpcNamesForPlansWithoutCalls(boolean enabled)
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_function_optimizer_enabled", Boolean.toString(enabled))
                .setSystemProperty("rpc_streaming_mode", "BATCH_DEFERRED")
                .build();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableAllocator variables = new VariableAllocator();
        VariableReferenceExpression input = variables.newVariable("input", VARCHAR);
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(input), ImmutableList.of(), Optional.empty());
        ProjectNode project = new ProjectNode(ids.getNextId(), source,
                Assignments.of(variables.newVariable("result", VARCHAR), varchar("{\"streaming_mode\":\"BATCH_DEFERRED\"}")));
        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(() -> {
            throw new IllegalStateException("RPC registry must not be loaded for this query");
        });
        assertEquals(optimizer.optimize(project, session, TypeProvider.viewOf(variables.getVariables()), variables, ids,
                WarningCollector.NOOP, false).getPlanNode(), project);
    }

    @Test(dataProvider = "optimizerEnablement")
    public void testOrdinaryCallsDoNotResolveRpcOptionsOrEvaluateArguments(boolean enabled)
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_function_optimizer_enabled", Boolean.toString(enabled))
                .setSystemProperty("rpc_streaming_mode", "BATCH_DEFERRED")
                .build();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableAllocator variables = new VariableAllocator();
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        CallExpression nonLocalConstant = new CallExpression("not_a_local_function", createFunctionHandle("not_a_local_function"), VARCHAR, ImmutableList.of());
        RowExpression expression = new SpecialFormExpression(SpecialFormExpression.Form.IF, VARCHAR,
                ImmutableList.of(new ConstantExpression(false, BOOLEAN), createConcat(varchar("prefix"), varchar("middle"), nonLocalConstant), varchar("safe")));
        ProjectNode project = new ProjectNode(ids.getNextId(), source, Assignments.of(variables.newVariable("result", VARCHAR), expression));
        List<String> layoutCalls = new ArrayList<>();
        RpcDeferredExecutionRegistry registry = new DefaultRpcDeferredExecutionRegistry(call -> {
            layoutCalls.add(call.getDisplayName());
            return call.getArguments().size() >= 3 ? RpcArgumentLayout.optionsAt(call.getArguments().size() - 1) : RpcArgumentLayout.noOptions();
        }, ImmutableList.of());
        PlanNode planned = new RpcFunctionOptimizer(() -> ImmutableSet.of("test_rpc_function"), null, new DefaultRpcExecutionPolicy(), true,
                FUNCTION_AND_TYPE_MANAGER, registry).optimize(project, session, TypeProvider.viewOf(variables.getVariables()), variables, ids,
                        WarningCollector.NOOP, false).getPlanNode();
        assertEquals(planned, project);
        assertTrue(layoutCalls.isEmpty());
    }

    private static FunctionHandle createFunctionHandle(String name)
    {
        return new FunctionHandle()
        {
            @Override
            public CatalogSchemaName getCatalogSchemaName()
            {
                return new CatalogSchemaName("presto", "default");
            }

            @Override
            public String getName()
            {
                return name;
            }

            @Override
            public FunctionKind getKind()
            {
                return FunctionKind.SCALAR;
            }

            @Override
            public List<TypeSignature> getArgumentTypes()
            {
                return Collections.emptyList();
            }
        };
    }

    private RPCNode.StreamingMode invokeParseStreamingMode(CallExpression rpcCall) throws Exception
    {
        return invokeParseStreamingMode(rpcCall, TestingSession.testSessionBuilder().build());
    }

    private RPCNode.StreamingMode invokeParseStreamingMode(CallExpression rpcCall, Session session) throws Exception
    {
        Object rewriter = createRewriter(session);
        Method method = rewriter.getClass().getDeclaredMethod("parseStreamingMode", CallExpression.class);
        method.setAccessible(true);
        return (RPCNode.StreamingMode) method.invoke(rewriter, rpcCall);
    }

    private int invokeParseDispatchBatchSize(CallExpression rpcCall) throws Exception
    {
        return invokeParseDispatchBatchSize(rpcCall, TestingSession.testSessionBuilder().build());
    }

    private int invokeParseDispatchBatchSize(CallExpression rpcCall, Session session) throws Exception
    {
        Object rewriter = createRewriter(session);
        Method method = rewriter.getClass().getDeclaredMethod("parseDispatchBatchSize", CallExpression.class);
        method.setAccessible(true);
        return (int) method.invoke(rewriter, rpcCall);
    }

    private Object createRewriter(Session session) throws Exception
    {
        Class<?>[] innerClasses = RpcFunctionOptimizer.class.getDeclaredClasses();
        for (Class<?> inner : innerClasses) {
            if (inner.getSimpleName().equals("Rewriter")) {
                for (Constructor<?> ctor : inner.getDeclaredConstructors()) {
                    if (ctor.getParameterCount() == 6) {
                        ctor.setAccessible(true);
                        return ctor.newInstance(
                                session,
                                new PlanNodeIdAllocator(),
                                new VariableAllocator(),
                                ImmutableSet.of(),
                                null,
                                new DefaultRpcExecutionPolicy());
                    }
                }
            }
        }
        throw new RuntimeException("Rewriter class not found");
    }

    @Test
    public void testParseStreamingModeConstantBatch() throws Exception
    {
        CallExpression rpcCall = createRpcCall(
                varchar("{\"api_key\":\"test-key\",\"streaming_mode\":\"batch\"}"));
        assertEquals(invokeParseStreamingMode(rpcCall), RPCNode.StreamingMode.BATCH);
    }

    @Test
    public void testParseStreamingModeConstantPerRow() throws Exception
    {
        CallExpression rpcCall = createRpcCall(
                varchar("{\"api_key\":\"test-key\"}"));
        assertEquals(invokeParseStreamingMode(rpcCall), RPCNode.StreamingMode.PER_ROW);
    }

    @Test
    public void testParseStreamingModeConstantExplicitPerRow() throws Exception
    {
        CallExpression rpcCall = createRpcCall(
                varchar("{\"api_key\":\"test-key\",\"streaming_mode\":\"per_row\"}"));
        assertEquals(invokeParseStreamingMode(rpcCall), RPCNode.StreamingMode.PER_ROW);
    }

    @Test
    public void testParseStreamingModeNonConstantOptionsFallsBackToSession() throws Exception
    {
        // Non-constant options (concat with variable) cannot be parsed as JSON.
        // Should fall back to session property default (PER_ROW).
        CallExpression concatOptions = createConcat(
                varchar("{\"api_key\":\""),
                new VariableReferenceExpression(Optional.empty(), "key_col", VARCHAR),
                varchar("\"}"));
        CallExpression rpcCall = createRpcCall(concatOptions);
        assertEquals(invokeParseStreamingMode(rpcCall), RPCNode.StreamingMode.PER_ROW);
    }

    @Test
    public void testParseStreamingModeNonConstantOptionsWithBatchSession() throws Exception
    {
        // Non-constant options with session property set to BATCH.
        CallExpression concatOptions = createConcat(
                varchar("{\"api_key\":\""),
                new VariableReferenceExpression(Optional.empty(), "key_col", VARCHAR),
                varchar("\"}"));
        CallExpression rpcCall = createRpcCall(concatOptions);
        Session batchSession = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", "BATCH")
                .build();
        assertEquals(invokeParseStreamingMode(rpcCall, batchSession), RPCNode.StreamingMode.BATCH);
    }

    @Test
    public void testParseStreamingModeVariableOptionsDefaultsPerRow() throws Exception
    {
        CallExpression rpcCall = createRpcCall(
                new VariableReferenceExpression(Optional.empty(), "options_col", VARCHAR));
        assertEquals(invokeParseStreamingMode(rpcCall), RPCNode.StreamingMode.PER_ROW);
    }

    @Test
    public void testParseStreamingModeTooFewArgs() throws Exception
    {
        FunctionHandle handle = createFunctionHandle("test_rpc_function");
        CallExpression rpcCall = new CallExpression(
                "test_rpc_function",
                handle,
                VARCHAR,
                ImmutableList.of(varchar("prompt"), varchar("model")));
        assertEquals(invokeParseStreamingMode(rpcCall), RPCNode.StreamingMode.PER_ROW);
    }

    @Test
    public void testParseStreamingModeSessionPropertyBatch() throws Exception
    {
        // No streaming_mode in JSON, session property set to BATCH.
        CallExpression rpcCall = createRpcCall(
                varchar("{\"api_key\":\"test-key\"}"));
        Session batchSession = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", "BATCH")
                .build();
        assertEquals(invokeParseStreamingMode(rpcCall, batchSession), RPCNode.StreamingMode.BATCH);
    }

    @DataProvider
    public Object[][] streamingModeOverrides()
    {
        List<Object[]> cases = new ArrayList<>();
        for (RPCNode.StreamingMode sessionMode : RPCNode.StreamingMode.values()) {
            for (RPCNode.StreamingMode functionMode : RPCNode.StreamingMode.values()) {
                for (boolean projected : new boolean[] {false, true}) {
                    cases.add(new Object[] {sessionMode, functionMode.name().toLowerCase(Locale.ENGLISH), projected});
                }
            }
            cases.add(new Object[] {sessionMode, null, false});
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "streamingModeOverrides")
    public void testFunctionModeOverridesSessionIncludingProjectedConstants(RPCNode.StreamingMode sessionMode, String functionMode, boolean projected)
    {
        PlanNode plan = optimizeModeCall(streamingModeSession(sessionMode.name()), "fake_completion", functionMode,
                projected, 0, true, new DefaultRpcExecutionPolicy());
        RPCNode.StreamingMode requested = functionMode == null ? sessionMode : RPCNode.StreamingMode.valueOf(functionMode.toUpperCase(Locale.ENGLISH));
        List<RPCNode> nodes = collectRpcNodes(plan);
        assertEquals(nodes.size(), requested == RPCNode.StreamingMode.BATCH_DEFERRED ? 3 : 1);
        RPCNode.StreamingMode physical = requested == RPCNode.StreamingMode.AUTOMATIC ? RPCNode.StreamingMode.PER_ROW :
                requested == RPCNode.StreamingMode.BATCH_DEFERRED ? RPCNode.StreamingMode.BATCH : requested;
        for (RPCNode node : nodes) {
            assertEquals(node.getStreamingMode(), physical);
        }
        if (requested == RPCNode.StreamingMode.BATCH_DEFERRED) {
            assertEquals(nodes.get(0).getExecutionPhase(), RPCNode.ExecutionPhase.COLLECT);
            assertEquals(nodes.get(1).getExecutionPhase(), RPCNode.ExecutionPhase.POLL);
            assertEquals(nodes.get(2).getExecutionPhase(), RPCNode.ExecutionPhase.SUBMIT);
        }
        else {
            assertEquals(nodes.get(0).getExecutionPhase(), RPCNode.ExecutionPhase.INLINE);
        }
        new ValidateDependenciesChecker().validate(plan);
    }

    @Test
    public void testDeferredIntentUsesExistingBatchPolicyContract()
    {
        List<RPCNode.StreamingMode> requested = new ArrayList<>();
        RpcExecutionPolicy policy = intent -> {
            requested.add(intent.getRequestedMode());
            return new DefaultRpcExecutionPolicy().translateIntent(intent);
        };
        PlanNode plan = optimizeModeCall(streamingModeSession("BATCH_DEFERRED"), "fake_completion", null,
                false, 0, true, policy);
        assertEquals(requested, ImmutableList.of(RPCNode.StreamingMode.BATCH));
        assertEquals(collectRpcNodes(plan).size(), 3);
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () ->
                optimizeModeCall(streamingModeSession("BATCH_DEFERRED"), "fake_completion", null,
                        false, 0, true, intent -> RpcExecutionProperties.of(RPCNode.StreamingMode.PER_ROW)));
        assertTrue(error.getMessage().contains("preserve BATCH"));
    }

    @Test
    public void testPolicyCannotLeakDeferredModeIntoPhysicalPlan()
    {
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () ->
                optimizeWithPolicy(streamingModeSession("PER_ROW"), intent -> RpcExecutionProperties.of(RPCNode.StreamingMode.BATCH_DEFERRED)));
        assertTrue(error.getMessage().contains("BATCH_DEFERRED"));
    }

    @Test
    public void testInlineFunctionOverrideDoesNotRequireNativeSpark()
    {
        for (String mode : new String[] {"PER_ROW", "BATCH", "AUTOMATIC"}) {
            PlanNode plan = optimizeModeCall(streamingModeSession("BATCH_DEFERRED"), "fake_completion", mode,
                    true, 0, false, new DefaultRpcExecutionPolicy());
            assertEquals(collectRpcNodes(plan).size(), 1);
            assertEquals(collectRpcNodes(plan).get(0).getExecutionPhase(), RPCNode.ExecutionPhase.INLINE);
        }
    }

    @DataProvider
    public Object[][] embeddingModes()
    {
        List<Object[]> cases = new ArrayList<>();
        for (String function : new String[] {"fake_embedding", "fake_embedding_alias", "fake_embedding_double",
                "fake_embedding_with_error", "fake_embedding_double_with_error"}) {
            for (int mediaCount : new int[] {1, 2}) {
                for (RPCNode.StreamingMode mode : RPCNode.StreamingMode.values()) {
                    for (boolean projected : new boolean[] {false, true}) {
                        cases.add(new Object[] {function, mediaCount, mode, projected});
                    }
                }
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "embeddingModes")
    public void testEmbeddingOptionsPrecedeMediaArguments(String function, int mediaCount, RPCNode.StreamingMode functionMode, boolean projected)
    {
        if (functionMode == RPCNode.StreamingMode.BATCH_DEFERRED) {
            PrestoException error = expectThrows(PrestoException.class, () ->
                    optimizeModeCall(streamingModeSession("PER_ROW"), function, functionMode.name(),
                            projected, mediaCount, true, new DefaultRpcExecutionPolicy()));
            assertEquals(error.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertTrue(error.getMessage().contains("BATCH_DEFERRED"));
            return;
        }
        PlanNode plan = optimizeModeCall(streamingModeSession("BATCH_DEFERRED"), function, functionMode.name(),
                projected, mediaCount, true, new DefaultRpcExecutionPolicy());
        List<RPCNode> nodes = collectRpcNodes(plan);
        assertEquals(nodes.size(), 1);
        assertEquals(nodes.get(0).getExecutionPhase(), RPCNode.ExecutionPhase.INLINE);
        assertEquals(nodes.get(0).getStreamingMode(), functionMode == RPCNode.StreamingMode.AUTOMATIC ? RPCNode.StreamingMode.PER_ROW : functionMode);
    }

    @Test
    public void testDeferredModeWithOptimizerDisabledChecksEffectiveFunctionMode()
    {
        for (RPCNode.StreamingMode mode : new RPCNode.StreamingMode[] {RPCNode.StreamingMode.PER_ROW, RPCNode.StreamingMode.BATCH, RPCNode.StreamingMode.AUTOMATIC}) {
            for (boolean projected : new boolean[] {false, true}) {
                Session session = Session.builder(streamingModeSession("BATCH_DEFERRED"))
                        .setSystemProperty("rpc_function_optimizer_enabled", "false").build();
                assertEquals(collectRpcNodes(optimizeModeCall(session, "fake_completion", mode.name(),
                        projected, 0, true, new DefaultRpcExecutionPolicy())).size(), 0);
            }
        }
        Session session = Session.builder(streamingModeSession("PER_ROW"))
                .setSystemProperty("rpc_function_optimizer_enabled", "false").build();
        PrestoException error = expectThrows(PrestoException.class, () ->
                optimizeModeCall(session, "fake_completion", "BATCH_DEFERRED", true, 0, true, new DefaultRpcExecutionPolicy()));
        assertEquals(error.getErrorCode(), NOT_SUPPORTED.toErrorCode());
        assertTrue(error.getMessage().contains("rpc_function_optimizer_enabled"));
        VariableAllocator variables = new VariableAllocator();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        PlanNode values = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        PlanNode unchanged = new RpcFunctionOptimizer(() -> ImmutableSet.of("fake_completion"))
                .optimize(values, Session.builder(session).setSystemProperty("rpc_streaming_mode", "BATCH_DEFERRED").build(),
                        TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        assertEquals(unchanged, values);
    }

    @DataProvider
    public Object[][] emptyOptionsModes()
    {
        return new Object[][] {
                {"PER_ROW"},
                {"BATCH"},
                {"AUTOMATIC"}};
    }

    @Test(dataProvider = "emptyOptionsModes")
    public void testEmptyOptionsRespectSessionWhenOptimizerDisabled(String mode)
    {
        Session session = Session.builder(streamingModeSession(mode))
                .setSystemProperty("rpc_function_optimizer_enabled", "false")
                .build();
        VariableAllocator variables = new VariableAllocator();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        ProjectNode project = new ProjectNode(ids.getNextId(), source,
                Assignments.builder().put(variables.newVariable("result", VARCHAR), createRpcCall(varchar(""))).build());
        PlanNode result = new RpcFunctionOptimizer(() -> ImmutableSet.of("test_rpc_function"))
                .optimize(project, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        assertEquals(result, project);
        assertTrue(collectRpcNodes(result).isEmpty());
        PrestoException error = expectThrows(PrestoException.class, () ->
                new RpcFunctionOptimizer(() -> ImmutableSet.of("test_rpc_function"))
                        .optimize(project, Session.builder(session).setSystemProperty("rpc_streaming_mode", "BATCH_DEFERRED").build(),
                                TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false));
        assertEquals(error.getErrorCode(), NOT_SUPPORTED.toErrorCode());
        assertTrue(error.getMessage().contains("rpc_function_optimizer_enabled"));
    }

    @Test(dataProvider = "emptyOptionsModes")
    public void testEmptyOptionsRespectSessionForInlineExecution(String mode)
            throws Exception
    {
        assertEquals(invokeParseStreamingMode(createRpcCall(varchar("")), streamingModeSession(mode)),
                RPCNode.StreamingMode.valueOf(mode));
    }

    @Test(dataProvider = "optimizerEnablement")
    public void testDeferredCallsInsideOrdinaryLambdasAreRejected(boolean enabled)
    {
        VariableAllocator variables = new VariableAllocator();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        CallExpression call = new CallExpression("fake_completion", createFunctionHandle("fake_completion"), VARCHAR,
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "p", VARCHAR), varchar("model"), varchar(""),
                        varchar("{\"streaming_mode\":\"BATCH_DEFERRED\"}")));
        LambdaDefinitionExpression lambda = new LambdaDefinitionExpression(Optional.empty(), ImmutableList.of(VARCHAR), ImmutableList.of("p"),
                createConcat(varchar("prefix"), call));
        ProjectNode project = new ProjectNode(ids.getNextId(), source,
                Assignments.builder().put(variables.newVariable("function", lambda.getType()), lambda).build());
        PrestoException error = expectThrows(PrestoException.class, () ->
                new RpcFunctionOptimizer(() -> ImmutableSet.of("fake_completion"), null, new DefaultRpcExecutionPolicy(), true, FUNCTION_AND_TYPE_MANAGER, fakeRegistry())
                        .optimize(project, Session.builder(streamingModeSession("PER_ROW"))
                                        .setSystemProperty("rpc_function_optimizer_enabled", Boolean.toString(enabled)).build(),
                                TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false));
        assertEquals(error.getErrorCode(), NOT_SUPPORTED.toErrorCode());
        assertTrue(error.getMessage().contains("BATCH_DEFERRED"));
        assertTrue(error.getMessage().contains(enabled ? "lambdas" : "rpc_function_optimizer_enabled"));
    }

    @Test
    public void testGenericRpcUsesLastOptionsArgument()
    {
        PlanNode plan = optimizeModeCall(streamingModeSession("BATCH_DEFERRED"), "custom_rpc", "PER_ROW",
                false, 1, true, new DefaultRpcExecutionPolicy());
        assertEquals(collectRpcNodes(plan).get(0).getStreamingMode(), RPCNode.StreamingMode.PER_ROW);
        PlanNode batch = optimizeModeCall(streamingModeSession("BATCH_DEFERRED"), "custom_rpc", "BATCH",
                false, 1, true, new DefaultRpcExecutionPolicy());
        assertEquals(collectRpcNodes(batch).get(0).getStreamingMode(), RPCNode.StreamingMode.BATCH);
        assertEquals(collectRpcNodes(batch).get(0).getExecutionPhase(), RPCNode.ExecutionPhase.INLINE);
        PrestoException error = expectThrows(PrestoException.class, () ->
                optimizeModeCall(streamingModeSession("PER_ROW"), "custom_rpc", "BATCH_DEFERRED",
                        false, 0, true, new DefaultRpcExecutionPolicy()));
        assertEquals(error.getErrorCode(), NOT_SUPPORTED.toErrorCode());
        assertTrue(error.getMessage().contains("No BATCH_DEFERRED"));
    }

    @Test
    public void testUnknownFunctionModeFallsBackToSession()
    {
        for (RPCNode.StreamingMode mode : new RPCNode.StreamingMode[] {RPCNode.StreamingMode.PER_ROW, RPCNode.StreamingMode.BATCH, RPCNode.StreamingMode.AUTOMATIC}) {
            PlanNode plan = optimizeModeCall(streamingModeSession(mode.name()), "fake_completion", "unknown",
                    false, 0, true, new DefaultRpcExecutionPolicy());
            List<RPCNode> nodes = collectRpcNodes(plan);
            assertEquals(nodes.size(), 1);
            assertEquals(nodes.get(0).getExecutionPhase(), RPCNode.ExecutionPhase.INLINE);
            assertEquals(nodes.get(0).getStreamingMode(), mode == RPCNode.StreamingMode.AUTOMATIC ? RPCNode.StreamingMode.PER_ROW : mode);
        }
    }

    @DataProvider
    public Object[][] collectOnlyDependencies()
    {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "collectOnlyDependencies")
    public void testDeclaredCollectDependenciesSurvivePolling(boolean groupingOnly)
    {
        VariableAllocator variables = new VariableAllocator();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableReferenceExpression input = variables.newVariable("input", VARCHAR);
        VariableReferenceExpression collectOnly = variables.newVariable("collect_only", VARCHAR);
        VariableReferenceExpression output = variables.newVariable("output", VARCHAR);
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(input, collectOnly), ImmutableList.of(), Optional.empty());
        CallExpression call = new CallExpression("collect_dependency", createFunctionHandle("collect_dependency"), VARCHAR,
                ImmutableList.of(input, varchar("model"), collectOnly, varchar("{\"streaming_mode\":\"batch_deferred\"}")));
        ProjectNode project = new ProjectNode(ids.getNextId(), source, Assignments.builder().put(output, call).build());
        DeferredRpcExecution execution = new DeferredRpcExecution()
        {
            @Override
            public DeferredRpcCall submit()
            {
                return DeferredRpcCall.builder("test_submit", fakeReceiptType()).setArguments(ImmutableList.of(input)).setDispatchBatchSize(2).build();
            }

            @Override
            public DeferredRpcCall poll(VariableReferenceExpression receipt)
            {
                return DeferredRpcCall.builder("test_poll", fakeReceiptType()).setArguments(ImmutableList.of(receipt)).setDispatchBatchSize(2)
                        .setInputGroupingExpressions(ImmutableList.of(fakeGrouping(receipt))).build();
            }

            @Override
            public DeferredRpcCall collect(VariableReferenceExpression ready)
            {
                return DeferredRpcCall.builder("test_collect", VARCHAR)
                        .setArguments(groupingOnly ? ImmutableList.of(ready) : ImmutableList.of(ready, collectOnly)).setDispatchBatchSize(2)
                        .setInputGroupingExpressions(ImmutableList.of(groupingOnly ? collectOnly : fakeGrouping(ready))).build();
            }
        };
        RpcDeferredExecutionRegistry registry = new DefaultRpcDeferredExecutionRegistry(ignored -> RpcArgumentLayout.optionsAt(3),
                ImmutableList.of(ignored -> Optional.of(execution)));
        Session session = streamingModeSession("PER_ROW");
        PlanNode plan = new RpcFunctionOptimizer(() -> ImmutableSet.of("collect_dependency"), null, new DefaultRpcExecutionPolicy(),
                true, FUNCTION_AND_TYPE_MANAGER, registry).optimize(project, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        PlanNode pruned = new PruneUnreferencedOutputs().optimize(plan, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        List<RPCNode> phases = collectRpcNodes(pruned);
        assertEquals(phases.size(), 3);
        assertTrue(phases.get(1).getSource().getOutputVariables().contains(collectOnly));
        assertFalse(phases.get(1).getSource().getOutputVariables().contains(input));
        assertTrue(phases.get(0).getSource().getOutputVariables().contains(collectOnly));
        assertFalse(pruned.getOutputVariables().contains(collectOnly));
        new ValidateDependenciesChecker().validate(pruned);
    }

    @Test
    public void testMissingAndAmbiguousDeferredProvidersFailClearly()
    {
        VariableAllocator variables = new VariableAllocator();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableReferenceExpression input = variables.newVariable("input", VARCHAR);
        VariableReferenceExpression output = variables.newVariable("output", VARCHAR);
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(input), ImmutableList.of(), Optional.empty());
        CallExpression call = new CallExpression("fake_completion", createFunctionHandle("fake_completion"), VARCHAR,
                ImmutableList.of(input, varchar("model"), varchar(""), varchar("{}")));
        ProjectNode project = new ProjectNode(ids.getNextId(), source, Assignments.builder().put(output, call).build());
        PrestoException missing = expectThrows(PrestoException.class, () ->
                new RpcFunctionOptimizer(() -> ImmutableSet.of("fake_completion"), null, new DefaultRpcExecutionPolicy(), true,
                        FUNCTION_AND_TYPE_MANAGER, new DefaultRpcDeferredExecutionRegistry())
                        .optimize(project, streamingModeSession("BATCH_DEFERRED"), TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false));
        assertEquals(missing.getErrorCode(), NOT_SUPPORTED.toErrorCode());
        assertTrue(missing.getMessage().contains("No BATCH_DEFERRED"));
        RpcDeferredExecutionRegistry ambiguous = new DefaultRpcDeferredExecutionRegistry(ignored -> RpcArgumentLayout.optionsAt(3),
                ImmutableList.of(context -> fakeRegistry().resolve(context), context -> fakeRegistry().resolve(context)));
        IllegalArgumentException multiple = expectThrows(IllegalArgumentException.class, () ->
                new RpcFunctionOptimizer(() -> ImmutableSet.of("fake_completion"), null, new DefaultRpcExecutionPolicy(), true,
                        FUNCTION_AND_TYPE_MANAGER, ambiguous)
                        .optimize(project, streamingModeSession("BATCH_DEFERRED"), TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false));
        assertTrue(multiple.getMessage().contains("Multiple deferred RPC providers"));
    }

    @Test
    public void testProviderControlsDispatchLimit()
    {
        Session session = Session.builder(streamingModeSession("BATCH_DEFERRED")).setSystemProperty("rpc_dispatch_batch_size", "8192").build();
        PlanNode plan = optimizeModeCall(session, "fake_completion", null, false, 0, true, new DefaultRpcExecutionPolicy());
        for (RPCNode phase : collectRpcNodes(plan)) {
            assertEquals(phase.getDispatchBatchSize(), 1024);
            assertEquals(phase.getInputGroupingExpressions().size(), phase.getExecutionPhase() == RPCNode.ExecutionPhase.SUBMIT ? 0 : 1);
        }
    }

    @DataProvider
    public Object[][] deferredCompletionShapes()
    {
        return new Object[][] {{2, false, false}, {3, false, false}, {4, false, false},
                {5, false, false}, {6, false, false}, {4, true, true}};
    }

    @Test(dataProvider = "deferredCompletionShapes")
    public void testDeferredCompletionPreservesProviderInputShapes(int argumentCount, boolean withError, boolean typedContent)
    {
        VariableAllocator variables = new VariableAllocator();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        Type inputType = typedContent ? new ArrayType(RowType.from(ImmutableList.of(RowType.field("payload", VARCHAR),
                RowType.field("category", VARCHAR)))) : VARCHAR;
        VariableReferenceExpression input = variables.newVariable("input", inputType);
        List<VariableReferenceExpression> sourceVariables = new ArrayList<>(ImmutableList.of(input));
        List<RowExpression> arguments = new ArrayList<>(ImmutableList.of(input, varchar("model")));
        if (argumentCount >= 3) {
            arguments.add(varchar(""));
        }
        if (argumentCount >= 4) {
            arguments.add(varchar("{\"streaming_mode\":\"batch_deferred\"}"));
        }
        for (int index = 4; index < argumentCount; index++) {
            Type mediaType = argumentCount == 6 ? new ArrayType(new ArrayType(VARCHAR)) : VARCHAR;
            VariableReferenceExpression media = variables.newVariable("media", mediaType);
            sourceVariables.add(media);
            arguments.add(media);
        }
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(), sourceVariables, ImmutableList.of(), Optional.empty());
        Type resultType = withError ? RowType.from(ImmutableList.of(RowType.field("result", VARCHAR), RowType.field("error", VARCHAR))) : VARCHAR;
        String function = withError ? "fake_completion_with_error" : "fake_completion";
        CallExpression call = new CallExpression(function, createFunctionHandle(function), resultType, arguments);
        ProjectNode project = new ProjectNode(ids.getNextId(), source,
                Assignments.builder().put(variables.newVariable("output", resultType), call).build());
        PlanNode plan = new RpcFunctionOptimizer(() -> ImmutableSet.of(function), null, new DefaultRpcExecutionPolicy(), true, FUNCTION_AND_TYPE_MANAGER, fakeRegistry())
                .optimize(project, streamingModeSession("BATCH_DEFERRED"), TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        List<RPCNode> nodes = collectRpcNodes(plan);
        assertEquals(nodes.size(), 3);
        assertEquals(nodes.get(2).getArguments().size(), argumentCount);
        assertEquals(nodes.get(0).getOutputVariable().getType(), resultType);
        for (RPCNode node : nodes) {
            assertEquals(node.getStreamingMode(), RPCNode.StreamingMode.BATCH);
        }
        new ValidateDependenciesChecker().validate(plan);
    }

    @Test
    public void testParseDispatchBatchSizeFromConstant() throws Exception
    {
        // JSON override takes precedence over session default (128).
        CallExpression rpcCall = createRpcCall(
                varchar("{\"api_key\":\"test-key\",\"dispatch_batch_size\":64}"));
        assertEquals(invokeParseDispatchBatchSize(rpcCall), 64);
    }

    @Test
    public void testParseDispatchBatchSizeSessionProperty() throws Exception
    {
        // No dispatch_batch_size in JSON, session property set to 25.
        CallExpression rpcCall = createRpcCall(
                varchar("{\"api_key\":\"test-key\"}"));
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_dispatch_batch_size", "25")
                .build();
        assertEquals(invokeParseDispatchBatchSize(rpcCall, session), 25);
    }

    @Test
    public void testParseDispatchBatchSizeJsonOverridesSession() throws Exception
    {
        // JSON has dispatch_batch_size=64, session has 25. JSON wins.
        CallExpression rpcCall = createRpcCall(
                varchar("{\"api_key\":\"test-key\",\"dispatch_batch_size\":64}"));
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_dispatch_batch_size", "25")
                .build();
        assertEquals(invokeParseDispatchBatchSize(rpcCall, session), 64);
    }

    @Test
    public void testParseDispatchBatchSizeDefault128() throws Exception
    {
        // No JSON override, default session value is 128.
        CallExpression rpcCall = createRpcCall(
                varchar("{\"api_key\":\"test-key\"}"));
        assertEquals(invokeParseDispatchBatchSize(rpcCall), 128);
    }

    @Test
    public void testTryWithRpcFunctionProducesRpcNode()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();

        VariableReferenceExpression inputVar = variableAllocator.newVariable("input_col", VARCHAR);
        VariableReferenceExpression outputVar = variableAllocator.newVariable("output_col", VARCHAR);

        // Build: $internal$try(BIND(input_col, (p) -> test_rpc_function(p, "model", "system", "{}")))
        CallExpression rpcInsideLambda = new CallExpression(
                "test_rpc_function",
                createFunctionHandle("test_rpc_function"),
                VARCHAR,
                ImmutableList.of(
                        new VariableReferenceExpression(Optional.empty(), "p", VARCHAR),
                        varchar("model"),
                        varchar("system"),
                        varchar("{}")));

        LambdaDefinitionExpression lambda = new LambdaDefinitionExpression(
                Optional.empty(),
                ImmutableList.of(VARCHAR),
                ImmutableList.of("p"),
                rpcInsideLambda);

        SpecialFormExpression bind = new SpecialFormExpression(
                SpecialFormExpression.Form.BIND,
                VARCHAR,
                ImmutableList.of(inputVar, lambda));

        CallExpression tryCall = new CallExpression(
                "$internal$try",
                createFunctionHandle("$internal$try"),
                VARCHAR,
                ImmutableList.of(bind));

        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(inputVar),
                ImmutableList.of(),
                Optional.empty());

        ProjectNode projectNode = new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.builder()
                        .put(outputVar, tryCall)
                        .build());

        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(
                () -> ImmutableSet.of("test_rpc_function"));
        Session session = TestingSession.testSessionBuilder().build();

        PlanOptimizerResult result = optimizer.optimize(
                projectNode,
                session,
                TypeProvider.empty(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP,
                false);

        assertTrue(result.isOptimizerTriggered(), "Plan should be optimized when TRY wraps an RPC function");
        assertTrue(containsPlanNode(result.getPlanNode(), RPCNode.class),
                "Optimized plan should contain an RPCNode");
    }

    @Test
    public void testTryWithoutRpcFunctionIsUnchanged()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();

        VariableReferenceExpression inputVar = variableAllocator.newVariable("input_col", VARCHAR);
        VariableReferenceExpression outputVar = variableAllocator.newVariable("output_col", VARCHAR);

        // Build: $internal$try(BIND(input_col, (p) -> concat(p, "suffix")))
        CallExpression concatInsideLambda = createConcat(
                new VariableReferenceExpression(Optional.empty(), "p", VARCHAR),
                varchar("suffix"));

        LambdaDefinitionExpression lambda = new LambdaDefinitionExpression(
                Optional.empty(),
                ImmutableList.of(VARCHAR),
                ImmutableList.of("p"),
                concatInsideLambda);

        SpecialFormExpression bind = new SpecialFormExpression(
                SpecialFormExpression.Form.BIND,
                VARCHAR,
                ImmutableList.of(inputVar, lambda));

        CallExpression tryCall = new CallExpression(
                "$internal$try",
                createFunctionHandle("$internal$try"),
                VARCHAR,
                ImmutableList.of(bind));

        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(inputVar),
                ImmutableList.of(),
                Optional.empty());

        ProjectNode projectNode = new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.builder()
                        .put(outputVar, tryCall)
                        .build());

        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(
                () -> ImmutableSet.of("test_rpc_function"));
        Session session = TestingSession.testSessionBuilder().build();

        PlanOptimizerResult result = optimizer.optimize(
                projectNode,
                session,
                TypeProvider.empty(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP,
                false);

        assertFalse(result.isOptimizerTriggered(), "Plan should not be optimized when TRY does not wrap an RPC function");
        assertFalse(containsPlanNode(result.getPlanNode(), RPCNode.class),
                "Plan should not contain an RPCNode");
    }

    @Test
    public void testDirectRpcFunctionStillWorks()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();

        VariableReferenceExpression inputVar = variableAllocator.newVariable("input_col", VARCHAR);
        VariableReferenceExpression outputVar = variableAllocator.newVariable("output_col", VARCHAR);

        // Build: test_rpc_function(input_col, "model", "system", "{}")  (no TRY wrapper)
        CallExpression rpcCall = new CallExpression(
                "test_rpc_function",
                createFunctionHandle("test_rpc_function"),
                VARCHAR,
                ImmutableList.of(inputVar, varchar("model"), varchar("system"), varchar("{}")));

        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(inputVar),
                ImmutableList.of(),
                Optional.empty());

        ProjectNode projectNode = new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.builder()
                        .put(outputVar, rpcCall)
                        .build());

        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(
                () -> ImmutableSet.of("test_rpc_function"));
        Session session = TestingSession.testSessionBuilder().build();

        PlanOptimizerResult result = optimizer.optimize(
                projectNode,
                session,
                TypeProvider.empty(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP,
                false);

        assertTrue(result.isOptimizerTriggered(), "Plan should be optimized for direct RPC function calls");
        assertTrue(containsPlanNode(result.getPlanNode(), RPCNode.class),
                "Optimized plan should contain an RPCNode");
    }

    @Test
    public void testTryWithDirectLambdaRpcFunction()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();

        VariableReferenceExpression inputVar = variableAllocator.newVariable("input_col", VARCHAR);
        VariableReferenceExpression outputVar = variableAllocator.newVariable("output_col", VARCHAR);

        // Build: $internal$try(() -> test_rpc_function(input_col, "model", "system", "{}"))
        // Direct lambda argument without BIND
        CallExpression rpcInsideLambda = new CallExpression(
                "test_rpc_function",
                createFunctionHandle("test_rpc_function"),
                VARCHAR,
                ImmutableList.of(inputVar, varchar("model"), varchar("system"), varchar("{}")));

        LambdaDefinitionExpression lambda = new LambdaDefinitionExpression(
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                rpcInsideLambda);

        CallExpression tryCall = new CallExpression(
                "$internal$try",
                createFunctionHandle("$internal$try"),
                VARCHAR,
                ImmutableList.of(lambda));

        PlanOptimizerResult result = optimizeWithTryCall(idAllocator, variableAllocator, inputVar, outputVar, tryCall);

        assertTrue(result.isOptimizerTriggered(), "Plan should be optimized for TRY with direct lambda");
        assertTrue(containsPlanNode(result.getPlanNode(), RPCNode.class),
                "Optimized plan should contain an RPCNode");
    }

    @Test
    public void testTryWithMultipleBoundVariables()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();

        VariableReferenceExpression inputVar1 = variableAllocator.newVariable("input_col1", VARCHAR);
        VariableReferenceExpression inputVar2 = variableAllocator.newVariable("input_col2", VARCHAR);
        VariableReferenceExpression outputVar = variableAllocator.newVariable("output_col", VARCHAR);

        // Build: $internal$try(BIND(input_col1, input_col2, (a, b) -> test_rpc_function(a, b, "system", "{}")))
        CallExpression rpcInsideLambda = new CallExpression(
                "test_rpc_function",
                createFunctionHandle("test_rpc_function"),
                VARCHAR,
                ImmutableList.of(
                        new VariableReferenceExpression(Optional.empty(), "a", VARCHAR),
                        new VariableReferenceExpression(Optional.empty(), "b", VARCHAR),
                        varchar("system"),
                        varchar("{}")));

        LambdaDefinitionExpression lambda = new LambdaDefinitionExpression(
                Optional.empty(),
                ImmutableList.of(VARCHAR, VARCHAR),
                ImmutableList.of("a", "b"),
                rpcInsideLambda);

        SpecialFormExpression bind = new SpecialFormExpression(
                SpecialFormExpression.Form.BIND,
                VARCHAR,
                ImmutableList.of(inputVar1, inputVar2, lambda));

        CallExpression tryCall = new CallExpression(
                "$internal$try",
                createFunctionHandle("$internal$try"),
                VARCHAR,
                ImmutableList.of(bind));

        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(inputVar1, inputVar2),
                ImmutableList.of(),
                Optional.empty());

        ProjectNode projectNode = new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.builder()
                        .put(outputVar, tryCall)
                        .build());

        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(
                () -> ImmutableSet.of("test_rpc_function"));
        Session session = TestingSession.testSessionBuilder().build();

        PlanOptimizerResult result = optimizer.optimize(
                projectNode, session, TypeProvider.empty(), variableAllocator, idAllocator, WarningCollector.NOOP, false);

        assertTrue(result.isOptimizerTriggered(), "Plan should be optimized for multi-param TRY");
        assertTrue(containsPlanNode(result.getPlanNode(), RPCNode.class),
                "Optimized plan should contain an RPCNode");
    }

    private PlanOptimizerResult optimizeWithTryCall(
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            VariableReferenceExpression inputVar,
            VariableReferenceExpression outputVar,
            CallExpression tryCall)
    {
        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(inputVar),
                ImmutableList.of(),
                Optional.empty());

        ProjectNode projectNode = new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.builder()
                        .put(outputVar, tryCall)
                        .build());

        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(
                () -> ImmutableSet.of("test_rpc_function"));
        Session session = TestingSession.testSessionBuilder().build();

        return optimizer.optimize(
                projectNode, session, TypeProvider.empty(), variableAllocator, idAllocator, WarningCollector.NOOP, false);
    }

    @Test
    public void testMultipleTryWrappedRpcCallsSurvivePruning()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();

        VariableReferenceExpression inputVar = variableAllocator.newVariable("content", VARCHAR);
        VariableReferenceExpression out1 = variableAllocator.newVariable("topic", VARCHAR);
        VariableReferenceExpression out2 = variableAllocator.newVariable("objective", VARCHAR);
        VariableReferenceExpression out3 = variableAllocator.newVariable("audience", VARCHAR);

        // 3 TRY-wrapped RPC calls: TRY(BIND(content, (p) -> rpc(p, ...)))
        CallExpression tryCall1 = createTryWrappedRpcCall(inputVar, "topic_prompt");
        CallExpression tryCall2 = createTryWrappedRpcCall(inputVar, "objective_prompt");
        CallExpression tryCall3 = createTryWrappedRpcCall(inputVar, "audience_prompt");

        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(inputVar),
                ImmutableList.of(),
                Optional.empty());

        ProjectNode projectNode = new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.builder()
                        .put(out1, tryCall1)
                        .put(out2, tryCall2)
                        .put(out3, tryCall3)
                        .build());

        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(
                () -> ImmutableSet.of("test_rpc_function"));
        Session session = TestingSession.testSessionBuilder().build();

        PlanOptimizerResult rpcResult = optimizer.optimize(
                projectNode, session, TypeProvider.empty(), variableAllocator, idAllocator, WarningCollector.NOOP, false);
        assertTrue(rpcResult.isOptimizerTriggered());

        int rpcCount = countPlanNodes(rpcResult.getPlanNode(), RPCNode.class);
        assertEquals(rpcCount, 3, "Should create 3 RPCNodes");

        // Wrap in OutputNode so PruneUnreferencedOutputs has a root
        // that declares which variables are needed (otherwise it starts
        // with empty context and prunes everything).
        PlanNode rpcPlan = rpcResult.getPlanNode();
        OutputNode outputNode = new OutputNode(
                Optional.empty(),
                idAllocator.getNextId(),
                rpcPlan,
                ImmutableList.of("topic", "objective", "audience"),
                rpcPlan.getOutputVariables());

        PruneUnreferencedOutputs pruner = new PruneUnreferencedOutputs();
        PlanOptimizerResult pruneResult = pruner.optimize(
                outputNode, session, TypeProvider.empty(), variableAllocator, idAllocator, WarningCollector.NOOP, false);

        PlanNode prunedPlan = pruneResult.getPlanNode();
        assertEquals(countPlanNodes(prunedPlan, RPCNode.class), 3,
                "All 3 RPCNodes must survive pruning");

        // The OutputNode wraps a ProjectNode
        assertTrue(prunedPlan instanceof OutputNode);
        PlanNode child = ((OutputNode) prunedPlan).getSource();
        assertTrue(child instanceof ProjectNode);
        ProjectNode finalProject = (ProjectNode) child;
        assertEquals(finalProject.getOutputVariables().size(), 3,
                "Final projection must output all 3 result variables");
    }

    @Test
    public void testTryWrappedRpcReturnsVariableDirectly()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();

        VariableReferenceExpression inputVar = variableAllocator.newVariable("input_col", VARCHAR);
        VariableReferenceExpression outputVar = variableAllocator.newVariable("output_col", VARCHAR);

        CallExpression tryCall = createTryWrappedRpcCall(inputVar, "prompt");

        PlanOptimizerResult result = optimizeWithTryCall(idAllocator, variableAllocator, inputVar, outputVar, tryCall);
        assertTrue(result.isOptimizerTriggered());
        assertTrue(containsPlanNode(result.getPlanNode(), RPCNode.class));

        // The final ProjectNode's expression for outputVar should be
        // a direct VariableReferenceExpression (the __rpc_result_ variable),
        // not a TRY(lambda) wrapping it.
        ProjectNode finalProject = (ProjectNode) result.getPlanNode();
        RowExpression outputExpr = finalProject.getAssignments().get(outputVar);
        assertTrue(outputExpr instanceof VariableReferenceExpression,
                "TRY(rpc_fn(...)) should rewrite to a direct variable reference, not a TRY lambda. Got: " + outputExpr.getClass().getSimpleName());
    }

    @Test
    public void testTryWrappedCastRpcReconstructsBind()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();

        VariableReferenceExpression inputVar = variableAllocator.newVariable("input_col", VARCHAR);
        VariableReferenceExpression outputVar = variableAllocator.newVariable("output_col", INTEGER);

        // TRY(CAST(rpc_fn(p, ...) AS INTEGER)) as
        // $internal$try(BIND(input_col, (p) -> CAST(rpc_fn(p, ...) AS INTEGER))).
        // The body is a CAST (not a bare variable), so this exercises the
        // extractFreeVariables + BIND-reconstruction branch: the TRY must be
        // kept (a CAST can throw) but rebuilt as a BIND that exposes
        // __rpc_result, otherwise PruneUnreferencedOutputs drops it.
        CallExpression rpcInsideLambda = new CallExpression(
                "test_rpc_function",
                createFunctionHandle("test_rpc_function"),
                VARCHAR,
                ImmutableList.of(
                        new VariableReferenceExpression(Optional.empty(), "p", VARCHAR),
                        varchar("prompt"),
                        varchar("system"),
                        varchar("{}")));
        CallExpression castExpr = new CallExpression(
                "CAST",
                createFunctionHandle("CAST"),
                INTEGER,
                ImmutableList.of(rpcInsideLambda));
        LambdaDefinitionExpression lambda = new LambdaDefinitionExpression(
                Optional.empty(),
                ImmutableList.of(VARCHAR),
                ImmutableList.of("p"),
                castExpr);
        SpecialFormExpression bind = new SpecialFormExpression(
                SpecialFormExpression.Form.BIND,
                INTEGER,
                ImmutableList.of(inputVar, lambda));
        CallExpression tryCall = new CallExpression(
                "$internal$try",
                createFunctionHandle("$internal$try"),
                INTEGER,
                ImmutableList.of(bind));

        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(inputVar),
                ImmutableList.of(),
                Optional.empty());
        ProjectNode projectNode = new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.builder().put(outputVar, tryCall).build());

        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(
                () -> ImmutableSet.of("test_rpc_function"));
        Session session = TestingSession.testSessionBuilder().build();

        PlanOptimizerResult result = optimizer.optimize(
                projectNode, session, TypeProvider.empty(), variableAllocator, idAllocator, WarningCollector.NOOP, false);

        assertTrue(result.isOptimizerTriggered());
        assertTrue(containsPlanNode(result.getPlanNode(), RPCNode.class));

        // The complex body must stay wrapped in $internal$try, but rebuilt as a
        // BIND whose leading argument is the __rpc_result variable (exposed to
        // VariablesExtractor), not a bare zero-arg lambda that hides it.
        ProjectNode finalProject = (ProjectNode) result.getPlanNode();
        RowExpression outputExpr = finalProject.getAssignments().get(outputVar);
        assertTrue(outputExpr instanceof CallExpression,
                "Complex TRY body should stay wrapped in $internal$try. Got: " + outputExpr.getClass().getSimpleName());
        CallExpression tryExpr = (CallExpression) outputExpr;
        assertEquals(tryExpr.getDisplayName(), "$internal$try");
        RowExpression tryArg = tryExpr.getArguments().get(0);
        assertTrue(tryArg instanceof SpecialFormExpression
                        && ((SpecialFormExpression) tryArg).getForm() == SpecialFormExpression.Form.BIND,
                "Complex TRY body must be reconstructed as a BIND. Got: " + tryArg.getClass().getSimpleName());
        List<RowExpression> bindArgs = ((SpecialFormExpression) tryArg).getArguments();
        assertTrue(bindArgs.get(0) instanceof VariableReferenceExpression,
                "BIND must expose the rpc result variable as a bound argument. Got: " + bindArgs.get(0).getClass().getSimpleName());

        // The reconstructed BIND fully applies its captured variable(s) to the
        // lambda, so its type must be the bound function type () -> INTEGER, not
        // the scalar TRY result type. Regression guard for the BIND mistyping
        // (previously tryCall.getType()); matches PlanRemoteProjections.
        RowExpression bindExpr = tryArg;
        assertTrue(bindExpr.getType() instanceof FunctionType,
                "Reconstructed BIND must be typed as a FunctionType. Got: " + bindExpr.getType());
        FunctionType bindType = (FunctionType) bindExpr.getType();
        assertTrue(bindType.getArgumentTypes().isEmpty(),
                "Fully-bound BIND must have zero argument types. Got: " + bindType.getArgumentTypes());
        assertEquals(bindType.getReturnType(), INTEGER,
                "BIND return type must be the CAST body type INTEGER");

        // And it must survive PruneUnreferencedOutputs.
        PlanNode rpcPlan = result.getPlanNode();
        OutputNode outputNode = new OutputNode(
                Optional.empty(),
                idAllocator.getNextId(),
                rpcPlan,
                ImmutableList.of("output_col"),
                rpcPlan.getOutputVariables());
        PruneUnreferencedOutputs pruner = new PruneUnreferencedOutputs();
        PlanOptimizerResult pruneResult = pruner.optimize(
                outputNode, session, TypeProvider.empty(), variableAllocator, idAllocator, WarningCollector.NOOP, false);
        assertEquals(countPlanNodes(pruneResult.getPlanNode(), RPCNode.class), 1,
                "RPCNode must survive pruning for a TRY(CAST(rpc_fn())) body");
    }

    private static CallExpression createTryWrappedRpcCall(VariableReferenceExpression inputVar, String prompt)
    {
        CallExpression rpcInsideLambda = new CallExpression(
                "test_rpc_function",
                createFunctionHandle("test_rpc_function"),
                VARCHAR,
                ImmutableList.of(
                        new VariableReferenceExpression(Optional.empty(), "p", VARCHAR),
                        varchar(prompt),
                        varchar("system"),
                        varchar("{}")));

        LambdaDefinitionExpression lambda = new LambdaDefinitionExpression(
                Optional.empty(),
                ImmutableList.of(VARCHAR),
                ImmutableList.of("p"),
                rpcInsideLambda);

        SpecialFormExpression bind = new SpecialFormExpression(
                SpecialFormExpression.Form.BIND,
                VARCHAR,
                ImmutableList.of(inputVar, lambda));

        return new CallExpression(
                "$internal$try",
                createFunctionHandle("$internal$try"),
                VARCHAR,
                ImmutableList.of(bind));
    }

    private static int countPlanNodes(PlanNode node, Class<? extends PlanNode> targetClass)
    {
        int count = targetClass.isInstance(node) ? 1 : 0;
        for (PlanNode child : node.getSources()) {
            count += countPlanNodes(child, targetClass);
        }
        return count;
    }

    private static boolean containsPlanNode(PlanNode node, Class<? extends PlanNode> targetClass)
    {
        if (targetClass.isInstance(node)) {
            return true;
        }
        return node.getSources().stream().anyMatch(source -> containsPlanNode(source, targetClass));
    }

    // ── RpcExecutionPolicy delegation (rpc_streaming_mode resolution) ──
    // The engine computes the estimated input cardinality and delegates the concrete
    // PER_ROW/BATCH decision to the injected RpcExecutionPolicy. These verify the OSS default
    // (no batching heuristic: AUTOMATIC -> PER_ROW) and that the optimizer applies whatever an
    // injected policy returns. The cardinality/threshold business logic lives in a deployment's
    // policy implementation and is unit-tested there.

    // Builds SELECT test_rpc_function(input_col, ...) over a ValuesNode, runs the optimizer
    // with the given session + execution policy, and returns the produced RPCNode.
    private RPCNode optimizeToRpcNode(Session session, RpcExecutionPolicy policy)
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();
        VariableReferenceExpression inputVar = variableAllocator.newVariable("input_col", VARCHAR);
        VariableReferenceExpression outputVar = variableAllocator.newVariable("output_col", VARCHAR);

        CallExpression rpcCall = new CallExpression(
                "test_rpc_function",
                createFunctionHandle("test_rpc_function"),
                VARCHAR,
                ImmutableList.of(inputVar, varchar("model"), varchar("system"), varchar("{}")));

        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(inputVar),
                ImmutableList.of(),
                Optional.empty());

        ProjectNode projectNode = new ProjectNode(
                idAllocator.getNextId(),
                source,
                Assignments.builder()
                        .put(outputVar, rpcCall)
                        .build());

        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(() -> ImmutableSet.of("test_rpc_function"), null, policy);

        PlanOptimizerResult result = optimizer.optimize(
                projectNode, session, TypeProvider.empty(), variableAllocator, idAllocator, WarningCollector.NOOP, false);

        RPCNode rpcNode = (RPCNode) findPlanNode(result.getPlanNode(), RPCNode.class);
        assertTrue(rpcNode != null, "Optimized plan should contain an RPCNode");
        return rpcNode;
    }

    private RPCNode.StreamingMode optimizeWithPolicy(Session session, RpcExecutionPolicy policy)
    {
        return optimizeToRpcNode(session, policy).getStreamingMode();
    }

    private static Session streamingModeSession(String mode)
    {
        return TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", mode)
                .build();
    }

    // Runs a policy's translateIntent for the given requested mode + input stats and returns the
    // resolved streaming mode.
    private static RPCNode.StreamingMode resolvedMode(RpcExecutionPolicy policy, RPCNode.StreamingMode requested, PlanNodeStatsEstimate stats, Session session)
    {
        RpcExecutionIntent intent = RpcExecutionIntent.builder()
                .setRequestedMode(requested)
                .setInputStats(stats)
                .setSession(session)
                .setFunctionName("test_rpc_function")
                .build();
        return policy.translateIntent(intent).getStreamingMode();
    }

    @Test
    public void testDefaultPolicyResolvesAutomaticToPerRow()
    {
        // OSS default carries no batching heuristic: AUTOMATIC -> PER_ROW; explicit modes pass through.
        DefaultRpcExecutionPolicy policy = new DefaultRpcExecutionPolicy();
        Session session = TestingSession.testSessionBuilder().build();
        PlanNodeStatsEstimate largeStats = PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000).build();
        assertEquals(resolvedMode(policy, RPCNode.StreamingMode.AUTOMATIC, largeStats, session), RPCNode.StreamingMode.PER_ROW);
        assertEquals(resolvedMode(policy, RPCNode.StreamingMode.AUTOMATIC, PlanNodeStatsEstimate.unknown(), session), RPCNode.StreamingMode.PER_ROW);
        assertEquals(resolvedMode(policy, RPCNode.StreamingMode.BATCH, PlanNodeStatsEstimate.builder().setOutputRowCount(1).build(), session), RPCNode.StreamingMode.BATCH);
        assertEquals(resolvedMode(policy, RPCNode.StreamingMode.PER_ROW, largeStats, session), RPCNode.StreamingMode.PER_ROW);

        Session sessionWithAutoTaskCap = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_batch_max_auto_task_count", "8")
                .build();
        RpcExecutionIntent intent = RpcExecutionIntent.builder()
                .setRequestedMode(RPCNode.StreamingMode.BATCH)
                .setInputStats(largeStats)
                .setSession(sessionWithAutoTaskCap)
                .setFunctionName("test_rpc_function")
                .build();
        assertEquals(policy.translateIntent(intent).getRecommendedTaskCount(), 0);
    }

    @Test
    public void testOptimizerDelegatesToExecutionPolicy()
    {
        // The optimizer applies whatever the injected policy returns to the produced RPCNode.
        RpcExecutionPolicy toBatch = intent -> RpcExecutionProperties.of(RPCNode.StreamingMode.BATCH);
        RpcExecutionPolicy toPerRow = intent -> RpcExecutionProperties.of(RPCNode.StreamingMode.PER_ROW);
        assertEquals(optimizeWithPolicy(streamingModeSession("AUTOMATIC"), toBatch), RPCNode.StreamingMode.BATCH);
        assertEquals(optimizeWithPolicy(streamingModeSession("AUTOMATIC"), toPerRow), RPCNode.StreamingMode.PER_ROW);

        RpcExecutionPolicy withRecommendedTaskCount = intent -> RpcExecutionProperties.builder()
                .setStreamingMode(RPCNode.StreamingMode.BATCH)
                .setRecommendedTaskCount(4)
                .build();
        assertEquals(
                optimizeToRpcNode(streamingModeSession("AUTOMATIC"), withRecommendedTaskCount).getRecommendedTaskCount(),
                4);
    }

    @Test
    public void testRecommendedTaskCountIsCoordinatorOnly()
            throws Exception
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();
        VariableReferenceExpression input = variableAllocator.newVariable("input", VARCHAR);
        VariableReferenceExpression output = variableAllocator.newVariable("output", VARCHAR);
        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(input),
                ImmutableList.of(),
                Optional.empty());
        RPCNode rpcNode = new RPCNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                "test_rpc_function",
                ImmutableList.of(input),
                ImmutableList.of(input.getName()),
                output,
                RPCNode.StreamingMode.BATCH,
                128,
                4);
        ObjectMapper objectMapper = new JsonObjectMapperProvider().get()
                .registerModule(new SimpleModule()
                        .addDeserializer(Type.class, new TestingTypeDeserializer(new TestingTypeManager())));

        String json = objectMapper.writeValueAsString(rpcNode);
        assertFalse(json.contains("recommendedTaskCount"));
        assertEquals(objectMapper.readValue(json, RPCNode.class).getRecommendedTaskCount(), 0);
    }

    @Test
    public void testGroupingMetadataSurvivesSerializationAndSymbolRewrites()
            throws Exception
    {
        VariableAllocator variables = new VariableAllocator();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableReferenceExpression receipt = variables.newVariable("receipt", fakeReceiptType());
        VariableReferenceExpression alias = variables.newVariable("receipt_alias", fakeReceiptType());
        VariableReferenceExpression ready = variables.newVariable("ready", fakeReceiptType());
        ValuesNode values = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(receipt), ImmutableList.of(), Optional.empty());
        ProjectNode source = new ProjectNode(ids.getNextId(), values, Assignments.builder().put(alias, receipt).build());
        RPCNode rpc = new RPCNode(Optional.empty(), ids.getNextId(), Optional.empty(), source, "fake_poll",
                ImmutableList.of(alias, varchar("{}")), ImmutableList.of(alias.getName(), "constant_options"), ready,
                RPCNode.StreamingMode.BATCH, 2, 1, RPCNode.ExecutionPhase.POLL, ImmutableList.of(fakeGrouping(alias)));
        ObjectMapper mapper = planNodeObjectMapper();
        String json = mapper.writeValueAsString(rpc);
        RPCNode copy = mapper.readValue(json, RPCNode.class);
        assertEquals(copy.getExecutionPhase(), RPCNode.ExecutionPhase.POLL);
        assertEquals(copy.getInputGroupingExpressions(), rpc.getInputGroupingExpressions());
        assertEquals(((RPCNode) copy.replaceChildren(ImmutableList.of(source))).getExecutionPhase(), RPCNode.ExecutionPhase.POLL);
        assertEquals(((RPCNode) copy.replaceChildren(ImmutableList.of(source))).getInputGroupingExpressions(), rpc.getInputGroupingExpressions());
        assertEquals(((RPCNode) copy.assignStatsEquivalentPlanNode(Optional.empty())).getInputGroupingExpressions(), rpc.getInputGroupingExpressions());
        com.fasterxml.jackson.databind.node.ObjectNode oldJson = (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(json);
        oldJson.remove("inputGroupingExpressions");
        assertTrue(mapper.treeToValue(oldJson, RPCNode.class).getInputGroupingExpressions().isEmpty());
        PlanNode rewritten = new UnaliasSymbolReferences(FUNCTION_AND_TYPE_MANAGER)
                .optimize(rpc, streamingModeSession("PER_ROW"), TypeProvider.viewOf(variables.getVariables()), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        RPCNode canonical = (RPCNode) rewritten;
        assertEquals(canonical.getExecutionPhase(), RPCNode.ExecutionPhase.POLL);
        assertEquals(((SpecialFormExpression) canonical.getInputGroupingExpressions().get(0)).getArguments().get(0), canonical.getArguments().get(0));
        assertTrue(com.facebook.presto.sql.planner.ExpressionExtractor.extractExpressionsNonRecursive(canonical).contains(canonical.getInputGroupingExpressions().get(0)));
        new ValidateDependenciesChecker().validate(canonical);
    }

    @Test
    public void testOptimizerRejectsPolicyReturningAutomatic()
    {
        // The optimizer enforces the policy contract at plan time: a policy that returns
        // AUTOMATIC (e.g. by passing the requested mode through unchanged) must fail loudly
        // here rather than leak AUTOMATIC into the serialized RPCNode and fail at the worker.
        RpcExecutionPolicy passthrough = intent -> RpcExecutionProperties.of(intent.getRequestedMode());
        try {
            optimizeWithPolicy(streamingModeSession("AUTOMATIC"), passthrough);
            fail("expected rejection when policy returns AUTOMATIC");
        }
        catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("AUTOMATIC"), "message should mention AUTOMATIC: " + expected.getMessage());
        }
    }

    @DataProvider
    public Object[][] nonPhysicalStreamingModes()
    {
        return new Object[][] {{RPCNode.StreamingMode.AUTOMATIC}, {RPCNode.StreamingMode.BATCH_DEFERRED}};
    }

    @Test(dataProvider = "nonPhysicalStreamingModes")
    public void testRpcNodeConstructorRejectsPlanningModes(RPCNode.StreamingMode mode)
    {
        // Defense in depth: even outside the optimizer, an RPCNode must never be constructed with
        // AUTOMATIC (this guard also covers the @JsonCreator deserialization path), so the value
        // can never reach a native worker that only understands PER_ROW / BATCH.
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();
        VariableReferenceExpression inputVar = variableAllocator.newVariable("input_col", VARCHAR);
        VariableReferenceExpression outputVar = variableAllocator.newVariable("output_col", VARCHAR);
        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(inputVar),
                ImmutableList.of(),
                Optional.empty());
        try {
            new RPCNode(
                    Optional.empty(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    source,
                    "test_rpc_function",
                    ImmutableList.of(inputVar),
                    ImmutableList.of("input_col"),
                    outputVar,
                    mode,
                    0,
                    0);
            fail("expected rejection when constructing RPCNode with AUTOMATIC");
        }
        catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(mode.name()), "message should mention AUTOMATIC: " + expected.getMessage());
        }
    }

    @Test(dataProvider = "nonPhysicalStreamingModes")
    public void testRpcNodeJsonCreatorRejectsPlanningModes(RPCNode.StreamingMode mode)
    {
        // The @JsonCreator constructor (the deserialization entry point) must route through the
        // same guard, so a JSON plan payload carrying streamingMode=AUTOMATIC is rejected at the
        // worker boundary. Guards against a future refactor that splits the @JsonCreator path
        // from the full constructor.
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        VariableAllocator variableAllocator = new VariableAllocator();
        VariableReferenceExpression inputVar = variableAllocator.newVariable("input_col", VARCHAR);
        VariableReferenceExpression outputVar = variableAllocator.newVariable("output_col", VARCHAR);
        ValuesNode source = new ValuesNode(
                Optional.empty(),
                idAllocator.getNextId(),
                ImmutableList.of(inputVar),
                ImmutableList.of(),
                Optional.empty());
        try {
            // 9-arg @JsonCreator constructor (no statsEquivalentPlanNode; boxed dispatchBatchSize).
            new RPCNode(
                    Optional.empty(),
                    idAllocator.getNextId(),
                    source,
                    "test_rpc_function",
                    ImmutableList.of(inputVar),
                    ImmutableList.of("input_col"),
                    outputVar,
                    mode,
                    Integer.valueOf(0), RPCNode.ExecutionPhase.INLINE);
            fail("expected rejection when deserializing an RPCNode with AUTOMATIC");
        }
        catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains(mode.name()), "message should mention AUTOMATIC: " + expected.getMessage());
        }
    }

    @Test
    public void testRpcBatchMinRowsRejectsNonPositive()
    {
        // rpc_batch_min_rows must be > 0: 0 or negative would make (rowCount >= threshold)
        // always true and silently force BATCH on every query under AUTOMATIC.
        PropertyMetadata<?> meta = new SystemSessionProperties().getSessionProperties().stream()
                .filter(p -> p.getName().equals(SystemSessionProperties.RPC_BATCH_MIN_ROWS))
                .findFirst()
                .orElseThrow(() -> new AssertionError("rpc_batch_min_rows property not registered"));

        // A positive value decodes through unchanged.
        assertEquals(meta.decode(2000), 2000);

        // 0 and negative are rejected with INVALID_SESSION_PROPERTY.
        for (int bad : new int[] {0, -1}) {
            try {
                meta.decode(bad);
                fail("expected rejection for rpc_batch_min_rows=" + bad);
            }
            catch (PrestoException expected) {
                assertEquals(expected.getErrorCode(), INVALID_SESSION_PROPERTY.toErrorCode());
            }
        }
    }

    @Test
    public void testDeferredTaskCountMetadataRejectsInvalidValues()
    {
        for (PropertyMetadata<?> property : new SystemSessionProperties().getSessionProperties()) {
            if (property.getName().equals(SystemSessionProperties.RPC_DEFERRED_POLL_TASK_COUNT)) {
                assertEquals(property.decode(1), 1);
                for (int value : new int[] {-1, 0}) {
                    PrestoException failure = expectThrows(PrestoException.class, () -> property.decode(value));
                    assertEquals(failure.getErrorCode(), INVALID_SESSION_PROPERTY.toErrorCode());
                }
            }
            if (property.getName().equals(SystemSessionProperties.RPC_DEFERRED_COLLECT_TASK_COUNT)) {
                assertEquals(property.decode(0), 0);
                assertEquals(property.decode(8), 8);
                PrestoException failure = expectThrows(PrestoException.class, () -> property.decode(-1));
                assertEquals(failure.getErrorCode(), INVALID_SESSION_PROPERTY.toErrorCode());
            }
        }
    }

    @Test
    public void testRpcBatchMaxAutoTaskCountRejectsNonPositive()
    {
        PropertyMetadata<?> meta = new SystemSessionProperties().getSessionProperties().stream()
                .filter(p -> p.getName().equals(SystemSessionProperties.RPC_BATCH_MAX_AUTO_TASK_COUNT))
                .findFirst()
                .orElseThrow(() -> new AssertionError("rpc_batch_max_auto_task_count property not registered"));

        assertEquals(meta.decode(1), 1);
        for (int bad : new int[] {0, -1}) {
            try {
                meta.decode(bad);
                fail("expected rejection for rpc_batch_max_auto_task_count=" + bad);
            }
            catch (PrestoException expected) {
                assertEquals(expected.getErrorCode(), INVALID_SESSION_PROPERTY.toErrorCode());
            }
        }
    }

    private static PlanNode findPlanNode(PlanNode node, Class<? extends PlanNode> targetClass)
    {
        if (targetClass.isInstance(node)) {
            return node;
        }
        for (PlanNode source : node.getSources()) {
            PlanNode found = findPlanNode(source, targetClass);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
    @DataProvider
    public Object[][] deferredFakeBackendModes()
    {
        return new Object[][] {
                {false, "fake_backend", "BATCH", 1},
                {true, "fake_backend", "BATCH", 3},
                {false, "unregistered", "BATCH", 1},
                {false, "other_backend", "BATCH", 1},
                {false, "fake_backend", "PER_ROW", 1},
                {false, "fake_backend", "AUTOMATIC", 1}};
    }

    @Test(dataProvider = "deferredFakeBackendModes")
    public void testDeferredFakeBackendIsExplicitlyEnabled(boolean enabled, String backend, String mode, int expectedNodes)
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", enabled ? "BATCH_DEFERRED" : mode)
                .setSystemProperty("rpc_deferred_poll_task_count", "1")
                .setSystemProperty("rpc_deferred_collect_task_count", "2")
                .build();
        PlanNode plan = optimizeFakeBackend(session, backend, true);
        List<RPCNode> nodes = collectRpcNodes(plan);
        assertEquals(nodes.size(), expectedNodes);
        if (expectedNodes == 3) {
            assertEquals(nodes.get(0).getExecutionPhase(), RPCNode.ExecutionPhase.COLLECT);
            assertEquals(nodes.get(1).getExecutionPhase(), RPCNode.ExecutionPhase.POLL);
            assertEquals(nodes.get(2).getExecutionPhase(), RPCNode.ExecutionPhase.SUBMIT);
            assertEquals(nodes.get(0).getRecommendedTaskCount(), 2);
            assertEquals(nodes.get(1).getRecommendedTaskCount(), 1);
            assertEquals(nodes.get(2).getFunctionName(), "fake_submit");
            assertEquals(nodes.get(0).getFunctionName(), "fake_collect");
            assertFalse(nodes.get(1).getSource().getOutputVariables().stream()
                    .anyMatch(variable -> variable.getName().contains("options")));
            assertFalse(nodes.get(1).getSource().getOutputVariables().stream()
                    .anyMatch(variable -> variable.getName().equals("prompt")));
            new ValidateDependenciesChecker().validate(plan);
        }
        else {
            assertEquals(nodes.get(0).getExecutionPhase(), RPCNode.ExecutionPhase.INLINE);
            assertEquals(nodes.get(0).getFunctionName(), "fake_completion");
        }
    }

    @Test
    public void testBatchModeRemainsInline()
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", "BATCH")
                .build();
        assertEquals(collectRpcNodes(optimizeFakeBackend(session, "fake_backend", true)).size(), 1);
    }

    @DataProvider
    public Object[][] deferredProjectedArguments()
    {
        return new Object[][] {
                {true, "fake_backend", "constant", false, 3},
                {true, "fake_backend", "constant", true, 3},
                {false, "fake_backend", "constant", true, 1},
                {true, "unregistered", "constant", true, 0},
                {true, "other_backend", "constant", true, 0},
                {true, "fake_backend", "dynamic_model", true, 0},
                {true, "fake_backend", "dynamic_options", true, 0},
                {true, "fake_backend", "nondeterministic_options", true, 0},
                {true, "fake_backend", "invalid_options_handle", true, 0}};
    }

    @Test(dataProvider = "deferredProjectedArguments")
    public void testDeferredFakeBackendResolvesOnlyProvenProjectedConstants(boolean enabled, String backend, String argumentKind, boolean withTry, int expectedNodes)
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", enabled ? "BATCH_DEFERRED" : "BATCH")
                .build();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableAllocator variables = new VariableAllocator();
        VariableReferenceExpression prompt = variables.newVariable("prompt", VARCHAR);
        VariableReferenceExpression dynamic = variables.newVariable("dynamic", VARCHAR);
        VariableReferenceExpression model = variables.newVariable("model", VARCHAR);
        VariableReferenceExpression extraOptions = variables.newVariable("extra_options", VARCHAR);
        VariableReferenceExpression modelAlias = variables.newVariable("model_alias", VARCHAR);
        VariableReferenceExpression optionsAlias = variables.newVariable("options_alias", VARCHAR);
        VariableReferenceExpression output = variables.newVariable("output", VARCHAR);
        ValuesNode values = new ValuesNode(Optional.empty(), ids.getNextId(),
                ImmutableList.of(prompt, dynamic), ImmutableList.of(), Optional.empty());

        String optionsSuffix = ",\"streaming_mode\":\"" + (enabled ? "batch_deferred" : "batch") + "\",\"dispatch_batch_size\":10";
        RowExpression suffix = varchar(optionsSuffix);
        if (argumentKind.equals("dynamic_options")) {
            suffix = dynamic;
        }
        else if (argumentKind.equals("nondeterministic_options")) {
            suffix = new CallExpression("CAST", FUNCTION_AND_TYPE_MANAGER.lookupCast(CAST, DOUBLE, VARCHAR), VARCHAR,
                    ImmutableList.of(localCall("random", DOUBLE)));
        }
        else if (argumentKind.equals("invalid_options_handle")) {
            suffix = new CallExpression("unknown_options", createFunctionHandle("unknown_options"), VARCHAR, ImmutableList.of());
        }
        ProjectNode constants = new ProjectNode(ids.getNextId(), values,
                Assignments.builder().put(prompt, prompt)
                        .put(model, argumentKind.equals("dynamic_model") ? dynamic : varchar("model"))
                        .put(extraOptions, suffix).build());
        RowExpression optionalSuffix = new SpecialFormExpression(SpecialFormExpression.Form.IF, VARCHAR,
                new SpecialFormExpression(SpecialFormExpression.Form.IS_NULL, BOOLEAN, extraOptions),
                varchar(""), extraOptions);
        RowExpression options = localCall("concat", VARCHAR,
                varchar("{\"inference_backend\":\"" + backend + "\""), optionalSuffix, varchar("}"));
        ProjectNode aliases = new ProjectNode(ids.getNextId(), constants,
                Assignments.builder().put(prompt, prompt).put(modelAlias, model).put(optionsAlias, options).build());
        FilterNode source = new FilterNode(Optional.empty(), ids.getNextId(), aliases, new ConstantExpression(true, BOOLEAN));
        CallExpression call = new CallExpression("fake_completion", createFunctionHandle("fake_completion"), VARCHAR,
                ImmutableList.of(prompt, modelAlias, varchar(""), optionsAlias));
        RowExpression expression = call;
        if (withTry) {
            CallExpression lambdaCall = new CallExpression("fake_completion", createFunctionHandle("fake_completion"), VARCHAR,
                    ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "p", VARCHAR),
                            new VariableReferenceExpression(Optional.empty(), "m", VARCHAR), varchar(""),
                            new VariableReferenceExpression(Optional.empty(), "o", VARCHAR)));
            LambdaDefinitionExpression lambda = new LambdaDefinitionExpression(Optional.empty(),
                    ImmutableList.of(VARCHAR, VARCHAR, VARCHAR), ImmutableList.of("p", "m", "o"), lambdaCall);
            RowExpression bind = new SpecialFormExpression(SpecialFormExpression.Form.BIND, new FunctionType(ImmutableList.of(), VARCHAR),
                    ImmutableList.of(prompt, modelAlias, optionsAlias, lambda));
            expression = new CallExpression("$internal$try", createFunctionHandle("$internal$try"), VARCHAR,
                    ImmutableList.of(bind));
        }
        ProjectNode project = new ProjectNode(ids.getNextId(), source, Assignments.builder().put(output, expression).build());
        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(() -> ImmutableSet.of("fake_completion"), null, new DefaultRpcExecutionPolicy(), true, FUNCTION_AND_TYPE_MANAGER, fakeRegistry());
        if (expectedNodes == 0) {
            if (!backend.equals("fake_backend")) {
                PrestoException error = expectThrows(PrestoException.class, () ->
                        optimizer.optimize(project, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false));
                assertEquals(error.getErrorCode(), NOT_SUPPORTED.toErrorCode());
                assertTrue(error.getMessage().contains("No BATCH_DEFERRED"));
                return;
            }
            IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () ->
                    optimizer.optimize(project, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false));
            if (argumentKind.equals("invalid_options_handle")) {
                assertEquals(error.getMessage(), "Expect BuiltInFunctionHandle");
            }
            else {
                assertTrue(error.getMessage().contains("BATCH_DEFERRED"));
            }
            return;
        }
        PlanNode plan = optimizer.optimize(project, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        List<RPCNode> nodes = collectRpcNodes(plan);
        assertEquals(nodes.size(), expectedNodes);
        if (expectedNodes == 3) {
            RPCNode submit = nodes.get(2);
            ConstantExpression expectedOptions = varchar("{\"inference_backend\":\"fake_backend\"" + optionsSuffix + "}");
            assertEquals(submit.getExecutionPhase(), RPCNode.ExecutionPhase.SUBMIT);
            assertEquals(submit.getArguments().get(1), varchar("model"));
            assertEquals(submit.getArguments().get(3), expectedOptions);
            assertEquals(nodes.get(1).getArguments().get(1), expectedOptions);
            assertEquals(nodes.get(0).getArguments().get(1), expectedOptions);
            for (RPCNode node : nodes) {
                assertEquals(node.getDispatchBatchSize(), 10);
            }
        }
        else {
            assertEquals(nodes.get(0).getExecutionPhase(), RPCNode.ExecutionPhase.INLINE);
            assertEquals(nodes.get(0).getStreamingMode(), RPCNode.StreamingMode.BATCH);
            assertEquals(nodes.get(0).getDispatchBatchSize(), 10);
            assertEquals(nodes.get(0).getArguments(), call.getArguments());
            assertEquals(nodes.get(0).getArgumentColumns().get(3), optionsAlias.getName());
        }
        new ValidateDependenciesChecker().validate(plan);
    }

    private static CallExpression localCall(String name, Type returnType, RowExpression... arguments)
    {
        List<Type> types = new ArrayList<>();
        for (RowExpression argument : arguments) {
            types.add(argument.getType());
        }
        return new CallExpression(name, FUNCTION_AND_TYPE_MANAGER.lookupFunction(name, fromTypes(types)), returnType, ImmutableList.copyOf(arguments));
    }

    @DataProvider
    public Object[][] deferredNonFakeBackendBackends()
    {
        return new Object[][] {{"unregistered"}, {"other_backend"}};
    }

    @Test(dataProvider = "deferredNonFakeBackendBackends")
    public void testDeferredNonFakeBackendMultiAttachmentCallRemainsInline(String backend)
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", "BATCH")
                .build();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableAllocator variables = new VariableAllocator();
        VariableReferenceExpression prompt = variables.newVariable("prompt", VARCHAR);
        Type attachmentsType = new ArrayType(new ArrayType(VARCHAR));
        VariableReferenceExpression images = variables.newVariable("images", attachmentsType);
        VariableReferenceExpression videos = variables.newVariable("videos", attachmentsType);
        VariableReferenceExpression output = variables.newVariable("output", VARCHAR);
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(),
                ImmutableList.of(prompt, images, videos), ImmutableList.of(), Optional.empty());
        CallExpression call = new CallExpression("fake_completion", createFunctionHandle("fake_completion"), VARCHAR,
                ImmutableList.of(prompt, varchar("model"), varchar(""),
                        varchar("{\"inference_backend\":\"" + backend + "\"}"), images, videos));
        ProjectNode project = new ProjectNode(ids.getNextId(), source,
                Assignments.builder().put(output, call).build());
        PlanNode plan = new RpcFunctionOptimizer(() -> ImmutableSet.of("fake_completion"), null, new DefaultRpcExecutionPolicy(), true, FUNCTION_AND_TYPE_MANAGER, fakeRegistry())
                .optimize(project, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();

        List<RPCNode> nodes = collectRpcNodes(plan);
        assertEquals(nodes.size(), 1);
        assertEquals(nodes.get(0).getExecutionPhase(), RPCNode.ExecutionPhase.INLINE);
        assertEquals(nodes.get(0).getStreamingMode(), RPCNode.StreamingMode.BATCH);
        assertEquals(nodes.get(0).getFunctionName(), "fake_completion");
        assertEquals(nodes.get(0).getArguments(), call.getArguments());
        new ValidateDependenciesChecker().validate(plan);
    }

    @Test
    public void testDeferredFakeBackendRequiresNativeSpark()
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", "BATCH_DEFERRED")
                .build();
        PrestoException error = expectThrows(PrestoException.class, () -> optimizeFakeBackend(session, "fake_backend", false));
        assertEquals(error.getErrorCode(), NOT_SUPPORTED.toErrorCode());
        assertTrue(error.getMessage().contains("Presto-on-Spark"));
    }

    @Test
    public void testDeferredPhasesSurvivePruningAndSymbolRewrites()
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", "BATCH_DEFERRED")
                .build();
        PlanNode plan = optimizeFakeBackend(session, "fake_backend", true);
        VariableAllocator variables = new VariableAllocator();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        for (PlanOptimizer optimizer : ImmutableList.of(new PruneUnreferencedOutputs(), new UnaliasSymbolReferences(com.facebook.presto.metadata.MetadataManager.createTestMetadataManager().getFunctionAndTypeManager()))) {
            plan = optimizer.optimize(plan, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        }
        List<RPCNode> nodes = collectRpcNodes(plan);
        assertEquals(nodes.size(), 3);
        assertEquals(nodes.get(0).getExecutionPhase(), RPCNode.ExecutionPhase.COLLECT);
        assertEquals(nodes.get(1).getExecutionPhase(), RPCNode.ExecutionPhase.POLL);
        assertEquals(nodes.get(2).getExecutionPhase(), RPCNode.ExecutionPhase.SUBMIT);
        new ValidateDependenciesChecker().validate(plan);
    }

    @Test
    public void testMultipleDeferredCallsKeepTheirDependencies()
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty("rpc_streaming_mode", "BATCH_DEFERRED")
                .build();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableAllocator variables = new VariableAllocator();
        VariableReferenceExpression prompt = variables.newVariable("prompt", VARCHAR);
        VariableReferenceExpression first = variables.newVariable("first", VARCHAR);
        VariableReferenceExpression second = variables.newVariable("second", VARCHAR);
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(),
                ImmutableList.of(prompt), ImmutableList.of(), Optional.empty());
        CallExpression call = new CallExpression("fake_completion", createFunctionHandle("fake_completion"), VARCHAR,
                ImmutableList.of(prompt, varchar("model"), varchar(""), varchar("{}")));
        ProjectNode project = new ProjectNode(ids.getNextId(), source,
                Assignments.builder().put(first, call).put(second, call).build());
        PlanNode plan = new RpcFunctionOptimizer(() -> ImmutableSet.of("fake_completion"), null, new DefaultRpcExecutionPolicy(), true, FUNCTION_AND_TYPE_MANAGER, fakeRegistry())
                .optimize(project, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
        assertEquals(collectRpcNodes(plan).size(), 6);
        PlanNode pruned = new PruneUnreferencedOutputs().optimize(plan, session, TypeProvider.empty(),
                variables, ids, WarningCollector.NOOP, false).getPlanNode();
        assertEquals(collectRpcNodes(pruned).size(), 6);
        new ValidateDependenciesChecker().validate(pruned);
    }

    private PlanNode optimizeFakeBackend(Session session, String backend, boolean nativeSpark)
    {
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableAllocator variables = new VariableAllocator();
        VariableReferenceExpression prompt = variables.newVariable("prompt", VARCHAR);
        VariableReferenceExpression rowKey = variables.newVariable("row_key", VARCHAR);
        VariableReferenceExpression output = variables.newVariable("output", VARCHAR);
        ValuesNode source = new ValuesNode(Optional.empty(), ids.getNextId(),
                ImmutableList.of(prompt, rowKey), ImmutableList.of(), Optional.empty());
        CallExpression call = new CallExpression("fake_completion", createFunctionHandle("fake_completion"), VARCHAR,
                ImmutableList.of(prompt, varchar("model"), varchar(""), varchar("{\"inference_backend\":\"" + backend + "\"}")));
        ProjectNode project = new ProjectNode(ids.getNextId(), source,
                Assignments.builder().put(rowKey, rowKey).put(output, call).build());
        RpcFunctionOptimizer optimizer = new RpcFunctionOptimizer(() -> ImmutableSet.of("fake_completion"),
                null, new DefaultRpcExecutionPolicy(), nativeSpark, FUNCTION_AND_TYPE_MANAGER, fakeRegistry());
        return optimizer.optimize(project, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
    }

    private static List<RPCNode> collectRpcNodes(PlanNode plan)
    {
        List<RPCNode> nodes = new ArrayList<>();
        if (plan instanceof RPCNode) {
            nodes.add((RPCNode) plan);
        }
        for (PlanNode source : plan.getSources()) {
            nodes.addAll(collectRpcNodes(source));
        }
        return nodes;
    }

    private PlanNode optimizeModeCall(Session session, String function, String functionMode, boolean projectedOptions,
            int mediaCount, boolean nativeSpark, RpcExecutionPolicy policy)
    {
        VariableAllocator variables = new VariableAllocator();
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        VariableReferenceExpression prompt = variables.newVariable("prompt", VARCHAR);
        VariableReferenceExpression rowKey = variables.newVariable("row_key", VARCHAR);
        boolean embedding = function.startsWith("fake_embedding") || function.equals("fake_embedding_alias");
        boolean completion = ImmutableSet.of("fake_completion", "fake_completion_alias", "fake_completion_alias", "fake_completion_with_error").contains(function);
        Type resultType = embedding ? new ArrayType(function.contains("double") ? DOUBLE : REAL) : VARCHAR;
        if (function.endsWith("_with_error")) {
            resultType = RowType.from(ImmutableList.of(RowType.field("result", resultType), RowType.field("error", VARCHAR)));
        }
        VariableReferenceExpression output = variables.newVariable("output", resultType);
        PlanNode source = new ValuesNode(Optional.empty(), ids.getNextId(), ImmutableList.of(prompt, rowKey), ImmutableList.of(), Optional.empty());
        RowExpression options = varchar("{\"inference_backend\":\"fake_backend\"" +
                (functionMode == null ? "" : ",\"streaming_mode\":\"" + functionMode + "\"") + "}");
        if (projectedOptions) {
            VariableReferenceExpression alias = variables.newVariable("options_alias", VARCHAR);
            source = new ProjectNode(ids.getNextId(), source,
                    Assignments.builder().put(prompt, prompt).put(rowKey, rowKey).put(alias, options).build());
            options = alias;
        }
        List<RowExpression> arguments = new ArrayList<>(ImmutableList.of(prompt, varchar("model")));
        if (!embedding) {
            arguments.add(varchar(""));
        }
        if (embedding || completion) {
            arguments.add(options);
        }
        for (int index = 0; index < mediaCount; index++) {
            arguments.add(varchar("{\"streaming_mode\":\"not-options\"}"));
        }
        if (!embedding && !completion) {
            arguments.add(options);
        }
        CallExpression call = new CallExpression(function, createFunctionHandle(function), resultType, arguments);
        ProjectNode project = new ProjectNode(ids.getNextId(), source,
                Assignments.builder().put(rowKey, rowKey).put(output, call).build());
        return new RpcFunctionOptimizer(() -> ImmutableSet.of(function), null, policy, nativeSpark, FUNCTION_AND_TYPE_MANAGER, fakeRegistry())
                .optimize(project, session, TypeProvider.empty(), variables, ids, WarningCollector.NOOP, false).getPlanNode();
    }

    static RowType fakeReceiptType()
    {
        return RowType.from(ImmutableList.of(RowType.field("job_key", VARCHAR), RowType.field("input_position", BIGINT)));
    }

    static RowExpression fakeGrouping(VariableReferenceExpression receipt)
    {
        return new SpecialFormExpression(SpecialFormExpression.Form.DEREFERENCE, VARCHAR,
                ImmutableList.of(receipt, new ConstantExpression(0L, INTEGER)));
    }

    static RpcDeferredExecutionRegistry fakeRegistry()
    {
        return new DefaultRpcDeferredExecutionRegistry(call -> {
            String name = call.getDisplayName().toLowerCase(Locale.ENGLISH);
            if (name.startsWith("fake_completion")) {
                return call.getArguments().size() >= 4 ? RpcArgumentLayout.optionsAt(3) : RpcArgumentLayout.noOptions();
            }
            if (name.startsWith("fake_embedding")) {
                return call.getArguments().size() >= 3 ? RpcArgumentLayout.optionsAt(2) : RpcArgumentLayout.noOptions();
            }
            return call.getArguments().size() >= 3 ? RpcArgumentLayout.optionsAt(call.getArguments().size() - 1) : RpcArgumentLayout.noOptions();
        }, ImmutableList.of(context -> {
            CallExpression call = context.getCall();
            if (!call.getDisplayName().startsWith("fake_completion")) {
                return Optional.empty();
            }
            ConstantExpression options = context.getArgumentLayout().getOptionsArgumentIndex().isPresent()
                    ? context.resolveConstant(call.getArguments().get(context.getArgumentLayout().getOptionsArgumentIndex().getAsInt()))
                            .orElseThrow(() -> new IllegalArgumentException("BATCH_DEFERRED fake provider requires constant options"))
                    : varchar("{}");
            if (options.getValue() == null) {
                options = varchar("{}");
            }
            try {
                JsonNode json = new ObjectMapper().readTree(((Slice) options.getValue()).toStringUtf8());
                if (!json.path("inference_backend").asText("fake_backend").equals("fake_backend")) {
                    return Optional.empty();
                }
            }
            catch (IOException invalidOptions) {
                throw new IllegalArgumentException("BATCH_DEFERRED fake provider requires valid options", invalidOptions);
            }
            checkFakeArguments(call);
            ConstantExpression model = context.resolveConstant(call.getArguments().get(1))
                    .orElseThrow(() -> new IllegalArgumentException("BATCH_DEFERRED fake provider requires a constant model"));
            if (model.getValue() == null) {
                throw new IllegalArgumentException("BATCH_DEFERRED fake provider requires a non-null model");
            }
            List<RowExpression> arguments = new ArrayList<>(call.getArguments());
            arguments.set(1, model);
            if (context.getArgumentLayout().getOptionsArgumentIndex().isPresent()) {
                arguments.set(context.getArgumentLayout().getOptionsArgumentIndex().getAsInt(), options);
            }
            ConstantExpression phaseOptions = options;
            int batchSize = context.getRequestedDispatchBatchSize() == 0 ? 64 : Math.min(1024, context.getRequestedDispatchBatchSize());
            boolean withError = call.getDisplayName().endsWith("_with_error");
            return Optional.of(new DeferredRpcExecution()
            {
                @Override
                public DeferredRpcCall submit()
                {
                    return DeferredRpcCall.builder(withError ? "fake_submit_with_error" : "fake_submit", fakeReceiptType())
                            .setArguments(arguments).setDispatchBatchSize(batchSize).build();
                }

                @Override
                public DeferredRpcCall poll(VariableReferenceExpression receipt)
                {
                    return DeferredRpcCall.builder("fake_poll", fakeReceiptType()).setArguments(ImmutableList.of(receipt, phaseOptions))
                            .setDispatchBatchSize(batchSize).setInputGroupingExpressions(ImmutableList.of(fakeGrouping(receipt))).build();
                }

                @Override
                public DeferredRpcCall collect(VariableReferenceExpression ready)
                {
                    return DeferredRpcCall.builder(withError ? "fake_collect_with_error" : "fake_collect", call.getType())
                            .setArguments(ImmutableList.of(ready, phaseOptions)).setDispatchBatchSize(batchSize)
                            .setInputGroupingExpressions(ImmutableList.of(fakeGrouping(ready))).build();
                }
            });
        }));
    }

    private static void checkFakeArguments(CallExpression call)
    {
        if (call.getArguments().size() < 2) {
            throw new IllegalArgumentException("BATCH_DEFERRED fake provider requires two input arguments");
        }
    }

    private static ObjectMapper planNodeObjectMapper()
            throws Exception
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            binder.install(new ThriftCodecModule());
            binder.install(new HandleJsonModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);
            binder.bind(ConnectorManager.class).toProvider(() -> null);
            binder.bind(SqlParser.class).toInstance(new SqlParser());
            binder.bind(TypeManager.class).toInstance(FUNCTION_AND_TYPE_MANAGER);
            newSetBinder(binder, Type.class);
            binder.bind(BlockEncodingSerde.class).toInstance(new TestingBlockEncodingSerde());
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            jsonBinder(binder).addSerializerBinding(Slice.class).to(SliceSerializer.class);
            jsonBinder(binder).addDeserializerBinding(Slice.class).to(SliceDeserializer.class);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            jsonBinder(binder).addSerializerBinding(Expression.class).to(Serialization.ExpressionSerializer.class);
            jsonBinder(binder).addDeserializerBinding(Expression.class).to(Serialization.ExpressionDeserializer.class);
            jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(Serialization.FunctionCallDeserializer.class);
            jsonBinder(binder).addKeySerializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionSerializer.class);
            jsonBinder(binder).addKeyDeserializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionDeserializer.class);
        };
        return new Bootstrap(ImmutableList.of(module)).doNotInitializeLogging().quiet().initialize().getInstance(ObjectMapper.class);
    }
}
