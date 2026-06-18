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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.plan.Assignments;
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
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestRpcFunctionOptimizer
{
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

    private static FunctionHandle createFunctionHandle(String name)
    {
        return new FunctionHandle()
        {
            @Override
            public com.facebook.presto.common.CatalogSchemaName getCatalogSchemaName()
            {
                return new com.facebook.presto.common.CatalogSchemaName("presto", "default");
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
            public List<com.facebook.presto.common.type.TypeSignature> getArgumentTypes()
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
                for (java.lang.reflect.Constructor<?> ctor : inner.getDeclaredConstructors()) {
                    if (ctor.getParameterCount() == 6) {
                        ctor.setAccessible(true);
                        return ctor.newInstance(
                                session,
                                new com.facebook.presto.spi.plan.PlanNodeIdAllocator(),
                                new com.facebook.presto.spi.VariableAllocator(),
                                com.google.common.collect.ImmutableSet.of(),
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
                WarningCollector.NOOP);

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
                WarningCollector.NOOP);

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
                WarningCollector.NOOP);

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
                projectNode, session, TypeProvider.empty(), variableAllocator, idAllocator, WarningCollector.NOOP);

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
                projectNode, session, TypeProvider.empty(), variableAllocator, idAllocator, WarningCollector.NOOP);
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
                projectNode, session, TypeProvider.empty(), variableAllocator, idAllocator, WarningCollector.NOOP);

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
    }

    @Test
    public void testOptimizerDelegatesToExecutionPolicy()
    {
        // The optimizer applies whatever the injected policy returns to the produced RPCNode.
        RpcExecutionPolicy toBatch = intent -> RpcExecutionProperties.of(RPCNode.StreamingMode.BATCH);
        RpcExecutionPolicy toPerRow = intent -> RpcExecutionProperties.of(RPCNode.StreamingMode.PER_ROW);
        assertEquals(optimizeWithPolicy(streamingModeSession("AUTOMATIC"), toBatch), RPCNode.StreamingMode.BATCH);
        assertEquals(optimizeWithPolicy(streamingModeSession("AUTOMATIC"), toPerRow), RPCNode.StreamingMode.PER_ROW);
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

    @Test
    public void testRpcNodeConstructorRejectsAutomatic()
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
                    RPCNode.StreamingMode.AUTOMATIC,
                    0);
            fail("expected rejection when constructing RPCNode with AUTOMATIC");
        }
        catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("AUTOMATIC"), "message should mention AUTOMATIC: " + expected.getMessage());
        }
    }

    @Test
    public void testRpcNodeJsonCreatorRejectsAutomatic()
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
                    RPCNode.StreamingMode.AUTOMATIC,
                    Integer.valueOf(0));
            fail("expected rejection when deserializing an RPCNode with AUTOMATIC");
        }
        catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("AUTOMATIC"), "message should mention AUTOMATIC: " + expected.getMessage());
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
}
