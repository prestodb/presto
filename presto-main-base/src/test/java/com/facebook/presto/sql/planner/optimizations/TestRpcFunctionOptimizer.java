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
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.RPCNode;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

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
                    if (ctor.getParameterCount() == 4) {
                        ctor.setAccessible(true);
                        return ctor.newInstance(
                                session,
                                new com.facebook.presto.spi.plan.PlanNodeIdAllocator(),
                                new com.facebook.presto.spi.VariableAllocator(),
                                com.google.common.collect.ImmutableSet.of());
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
}
