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
package com.facebook.presto.hive.functions;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.MapBlock;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.functions.scalar.ScalarFunctionInvoker;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.google.inject.Key;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.hive.functions.HiveFunctionsTestUtils.createTestingPrestoServer;
import static com.facebook.presto.hive.functions.gen.ScalarMethodHandles.generateUnbound;
import static org.testng.Assert.assertEquals;

public class TestScalarMethodHandles
{
    private AtomicInteger number;
    private TestingPrestoServer server;
    private TypeManager typeManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.number = new AtomicInteger(0);
        this.server = createTestingPrestoServer();
        this.typeManager = server.getInstance(Key.get(TypeManager.class));
    }

    @Test
    public void generateUnboundTest()
            throws Throwable
    {
        runCase("boolean", tokens("boolean"), true, false);
        runCase("bigint", tokens("bigint,bigint"), 3L, 2L, 1L);
        runCase("varchar", tokens("varchar,double"), Slices.utf8Slice("output"), Slices.utf8Slice("input"), 2.1);
        runCase("array(bigint)", tokens("bigint,bigint"), createArrayBlock(), 2L, 1L);
        runCase("map(varchar,varchar)", tokens("varchar,varchar"), createMapBlock(), Slices.utf8Slice("a"), Slices.utf8Slice("b"));
    }

    private void runCase(String resultType, String[] argumentTypes, Object result, Object... arguments)
            throws Throwable
    {
        Signature signature = createScalarSignature(resultType, argumentTypes);
        MethodHandle methodHandle = generateUnbound(signature, typeManager);
        TestingScalarFunctionInvoker invoker = new TestingScalarFunctionInvoker(signature, result);
        final Object output;
        if (arguments.length == 1) {
            output = methodHandle.invoke(invoker, arguments[0]);
        }
        else if (arguments.length == 2) {
            output = methodHandle.invoke(invoker, arguments[0], arguments[1]);
        }
        else if (arguments.length == 3) {
            output = methodHandle.invoke(invoker, arguments[0], arguments[1], arguments[2]);
        }
        else if (arguments.length == 4) {
            output = methodHandle.invoke(invoker, arguments[0], arguments[1], arguments[2], arguments[3]);
        }
        else {
            throw new RuntimeException("Not supported yet");
        }
        Object[] inputs = invoker.getInputs();

        assertEquals(output, result);
        assertEquals(inputs, arguments);
    }

    private String[] tokens(String s)
    {
        return s.split(",");
    }

    private Signature createScalarSignature(String returnType, String... argumentTypes)
    {
        return new Signature(QualifiedObjectName.valueOf("hive.default.testing_" + number.incrementAndGet()),
                FunctionKind.SCALAR,
                parseTypeSignature(returnType),
                Stream.of(argumentTypes)
                        .map(TypeSignature::parseTypeSignature)
                        .toArray(TypeSignature[]::new));
    }

    private static class TestingScalarFunctionInvoker
            implements ScalarFunctionInvoker
    {
        private final Signature signature;
        private final Object result;
        private Object[] inputs;

        public TestingScalarFunctionInvoker(Signature signature, Object result)
        {
            this.signature = signature;
            this.result = result;
        }

        @Override
        public Signature getSignature()
        {
            return signature;
        }

        @Override
        public Object evaluate(Object... inputs)
        {
            this.inputs = inputs;
            return result;
        }

        public Object[] getInputs()
        {
            return inputs;
        }
    }

    private Block createEmptyBlock()
    {
        return new ByteArrayBlock(0, Optional.empty(), new byte[0]);
    }

    private Block createArrayBlock()
    {
        Block emptyValueBlock = createEmptyBlock();
        return ArrayBlock.fromElementBlock(1, Optional.empty(), IntStream.range(0, 2).toArray(), emptyValueBlock);
    }

    private Block createMapBlock()
    {
        Block emptyKeyBlock = createEmptyBlock();
        Block emptyValueBlock = createEmptyBlock();
        return MapBlock.fromKeyValueBlock(1, Optional.empty(), IntStream.range(0, 2).toArray(), emptyKeyBlock, emptyValueBlock);
    }
}
