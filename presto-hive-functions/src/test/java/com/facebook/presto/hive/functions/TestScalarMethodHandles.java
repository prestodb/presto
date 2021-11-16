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
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.functions.scalar.ScalarFunctionInvoker;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.hive.functions.gen.ScalarMethodHandles.generateUnbound;
import static org.testng.Assert.assertEquals;

public class TestScalarMethodHandles
{
    private final AtomicInteger number = new AtomicInteger(0);
    private final TestingTypeManager typeManager = new TestingTypeManager();

    @Test
    public void generateUnboundTest()
            throws Throwable
    {
        runCase("boolean", tokens("boolean"), true, false);
        runCase("bigint", tokens("bigint,bigint"), 3L, 2L, 1L);
        runCase("varchar", tokens("varchar,double"), Slices.utf8Slice("output"), Slices.utf8Slice("input"), 2.1);
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
}
