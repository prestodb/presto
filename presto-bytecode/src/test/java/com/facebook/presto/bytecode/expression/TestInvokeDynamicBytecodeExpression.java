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
package com.facebook.presto.bytecode.expression;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;

import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeExpression;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeDynamic;

public class TestInvokeDynamicBytecodeExpression
{
    @Test
    public void testInvokeStaticMethod()
            throws Exception
    {
        assertBytecodeExpression(
                invokeDynamic(TEST_BOOTSTRAP_METHOD, ImmutableList.of("bar"), "foo", String.class, constantString("baz")),
                "foo-bar-baz",
                "[bootstrap(\"bar\")]=>foo(\"baz\")");
    }

    public static final Method TEST_BOOTSTRAP_METHOD;

    static {
        try {
            TEST_BOOTSTRAP_METHOD = TestInvokeDynamicBytecodeExpression.class.getMethod("bootstrap", MethodHandles.Lookup.class, String.class, MethodType.class, String.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static CallSite bootstrap(MethodHandles.Lookup callerLookup, String name, MethodType type, String prefix)
            throws Exception
    {
        MethodHandle methodHandle = callerLookup.findVirtual(String.class, "concat", MethodType.methodType(String.class, String.class));
        methodHandle = methodHandle.bindTo(name + "-" + prefix + "-");
        return new ConstantCallSite(methodHandle);
    }
}
