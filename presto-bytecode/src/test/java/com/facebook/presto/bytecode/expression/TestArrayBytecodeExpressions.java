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

import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeExpression;
import static com.facebook.presto.bytecode.expression.BytecodeExpressionAssertions.assertBytecodeExpressionType;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantDouble;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFloat;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;

public class TestArrayBytecodeExpressions
{
    private final DynamicClassLoader classLoader = new DynamicClassLoader(TestArrayBytecodeExpressions.class.getClassLoader());
    private static final ClassDefinition classDefinition = new ClassDefinition(a(PUBLIC, FINAL), "DummyClass", type(Object.class));
    private final Map<Class<?>, MethodDefinition> typeMethodMap = new HashMap<>();

    @BeforeClass
    public void setUp()
            throws Exception
    {
        for (Class<?> aClass : ImmutableList.of(boolean[].class, char[].class, float[].class, double[].class, byte[].class, short[].class, int[].class, long[].class, String[].class)) {
            MethodDefinition methodDefinition = defineSetAndGetMethod(aClass);
            typeMethodMap.put(aClass, methodDefinition);
        }
        defineClass(classDefinition, Object.class, classLoader);
    }

    @Test
    public void testNewArray()
            throws Exception
    {
        assertBytecodeExpressionType(newArray(type(boolean[].class), 5), type(boolean[].class));
        assertBytecodeExpression(newArray(type(boolean[].class), 5).length(), 5, "new boolean[5].length");

        assertBytecodeExpressionType(newArray(type(char[].class), 5), type(char[].class));
        assertBytecodeExpression(newArray(type(char[].class), 5).length(), 5, "new char[5].length");

        assertBytecodeExpressionType(newArray(type(float[].class), 5), type(float[].class));
        assertBytecodeExpression(newArray(type(float[].class), 5).length(), 5, "new float[5].length");

        assertBytecodeExpressionType(newArray(type(double[].class), 5), type(double[].class));
        assertBytecodeExpression(newArray(type(double[].class), 5).length(), 5, "new double[5].length");

        assertBytecodeExpressionType(newArray(type(byte[].class), 5), type(byte[].class));
        assertBytecodeExpression(newArray(type(byte[].class), 5).length(), 5, "new byte[5].length");

        assertBytecodeExpressionType(newArray(type(short[].class), 5), type(short[].class));
        assertBytecodeExpression(newArray(type(short[].class), 5).length(), 5, "new short[5].length");

        assertBytecodeExpressionType(newArray(type(int[].class), 5), type(int[].class));
        assertBytecodeExpression(newArray(type(int[].class), 5).length(), 5, "new int[5].length");

        assertBytecodeExpressionType(newArray(type(long[].class), 5), type(long[].class));
        assertBytecodeExpression(newArray(type(long[].class), 5).length(), 5, "new long[5].length");

        assertBytecodeExpressionType(constantString("foo bar baz").invoke("split", String[].class, constantString(" ")), type(String[].class));
        assertBytecodeExpression(constantString("foo bar baz").invoke("split", String[].class, constantString(" ")).length(), 3, "\"foo bar baz\".split(\" \").length");
    }

    @Test
    public void testNewArrayPrefilled()
            throws Exception
    {
        assertBytecodeExpressionType(newArray(type(boolean[].class), ImmutableList.of(constantTrue(), constantFalse(), constantTrue())), type(boolean[].class));
        assertBytecodeExpression(
                newArray(type(boolean[].class), ImmutableList.of(constantTrue(), constantFalse(), constantTrue())),
                new boolean[] {true, false, true},
                "new boolean[] {true, false, true}");

        assertBytecodeExpressionType(newArray(type(int[].class), ImmutableList.of(constantInt(65), constantInt(66), constantInt(99))), type(int[].class));
        assertBytecodeExpression(
                newArray(type(int[].class), ImmutableList.of(constantInt(65), constantInt(66), constantInt(99))),
                new int[] {65, 66, 99},
                "new int[] {65, 66, 99}");

        assertBytecodeExpressionType(newArray(type(long[].class), ImmutableList.of(constantLong(1234L), constantLong(12345L), constantLong(9876543210L))), type(long[].class));
        assertBytecodeExpression(
                newArray(type(long[].class), ImmutableList.of(constantLong(1234L), constantLong(12345L), constantLong(9876543210L))),
                new long[] {1234L, 12345L, 9876543210L},
                "new long[] {1234L, 12345L, 9876543210L}");

        assertBytecodeExpressionType(newArray(type(String[].class), ImmutableList.of(constantString("presto"), constantNull(String.class), constantString("new"), constantString("array"))), type(String[].class));
        assertBytecodeExpression(
                newArray(type(String[].class), ImmutableList.of(constantString("presto"), constantNull(String.class), constantString("new"), constantString("array"))),
                new String[] {"presto", null, "new", "array"},
                "new String[] {\"presto\", null, \"new\", \"array\"}");
    }

    @Test
    public void testSetElement()
            throws Exception
    {
        BytecodeExpression stringArray = constantString("foo bar baz").invoke("split", String[].class, constantString(" "));

        assertBytecodeExpressionType(stringArray, type(String[].class));
        assertBytecodeExpression(stringArray.length(), 3, "\"foo bar baz\".split(\" \").length");
        assertBytecodeExpression(stringArray.getElement(0), "foo", "\"foo bar baz\".split(\" \")[0]");
        assertBytecodeExpression(stringArray.getElement(1), "bar", "\"foo bar baz\".split(\" \")[1]");
        assertBytecodeExpression(stringArray.getElement(2), "baz", "\"foo bar baz\".split(\" \")[2]");

        assertBytecodeExpression(invokeStatic(typeMethodMap.get(boolean[].class), newArray(type(boolean[].class), 5), constantInt(0), constantTrue()), true, classLoader);
        assertBytecodeExpression(invokeStatic(typeMethodMap.get(int[].class), newArray(type(int[].class), 5), constantInt(0), constantInt(999)), 999, classLoader);
        assertBytecodeExpression(invokeStatic(typeMethodMap.get(float[].class), newArray(type(float[].class), 5), constantInt(0), constantFloat(0.777f)), 0.777f, classLoader);
        assertBytecodeExpression(invokeStatic(typeMethodMap.get(double[].class), newArray(type(double[].class), 5), constantInt(0), constantDouble(0.888d)), 0.888d, classLoader);
        assertBytecodeExpression(invokeStatic(typeMethodMap.get(String[].class), stringArray, constantInt(0), constantString("hello")), "hello", classLoader);
    }

    @Test
    public void testGetElement()
            throws Exception
    {
        assertBytecodeExpression(constantString("abc").invoke("getBytes", byte[].class).getElement(1), "abc".getBytes()[1], "\"abc\".getBytes()[1]");
        assertBytecodeExpression(constantString("abc").invoke("getBytes", byte[].class).getElement(constantInt(1)), "abc".getBytes()[1], "\"abc\".getBytes()[1]");
    }

    private static MethodDefinition defineSetAndGetMethod(Class<?> aClass)
    {
        Parameter arr = arg("arr", type(aClass));
        Parameter index = arg("index", type(int.class));
        Class<?> componentType = aClass.getComponentType();
        Parameter value = arg("value", type(componentType));
        MethodDefinition methodDefinition = classDefinition.declareMethod(a(PUBLIC, STATIC), "setAndGetMethod_" + componentType.getSimpleName(), type(componentType), arr, index, value);
        methodDefinition.getBody()
                .append(arr.setElement(index, value))
                .append(arr.getElement(index).ret());
        return methodDefinition;
    }
}
