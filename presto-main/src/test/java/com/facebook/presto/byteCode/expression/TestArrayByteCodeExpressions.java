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
package com.facebook.presto.byteCode.expression;

import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeExpression;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressionAssertions.assertByteCodeExpressionType;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantDouble;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantFloat;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantString;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantTrue;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.invokeStatic;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.newArray;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;

public class TestArrayByteCodeExpressions
{
    private final DynamicClassLoader classLoader = new DynamicClassLoader(TestArrayByteCodeExpressions.class.getClassLoader());
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
        assertByteCodeExpressionType(newArray(type(boolean[].class), 5), type(boolean[].class));
        assertByteCodeExpression(newArray(type(boolean[].class), 5).length(), 5, "new boolean[5].length");

        assertByteCodeExpressionType(newArray(type(char[].class), 5), type(char[].class));
        assertByteCodeExpression(newArray(type(char[].class), 5).length(), 5, "new char[5].length");

        assertByteCodeExpressionType(newArray(type(float[].class), 5), type(float[].class));
        assertByteCodeExpression(newArray(type(float[].class), 5).length(), 5, "new float[5].length");

        assertByteCodeExpressionType(newArray(type(double[].class), 5), type(double[].class));
        assertByteCodeExpression(newArray(type(double[].class), 5).length(), 5, "new double[5].length");

        assertByteCodeExpressionType(newArray(type(byte[].class), 5), type(byte[].class));
        assertByteCodeExpression(newArray(type(byte[].class), 5).length(), 5, "new byte[5].length");

        assertByteCodeExpressionType(newArray(type(short[].class), 5), type(short[].class));
        assertByteCodeExpression(newArray(type(short[].class), 5).length(), 5, "new short[5].length");

        assertByteCodeExpressionType(newArray(type(int[].class), 5), type(int[].class));
        assertByteCodeExpression(newArray(type(int[].class), 5).length(), 5, "new int[5].length");

        assertByteCodeExpressionType(newArray(type(long[].class), 5), type(long[].class));
        assertByteCodeExpression(newArray(type(long[].class), 5).length(), 5, "new long[5].length");

        assertByteCodeExpressionType(constantString("foo bar baz").invoke("split", String[].class, constantString(" ")), type(String[].class));
        assertByteCodeExpression(constantString("foo bar baz").invoke("split", String[].class, constantString(" ")).length(), 3, "\"foo bar baz\".split(\" \").length");
    }

    @Test
    public void testSetElement()
            throws Exception
    {
        ByteCodeExpression stringArray = constantString("foo bar baz").invoke("split", String[].class, constantString(" "));

        assertByteCodeExpressionType(stringArray, type(String[].class));
        assertByteCodeExpression(stringArray.length(), 3, "\"foo bar baz\".split(\" \").length");
        assertByteCodeExpression(stringArray.getElement(0), "foo", "\"foo bar baz\".split(\" \")[0]");
        assertByteCodeExpression(stringArray.getElement(1), "bar", "\"foo bar baz\".split(\" \")[1]");
        assertByteCodeExpression(stringArray.getElement(2), "baz", "\"foo bar baz\".split(\" \")[2]");

        assertByteCodeExpression(invokeStatic(typeMethodMap.get(boolean[].class), newArray(type(boolean[].class), 5), constantInt(0), constantTrue()), true, classLoader);
        assertByteCodeExpression(invokeStatic(typeMethodMap.get(int[].class), newArray(type(int[].class), 5), constantInt(0), constantInt(999)), 999, classLoader);
        assertByteCodeExpression(invokeStatic(typeMethodMap.get(float[].class), newArray(type(float[].class), 5), constantInt(0), constantFloat(0.777f)), 0.777f, classLoader);
        assertByteCodeExpression(invokeStatic(typeMethodMap.get(double[].class), newArray(type(double[].class), 5), constantInt(0), constantDouble(0.888d)), 0.888d, classLoader);
        assertByteCodeExpression(invokeStatic(typeMethodMap.get(String[].class), stringArray, constantInt(0), constantString("hello")), "hello", classLoader);
    }

    @Test
    public void testGetElement()
            throws Exception
    {
        assertByteCodeExpression(constantString("abc").invoke("getBytes", byte[].class).getElement(1), "abc".getBytes()[1], "\"abc\".getBytes()[1]");
        assertByteCodeExpression(constantString("abc").invoke("getBytes", byte[].class).getElement(constantInt(1)), "abc".getBytes()[1], "\"abc\".getBytes()[1]");
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
