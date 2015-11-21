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

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.ClassInfoLoader;
import com.facebook.presto.byteCode.DumpByteCodeVisitor;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.ParameterizedType;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.SmartClassWriter;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.ClassWriter;

import java.util.function.Function;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.sql.gen.CompilerUtils.makeClassName;
import static org.testng.Assert.assertEquals;

public final class ByteCodeExpressionAssertions
{
    private ByteCodeExpressionAssertions()
    {
    }

    private static final boolean DUMP_BYTE_CODE_TREE = false;

    public static void assertByteCodeExpression(ByteCodeExpression expression, Object expected, String expectedRendering)
            throws Exception
    {
        assertEquals(expression.toString(), expectedRendering);

        assertByteCodeNode(expression.ret(), expression.getType(), expected);
    }

    public static void assertByteCodeNode(ByteCodeNode node, ParameterizedType returnType, Object expected)
            throws Exception
    {
        assertEquals(execute(context -> node, returnType), expected);
    }

    public static void assertByteCodeNode(Function<Scope, ByteCodeNode> nodeGenerator, ParameterizedType returnType, Object expected)
            throws Exception
    {
        assertEquals(execute(nodeGenerator, returnType), expected);
    }

    public static Object execute(Function<Scope, ByteCodeNode> nodeGenerator, ParameterizedType returnType)
            throws Exception
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("Test"),
                type(Object.class));

        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC, STATIC), "test", returnType);
        ByteCodeNode node = nodeGenerator.apply(method.getScope());
        method.getBody().append(node);

        if (DUMP_BYTE_CODE_TREE) {
            DumpByteCodeVisitor dumpByteCode = new DumpByteCodeVisitor(System.out);
            dumpByteCode.visitClass(classDefinition);
        }

        DynamicClassLoader classLoader = new DynamicClassLoader();
        ClassWriter cw = new SmartClassWriter(ClassInfoLoader.createClassInfoLoader(ImmutableList.of(classDefinition), classLoader));
        classDefinition.visit(cw);

        Class<?> clazz = classLoader.defineClass(classDefinition.getType().getJavaClassName(), cw.toByteArray());
        return clazz.getMethod("test").invoke(null);
    }
}
