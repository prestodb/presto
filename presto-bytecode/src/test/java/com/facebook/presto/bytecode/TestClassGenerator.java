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
package com.facebook.presto.bytecode;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.StringWriter;
import java.lang.reflect.Method;
import java.nio.file.Path;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.ClassGenerator.classGenerator;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestClassGenerator
{
    @Test
    public void testGenerator()
            throws Exception
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                "test/Example",
                type(Object.class));

        Parameter argA = arg("a", int.class);
        Parameter argB = arg("b", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC, STATIC),
                "add",
                type(int.class),
                ImmutableList.of(argA, argB));

        method.getBody()
                .append(add(argA, argB))
                .retInt();

        Path tempDir = createTempDirectory("test");

        try {
            StringWriter writer = new StringWriter();

            Class<?> clazz = classGenerator(getClass().getClassLoader())
                    .fakeLineNumbers(true)
                    .runAsmVerifier(true)
                    .dumpRawBytecode(true)
                    .outputTo(writer)
                    .dumpClassFilesTo(tempDir)
                    .defineClass(classDefinition, Object.class);

            Method add = clazz.getMethod("add", int.class, int.class);
            assertEquals(add.invoke(null, 13, 42), 55);

            assertThat(writer.toString())
                    .contains("00002 I I  : I I  :     IADD")
                    .contains("public final class test/Example {")
                    .contains("// declaration: int add(int, int)")
                    .contains("LINENUMBER 2002 L1");

            assertThat(tempDir.resolve("test/Example.class")).isRegularFile();
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }
}
