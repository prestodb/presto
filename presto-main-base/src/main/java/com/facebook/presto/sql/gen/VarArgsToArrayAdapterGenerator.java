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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.CallSiteBinder;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class VarArgsToArrayAdapterGenerator
{
    private VarArgsToArrayAdapterGenerator()
    {
    }

    public static class VarArgMethodHandle
    {
        private final MethodHandle methodHandle;

        private VarArgMethodHandle(MethodHandle methodHandle)
        {
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }
    }

    public static VarArgMethodHandle generateVarArgsToArrayAdapter(
            Class<?> returnType,
            Class<?> javaType,
            int argsLength,
            MethodHandle function)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(javaType, "javaType is null");
        requireNonNull(function, "function is null");

        checkCondition(argsLength <= 254, NOT_SUPPORTED, "Too many arguments for vararg function");

        MethodType methodType = function.type();
        Class<?> javaArrayType = toArrayClass(javaType);
        checkArgument(methodType.returnType() == returnType, "returnType does not match");
        checkArgument(methodType.parameterList().equals(ImmutableList.of(javaArrayType)), "parameter types do not match");

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = new ClassDefinition(a(PUBLIC, FINAL), makeClassName("VarArgsToListAdapter"), type(Object.class));
        classDefinition.declareDefaultConstructor(a(PRIVATE));

        // generate adapter method
        ImmutableList.Builder<Parameter> parameterListBuilder = ImmutableList.builder();
        for (int i = 0; i < argsLength; i++) {
            parameterListBuilder.add(arg("input_" + i, javaType));
        }
        ImmutableList<Parameter> parameterList = parameterListBuilder.build();
        MethodDefinition methodDefinition = classDefinition.declareMethod(a(PUBLIC, STATIC), "varArgsToArray", type(returnType), parameterList);
        BytecodeBlock body = methodDefinition.getBody();
        body.append(
                loadConstant(callSiteBinder, function, MethodHandle.class)
                        .invoke(
                                "invokeExact",
                                returnType,
                                BytecodeExpressions.newArray(ParameterizedType.type(javaArrayType), parameterList))
                        .ret());

        // define class
        Class<?> generatedClass = defineClass(classDefinition, Object.class, callSiteBinder.getBindings(), VarArgsToArrayAdapterGenerator.class.getClassLoader());
        return new VarArgMethodHandle(
                Reflection.methodHandle(
                        generatedClass,
                        "varArgsToArray",
                        ImmutableList.builder()
                                .addAll(nCopies(argsLength, javaType))
                                .build()
                                .toArray(new Class<?>[argsLength])));
    }

    private static Class<?> toArrayClass(Class<?> elementType)
    {
        return Array.newInstance(elementType, 0).getClass();
    }
}
