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
package io.prestosql.sql.gen;

import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.util.Reflection;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.gen.BytecodeUtils.loadConstant;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;
import static io.prestosql.util.Failures.checkCondition;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class VarArgsToArrayAdapterGenerator
{
    private VarArgsToArrayAdapterGenerator()
    {
    }

    public static class MethodHandleAndConstructor
    {
        private final MethodHandle methodHandle;
        private final MethodHandle constructor;

        private MethodHandleAndConstructor(MethodHandle methodHandle, MethodHandle constructor)
        {
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.constructor = requireNonNull(constructor, "constructor is null");
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        public MethodHandle getConstructor()
        {
            return constructor;
        }
    }

    @UsedByGeneratedCode
    public static final class VarArgsToArrayAdapterState
    {
        /**
         * User state created by provided user state factory
         */
        public final Object userState;

        /**
         * Array of argument, such as long[], Block[]
         */
        public final Object args;

        public VarArgsToArrayAdapterState(Object userState, Object args)
        {
            this.userState = userState;
            this.args = requireNonNull(args, "args is null");
        }
    }

    public static MethodHandleAndConstructor generateVarArgsToArrayAdapter(
            Class<?> returnType,
            Class<?> javaType,
            int argsLength,
            MethodHandle function,
            MethodHandle userStateFactory)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(javaType, "javaType is null");
        requireNonNull(function, "function is null");
        requireNonNull(userStateFactory, "userStateFactory is null");

        checkCondition(argsLength <= 253, NOT_SUPPORTED, "Too many arguments for vararg function");

        MethodType methodType = function.type();
        Class<?> javaArrayType = toArrayClass(javaType);
        checkArgument(methodType.returnType() == returnType, "returnType does not match");
        checkArgument(methodType.parameterList().equals(ImmutableList.of(Object.class, javaArrayType)), "parameter types do not match");

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = new ClassDefinition(a(PUBLIC, FINAL), makeClassName("VarArgsToListAdapter"), type(Object.class));
        classDefinition.declareDefaultConstructor(a(PRIVATE));

        // generate userState constructor
        MethodDefinition stateFactoryDefinition = classDefinition.declareMethod(a(PUBLIC, STATIC), "createState", type(VarArgsToArrayAdapterState.class));

        stateFactoryDefinition.getBody()
                .comment("create userState for current instance")
                .append(
                        newInstance(
                                VarArgsToArrayAdapterState.class,
                                loadConstant(callSiteBinder, userStateFactory, MethodHandle.class).invoke("invokeExact", Object.class),
                                newArray(type(javaArrayType), argsLength).cast(Object.class)).ret());

        // generate adapter method
        ImmutableList.Builder<Parameter> parameterListBuilder = ImmutableList.builder();
        parameterListBuilder.add(arg("userState", VarArgsToArrayAdapterState.class));
        for (int i = 0; i < argsLength; i++) {
            parameterListBuilder.add(arg("input_" + i, javaType));
        }
        ImmutableList<Parameter> parameterList = parameterListBuilder.build();

        MethodDefinition methodDefinition = classDefinition.declareMethod(a(PUBLIC, STATIC), "varArgsToArray", type(returnType), parameterList);
        BytecodeBlock body = methodDefinition.getBody();

        BytecodeExpression userState = parameterList.get(0).getField("userState", Object.class);
        BytecodeExpression args = parameterList.get(0).getField("args", Object.class).cast(javaArrayType);
        for (int i = 0; i < argsLength; i++) {
            body.append(args.setElement(i, parameterList.get(i + 1)));
        }

        body.append(
                loadConstant(callSiteBinder, function, MethodHandle.class)
                        .invoke("invokeExact", returnType, userState, args)
                        .ret());

        // define class
        Class<?> generatedClass = defineClass(classDefinition, Object.class, callSiteBinder.getBindings(), VarArgsToArrayAdapterGenerator.class.getClassLoader());
        return new MethodHandleAndConstructor(
                Reflection.methodHandle(
                        generatedClass,
                        "varArgsToArray",
                        ImmutableList.builder()
                                .add(VarArgsToArrayAdapterState.class)
                                .addAll(nCopies(argsLength, javaType))
                                .build()
                                .toArray(new Class<?>[argsLength])),
                Reflection.methodHandle(generatedClass, "createState"));
    }

    private static Class<?> toArrayClass(Class<?> elementType)
    {
        return Array.newInstance(elementType, 0).getClass();
    }
}
