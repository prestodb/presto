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
package com.facebook.presto.hive.functions.scalar;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.functions.HiveFunction;
import com.facebook.presto.hive.functions.gen.ScalarMethodHandles;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.InvocationConvention;
import com.facebook.presto.spi.function.JavaScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.hive.functions.scalar.HiveScalarFunctionInvoker.createFunctionInvoker;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.PUBLIC;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HiveScalarFunction
        extends HiveFunction
{
    private final JavaScalarFunctionImplementation implementation;
    private final FunctionMetadata functionMetadata;

    private HiveScalarFunction(FunctionMetadata metadata,
                               Signature signature,
                               String description,
                               JavaScalarFunctionImplementation implementation)
    {
        super(metadata.getName(),
                signature,
                false,
                metadata.isDeterministic(),
                metadata.isCalledOnNullInput(),
                description);

        this.functionMetadata = requireNonNull(metadata, "metadata is null");
        this.implementation = requireNonNull(implementation, "implementation is null");
    }

    public static HiveScalarFunction createHiveScalarFunction(Class<?> cls, QualifiedObjectName name, List<TypeSignature> argumentTypes, TypeManager typeManager)
    {
        HiveScalarFunctionInvoker invoker = createFunctionInvoker(cls, name, argumentTypes, typeManager);
        MethodHandle methodHandle = ScalarMethodHandles.generateUnbound(invoker.getSignature(), typeManager).bindTo(invoker);
        Signature signature = invoker.getSignature();
        FunctionMetadata functionMetadata = new FunctionMetadata(name,
                signature.getArgumentTypes(),
                signature.getReturnType(),
                SCALAR,
                FunctionImplementationType.JAVA,
                true,
                true);
        InvocationConvention invocationConvention = new InvocationConvention(
                signature.getArgumentTypes().stream().map(t -> BOXED_NULLABLE).collect(toImmutableList()),
                InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN,
                false);
        JavaScalarFunctionImplementation implementation = new HiveScalarFunctionImplementation(methodHandle, invocationConvention);
        return new HiveScalarFunction(functionMetadata, signature, name.getObjectName(), implementation);
    }

    public FunctionMetadata getFunctionMetadata()
    {
        return functionMetadata;
    }

    public JavaScalarFunctionImplementation getJavaScalarFunctionImplementation()
    {
        return implementation;
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return PUBLIC;
    }

    private static class HiveScalarFunctionImplementation
            implements JavaScalarFunctionImplementation
    {
        private final MethodHandle methodHandle;
        private final InvocationConvention invocationConvention;

        private HiveScalarFunctionImplementation(MethodHandle methodHandle, InvocationConvention invocationConvention)
        {
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.invocationConvention = requireNonNull(invocationConvention, "invocationConvention is null");
        }

        public InvocationConvention getInvocationConvention()
        {
            return invocationConvention;
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }
    }
}
