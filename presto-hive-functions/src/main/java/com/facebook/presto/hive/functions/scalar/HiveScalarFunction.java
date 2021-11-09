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
import com.facebook.presto.spi.function.ExtendHiveBuiltInScalarFunctionImplementation;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.spi.function.FunctionKind.SCALAR;

public class HiveScalarFunction
        extends HiveFunction
{
    private final ScalarFunctionImplementation implementation;
    private final FunctionMetadata functionMetadata;

    public static HiveScalarFunction create(Class<?> cls, QualifiedObjectName name, List<TypeSignature> argumentTypes, TypeManager typeManager)
    {
        HiveScalarFunctionInvoker invoker = HiveScalarFunctionInvoker.create(cls, name, argumentTypes, typeManager);
        MethodHandle methodHandle = ScalarMethodHandles.generateUnbound(invoker.getSignature(), typeManager).bindTo(invoker);
        Signature signature = invoker.getSignature();
        FunctionMetadata functionMetadata = new FunctionMetadata(name,
                signature.getArgumentTypes(),
                signature.getReturnType(),
                SCALAR,
                FunctionImplementationType.BUILTIN,
                true,
                true);
        ScalarFunctionImplementation implementation = new HiveScalarFunctionImplementationShim(functionMetadata, signature, methodHandle);
        return new HiveScalarFunction(functionMetadata, signature, name.getObjectName(), implementation);
    }

    private HiveScalarFunction(FunctionMetadata metadata,
                               Signature signature,
                               String description,
                               ScalarFunctionImplementation implementation)
    {
        super(metadata.getName(),
                signature,
                false,
                metadata.isDeterministic(),
                metadata.isCalledOnNullInput(),
                description);

        this.functionMetadata = metadata;
        this.implementation = implementation;
    }

    public FunctionMetadata getFunctionMetadata()
    {
        return functionMetadata;
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation()
    {
        return implementation;
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return null;
    }

    private static class HiveScalarFunctionImplementationShim
            implements ExtendHiveBuiltInScalarFunctionImplementation
    {
        private final FunctionMetadata metadata;
        private final Signature signature;
        private final MethodHandle methodHandle;

        private HiveScalarFunctionImplementationShim(FunctionMetadata metadata, Signature signature, MethodHandle methodHandle)
        {
            this.metadata = metadata;
            this.signature = signature;
            this.methodHandle = methodHandle;
        }

        public FunctionMetadata getFunctionMetadata()
        {
            return metadata;
        }

        public Signature getSignature()
        {
            return signature;
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }
    }
}
