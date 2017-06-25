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
package com.facebook.presto.operator.scalar;

import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public final class ScalarFunctionImplementation
{
    private final boolean nullable;
    private final List<Boolean> nullableArguments;
    private final List<Boolean> nullFlags;
    private final List<Optional<Class>> lambdaInterface;
    private final MethodHandle methodHandle;
    private final Optional<MethodHandle> instanceFactory;
    private final boolean deterministic;

    public ScalarFunctionImplementation(boolean nullable, List<Boolean> nullableArguments, MethodHandle methodHandle, boolean deterministic)
    {
        this(
                nullable,
                nullableArguments,
                nCopies(nullableArguments.size(), false),
                nCopies(nullableArguments.size(), Optional.empty()),
                methodHandle,
                Optional.empty(),
                deterministic);
    }

    public ScalarFunctionImplementation(boolean nullable, List<Boolean> nullableArguments, List<Boolean> nullFlags, MethodHandle methodHandle, boolean deterministic)
    {
        this(
                nullable,
                nullableArguments,
                nullFlags,
                nCopies(nullableArguments.size(), Optional.empty()),
                methodHandle,
                Optional.empty(),
                deterministic);
    }

    public ScalarFunctionImplementation(
            boolean nullable,
            List<Boolean> nullableArguments,
            List<Boolean> nullFlags,
            List<Optional<Class>> lambdaInterface,
            MethodHandle methodHandle,
            boolean deterministic)
    {
        this(
                nullable,
                nullableArguments,
                nullFlags,
                lambdaInterface,
                methodHandle,
                Optional.empty(),
                deterministic);
    }

    public ScalarFunctionImplementation(
            boolean nullable,
            List<Boolean> nullableArguments,
            List<Boolean> nullFlags,
            List<Optional<Class>> lambdaInterface,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory,
            boolean deterministic)
    {
        this.nullable = nullable;
        this.nullableArguments = ImmutableList.copyOf(requireNonNull(nullableArguments, "nullableArguments is null"));
        this.nullFlags = ImmutableList.copyOf(requireNonNull(nullFlags, "nullFlags is null"));
        this.lambdaInterface = ImmutableList.copyOf(requireNonNull(lambdaInterface, "lambdaInterface is null"));
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        this.instanceFactory = requireNonNull(instanceFactory, "instanceFactory is null");
        this.deterministic = deterministic;

        if (instanceFactory.isPresent()) {
            Class<?> instanceType = instanceFactory.get().type().returnType();
            checkArgument(instanceType.equals(methodHandle.type().parameterType(0)), "methodHandle is not an instance method");
        }

        checkCondition(nullFlags.size() == nullableArguments.size(), FUNCTION_IMPLEMENTATION_ERROR, "size of nullFlags is not equal to size of nullableArguments: %s", methodHandle);
        checkCondition(nullFlags.size() == lambdaInterface.size(), FUNCTION_IMPLEMENTATION_ERROR, "size of nullFlags is not equal to size of lambdaInterface: %s", methodHandle);
        // check if
        // - nullableArguments and nullFlags match
        // - lambda interface is not nullable
        // - lambda interface is annotated with FunctionalInterface
        for (int i = 0; i < nullFlags.size(); i++) {
            if (nullFlags.get(i)) {
                checkCondition(nullableArguments.get(i), FUNCTION_IMPLEMENTATION_ERROR, "argument %s marked as @IsNull is not nullable in method: %s", i, methodHandle);
            }
            if (lambdaInterface.get(i).isPresent()) {
                checkCondition(!nullableArguments.get(i), FUNCTION_IMPLEMENTATION_ERROR, "argument %s marked as lambda is nullable in method: %s", i, methodHandle);
                checkCondition(lambdaInterface.get(i).get().isAnnotationPresent(FunctionalInterface.class), FUNCTION_IMPLEMENTATION_ERROR, "argument %s is marked as lambda but the function interface class is not annotated: %s", i, methodHandle);
            }
        }
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public List<Boolean> getNullableArguments()
    {
        return nullableArguments;
    }

    public List<Boolean> getNullFlags()
    {
        return nullFlags;
    }

    public List<Optional<Class>> getLambdaInterface()
    {
        return lambdaInterface;
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }

    public Optional<MethodHandle> getInstanceFactory()
    {
        return instanceFactory;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }
}
