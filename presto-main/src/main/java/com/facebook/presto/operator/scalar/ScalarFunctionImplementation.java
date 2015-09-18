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

import static java.util.Objects.requireNonNull;

public final class ScalarFunctionImplementation
{
    private final boolean nullable;
    private final List<Boolean> nullableArguments;
    private final MethodHandle methodHandle;
    private final boolean deterministic;

    public ScalarFunctionImplementation(boolean nullable, List<Boolean> nullableArguments, MethodHandle methodHandle, boolean deterministic)
    {
        this.nullable = nullable;
        this.nullableArguments = ImmutableList.copyOf(requireNonNull(nullableArguments, "nullableArguments is null"));
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        this.deterministic = deterministic;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public List<Boolean> getNullableArguments()
    {
        return nullableArguments;
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }
}
