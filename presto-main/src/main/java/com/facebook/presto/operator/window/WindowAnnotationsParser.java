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
package com.facebook.presto.operator.window;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TypeVariableConstraint;
import com.facebook.presto.spi.function.WindowFunction;
import com.facebook.presto.spi.function.WindowFunctionSignature;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Stream;

import static com.facebook.presto.metadata.FunctionKind.WINDOW;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class WindowAnnotationsParser
{
    private WindowAnnotationsParser() {}

    public static List<SqlWindowFunction> parseFunctionDefinition(Class<? extends WindowFunction> clazz)
    {
        WindowFunctionSignature[] signatures = clazz.getAnnotationsByType(WindowFunctionSignature.class);
        checkArgument(signatures.length > 0, "Class is not annotated with @WindowFunctionSignature: %s", clazz.getName());
        return Stream.of(signatures)
                .map(signature -> parse(clazz, signature))
                .collect(toImmutableList());
    }

    private static SqlWindowFunction parse(Class<? extends WindowFunction> clazz, WindowFunctionSignature window)
    {
        List<TypeVariableConstraint> typeVariables = ImmutableList.of();
        if (!window.typeVariable().isEmpty()) {
            typeVariables = ImmutableList.of(typeVariable(window.typeVariable()));
        }

        List<TypeSignature> argumentTypes = Stream.of(window.argumentTypes())
                .map(TypeSignature::parseTypeSignature)
                .collect(toImmutableList());

        Signature signature = new Signature(
                window.name(),
                WINDOW,
                typeVariables,
                ImmutableList.of(),
                parseTypeSignature(window.returnType()),
                argumentTypes,
                false);

        return new SqlWindowFunction(new ReflectionWindowFunctionSupplier<>(signature, clazz));
    }
}
