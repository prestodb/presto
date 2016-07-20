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
package com.facebook.presto.metadata;

import com.facebook.presto.operator.scalar.annotations.ScalarFromAnnotationsParser;
import com.facebook.presto.operator.window.ReflectionWindowFunctionSupplier;
import com.facebook.presto.operator.window.SqlWindowFunction;
import com.facebook.presto.operator.window.WindowAnnotationsParser;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.function.ValueWindowFunction;
import com.facebook.presto.spi.function.WindowFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.metadata.FunctionKind.WINDOW;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

public class FunctionListBuilder
{
    private final List<SqlFunction> functions = new ArrayList<>();

    public FunctionListBuilder window(String name, Type returnType, List<? extends Type> argumentTypes, Class<? extends WindowFunction> functionClass)
    {
        WindowFunctionSupplier windowFunctionSupplier = new ReflectionWindowFunctionSupplier<>(
                new Signature(name, WINDOW, returnType.getTypeSignature(), Lists.transform(ImmutableList.copyOf(argumentTypes), Type::getTypeSignature)),
                functionClass);

        functions.add(new SqlWindowFunction(windowFunctionSupplier));
        return this;
    }

    public FunctionListBuilder window(String name, Class<? extends ValueWindowFunction> clazz, String typeVariable, String... argumentTypes)
    {
        Signature signature = new Signature(
                name,
                WINDOW,
                ImmutableList.of(typeVariable(typeVariable)),
                ImmutableList.of(),
                parseTypeSignature(typeVariable),
                Arrays.asList(argumentTypes).stream().map(TypeSignature::parseTypeSignature).collect(toImmutableList()),
                false);
        functions.add(new SqlWindowFunction(new ReflectionWindowFunctionSupplier<>(signature, clazz)));
        return this;
    }

    public FunctionListBuilder window(Class<? extends WindowFunction> clazz)
    {
        functions.addAll(WindowAnnotationsParser.parseFunctionDefinition(clazz));
        return this;
    }

    public FunctionListBuilder aggregate(Class<?> aggregationDefinition)
    {
        functions.addAll(SqlAggregationFunction.createByAnnotations(aggregationDefinition));
        return this;
    }

    public FunctionListBuilder scalar(Class<?> clazz)
    {
        functions.addAll(ScalarFromAnnotationsParser.parseFunctionDefinition(clazz));
        return this;
    }

    public FunctionListBuilder scalars(Class<?> clazz)
    {
        functions.addAll(ScalarFromAnnotationsParser.parseFunctionDefinitions(clazz));
        return this;
    }

    public FunctionListBuilder functions(SqlFunction... sqlFunctions)
    {
        for (SqlFunction sqlFunction : sqlFunctions) {
            function(sqlFunction);
        }
        return this;
    }

    public FunctionListBuilder function(SqlFunction sqlFunction)
    {
        requireNonNull(sqlFunction, "parametricFunction is null");
        functions.add(sqlFunction);
        return this;
    }

    public List<SqlFunction> getFunctions()
    {
        return ImmutableList.copyOf(functions);
    }
}
