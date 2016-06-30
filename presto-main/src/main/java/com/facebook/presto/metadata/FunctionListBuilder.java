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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.JsonPath;
import com.facebook.presto.operator.scalar.ReflectionParametricScalar;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.operator.window.ReflectionWindowFunctionSupplier;
import com.facebook.presto.operator.window.SqlWindowFunction;
import com.facebook.presto.operator.window.ValueWindowFunction;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.joni.Regex;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionKind.WINDOW;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FunctionListBuilder
{
    private static final Set<Class<?>> NON_NULLABLE_ARGUMENT_TYPES = ImmutableSet.of(
            long.class,
            double.class,
            boolean.class,
            Regex.class,
            JsonPath.class);

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

    public FunctionListBuilder aggregate(Class<?> aggregationDefinition)
    {
        functions.addAll(SqlAggregationFunction.createByAnnotations(aggregationDefinition));
        return this;
    }

    public FunctionListBuilder scalar(Class<?> clazz)
    {
        ScalarFunction scalarAnnotation = clazz.getAnnotation(ScalarFunction.class);
        ScalarOperator operatorAnnotation = clazz.getAnnotation(ScalarOperator.class);
        if (scalarAnnotation != null || operatorAnnotation != null) {
            functions.addAll(ReflectionParametricScalar.parseDefinition(clazz));
            return this;
        }

        boolean foundOne = false;
        for (Method method : clazz.getMethods()) {
            if (ReflectionParametricScalar.canParseMethodDefinition(method)) {
                functions.addAll(ReflectionParametricScalar.parseDefinition(method));
                foundOne = true;
            }
        }
        checkArgument(foundOne, "Expected class %s to be annotated with @%s, or contain at least one method annotated with @%s", clazz.getName(), ScalarFunction.class.getSimpleName(), ScalarFunction.class.getSimpleName());
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

    @Deprecated
    private static String getDescription(AnnotatedElement annotatedElement)
    {
        Description description = annotatedElement.getAnnotation(Description.class);
        return (description == null) ? null : description.value();
    }

    public List<SqlFunction> getFunctions()
    {
        return ImmutableList.copyOf(functions);
    }
}
