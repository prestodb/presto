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

import com.facebook.presto.operator.scalar.annotations.CodegenScalarFromAnnotationsParser;
import com.facebook.presto.operator.scalar.annotations.ScalarFromAnnotationsParser;
import com.facebook.presto.operator.scalar.annotations.SqlInvokedScalarFromAnnotationsParser;
import com.facebook.presto.operator.window.WindowAnnotationsParser;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunction;
import com.facebook.presto.spi.function.WindowFunction;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class FunctionExtractor
{
    private FunctionExtractor() {}

    public static List<SqlFunction> extractFunctions(Collection<Class<?>> classes)
    {
        return classes.stream()
                .map(FunctionExtractor::extractFunctions)
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    public static List<? extends SqlFunction> extractFunctions(Class<?> clazz)
    {
        if (WindowFunction.class.isAssignableFrom(clazz)) {
            @SuppressWarnings("unchecked")
            Class<? extends WindowFunction> windowClazz = (Class<? extends WindowFunction>) clazz;
            return WindowAnnotationsParser.parseFunctionDefinition(windowClazz);
        }

        if (clazz.isAnnotationPresent(AggregationFunction.class)) {
            return SqlAggregationFunction.createFunctionsByAnnotations(clazz);
        }

        if (clazz.isAnnotationPresent(ScalarFunction.class) ||
                clazz.isAnnotationPresent(ScalarOperator.class)) {
            return ScalarFromAnnotationsParser.parseFunctionDefinition(clazz);
        }

        if (clazz.isAnnotationPresent(SqlInvokedScalarFunction.class)) {
            return SqlInvokedScalarFromAnnotationsParser.parseFunctionDefinition(clazz);
        }

        List<SqlFunction> scalarFunctions = ImmutableList.<SqlFunction>builder()
                .addAll(ScalarFromAnnotationsParser.parseFunctionDefinitions(clazz))
                .addAll(SqlInvokedScalarFromAnnotationsParser.parseFunctionDefinitions(clazz))
                .addAll(CodegenScalarFromAnnotationsParser.parseFunctionDefinitions(clazz))
                .build();
        checkArgument(!scalarFunctions.isEmpty(), "Class [%s] does not define any scalar functions", clazz.getName());
        return scalarFunctions;
    }
}
