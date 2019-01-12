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
package io.prestosql.metadata;

import io.prestosql.operator.scalar.annotations.ScalarFromAnnotationsParser;
import io.prestosql.operator.window.WindowAnnotationsParser;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.WindowFunction;

import java.util.Collection;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public final class FunctionExtractor
{
    private FunctionExtractor() {}

    public static List<? extends SqlFunction> extractFunctions(Collection<Class<?>> classes)
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

        return ScalarFromAnnotationsParser.parseFunctionDefinitions(clazz);
    }
}
