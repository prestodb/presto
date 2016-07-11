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
import com.facebook.presto.operator.window.WindowAnnotationsParser;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.WindowFunction;

import java.util.Collection;
import java.util.List;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

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
            return SqlAggregationFunction.createByAnnotations(clazz);
        }

        if (clazz.isAnnotationPresent(ScalarFunction.class) ||
                clazz.isAnnotationPresent(ScalarOperator.class)) {
            return ScalarFromAnnotationsParser.parseFunctionDefinition(clazz);
        }

        return ScalarFromAnnotationsParser.parseFunctionDefinitions(clazz);
    }
}
