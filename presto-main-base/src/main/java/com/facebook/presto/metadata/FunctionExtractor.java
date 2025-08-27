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

import com.facebook.presto.common.CatalogSchemaName;
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

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public final class FunctionExtractor
{
    private FunctionExtractor() {}

    public static List<SqlFunction> extractFunctions(Collection<Class<?>> classes)
    {
        return extractFunctions(classes, JAVA_BUILTIN_NAMESPACE);
    }

    public static List<SqlFunction> extractFunctions(Collection<Class<?>> classes, CatalogSchemaName functionNamespace)
    {
        return classes.stream()
                .map(c -> extractFunctions(c, functionNamespace))
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    public static List<? extends SqlFunction> extractFunctions(Class<?> clazz)
    {
        return extractFunctions(clazz, JAVA_BUILTIN_NAMESPACE);
    }

    public static List<? extends SqlFunction> extractFunctions(Class<?> clazz, CatalogSchemaName defaultNamespace)
    {
        if (WindowFunction.class.isAssignableFrom(clazz)) {
            checkArgument(defaultNamespace.equals(JAVA_BUILTIN_NAMESPACE), format("Connector specific Window functions are not supported: Class [%s], Namespace [%s]", clazz.getName(), defaultNamespace));
            @SuppressWarnings("unchecked")
            Class<? extends WindowFunction> windowClazz = (Class<? extends WindowFunction>) clazz;
            return WindowAnnotationsParser.parseFunctionDefinition(windowClazz);
        }

        if (clazz.isAnnotationPresent(AggregationFunction.class)) {
            return SqlAggregationFunction.createFunctionsByAnnotations(clazz, defaultNamespace);
        }

        if (clazz.isAnnotationPresent(ScalarFunction.class)) {
            return ScalarFromAnnotationsParser.parseFunctionDefinition(clazz, defaultNamespace);
        }
        if (clazz.isAnnotationPresent(ScalarOperator.class)) {
            checkArgument(defaultNamespace.equals(JAVA_BUILTIN_NAMESPACE), format("Connector specific Scalar Operator functions are not supported: Class [%s], Namespace [%s]", clazz.getName(), defaultNamespace));
            return ScalarFromAnnotationsParser.parseFunctionDefinition(clazz);
        }

        if (clazz.isAnnotationPresent(SqlInvokedScalarFunction.class)) {
            return SqlInvokedScalarFromAnnotationsParser.parseFunctionDefinition(clazz, defaultNamespace);
        }

        List<SqlFunction> scalarFunctions = ImmutableList.<SqlFunction>builder()
                .addAll(ScalarFromAnnotationsParser.parseFunctionDefinitions(clazz, defaultNamespace))
                .addAll(SqlInvokedScalarFromAnnotationsParser.parseFunctionDefinitions(clazz, defaultNamespace))
                .addAll(CodegenScalarFromAnnotationsParser.parseFunctionDefinitions(clazz, defaultNamespace))
                .build();
        checkArgument(!scalarFunctions.isEmpty(), "Class [%s] does not define any scalar functions", clazz.getName());
        return scalarFunctions;
    }
}
