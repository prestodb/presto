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
package com.facebook.presto.operator.scalar.annotations;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.operator.scalar.ScalarHeader;
import com.facebook.presto.spi.function.ComplexTypeFunctionDescriptor;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.operator.annotations.FunctionsParserHelper.parseDescription;
import static com.facebook.presto.operator.annotations.FunctionsParserHelper.parseFunctionDescriptor;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ScalarImplementationHeader
{
    private final QualifiedObjectName name;
    private final Optional<OperatorType> operatorType;
    private final ScalarHeader header;

    private ScalarImplementationHeader(String name, ScalarHeader header)
    {
        this(name, header, JAVA_BUILTIN_NAMESPACE);
    }

    private ScalarImplementationHeader(String name, ScalarHeader header, CatalogSchemaName functionNamespace)
    {
        this.name = QualifiedObjectName.valueOf(requireNonNull(functionNamespace), requireNonNull(name));
        this.operatorType = Optional.empty();
        this.header = requireNonNull(header);
    }

    private ScalarImplementationHeader(OperatorType operatorType, ScalarHeader header)
    {
        this.name = operatorType.getFunctionName();
        this.operatorType = Optional.of(operatorType);
        this.header = requireNonNull(header);
    }

    private static String annotatedName(AnnotatedElement annotatedElement)
    {
        if (annotatedElement instanceof Class<?>) {
            return ((Class<?>) annotatedElement).getSimpleName();
        }
        else if (annotatedElement instanceof Method) {
            return ((Method) annotatedElement).getName();
        }

        throw new UnsupportedOperationException("Only Classes and Methods are supported as annotated elements.");
    }

    private static String camelToSnake(String name)
    {
        return LOWER_CAMEL.to(LOWER_UNDERSCORE, name);
    }

    public static List<ScalarImplementationHeader> fromAnnotatedElement(AnnotatedElement annotated, CatalogSchemaName functionNamespace)
    {
        ScalarFunction scalarFunction = annotated.getAnnotation(ScalarFunction.class);
        ScalarOperator scalarOperator = annotated.getAnnotation(ScalarOperator.class);
        Optional<String> description = parseDescription(annotated);
        ComplexTypeFunctionDescriptor descriptor = parseFunctionDescriptor(annotated);

        ImmutableList.Builder<ScalarImplementationHeader> builder = ImmutableList.builder();

        if (scalarFunction != null) {
            String baseName = scalarFunction.value().isEmpty() ? camelToSnake(annotatedName(annotated)) : scalarFunction.value();
            builder.add(new ScalarImplementationHeader(baseName, new ScalarHeader(description, scalarFunction.visibility(), scalarFunction.deterministic(), scalarFunction.calledOnNullInput(), descriptor), functionNamespace));

            for (String alias : scalarFunction.alias()) {
                builder.add(new ScalarImplementationHeader(alias, new ScalarHeader(description, scalarFunction.visibility(), scalarFunction.deterministic(), scalarFunction.calledOnNullInput(), descriptor), functionNamespace));
            }
        }

        if (scalarOperator != null) {
            builder.add(new ScalarImplementationHeader(scalarOperator.value(), new ScalarHeader(description, HIDDEN, true, scalarOperator.value().isCalledOnNullInput(), descriptor)));
        }

        List<ScalarImplementationHeader> result = builder.build();
        checkArgument(!result.isEmpty());
        return result;
    }

    public QualifiedObjectName getName()
    {
        return name;
    }

    public Optional<OperatorType> getOperatorType()
    {
        return operatorType;
    }

    public Optional<String> getDescription()
    {
        return header.getDescription();
    }

    public SqlFunctionVisibility getVisibility()
    {
        return header.getVisibility();
    }

    public ScalarHeader getHeader()
    {
        return header;
    }
}
