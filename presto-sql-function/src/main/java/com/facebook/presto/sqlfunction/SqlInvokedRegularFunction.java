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
package com.facebook.presto.sqlfunction;

import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.relation.FullyQualifiedName;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class SqlInvokedRegularFunction
        implements SqlFunction
{
    private static final Map<RoutineCharacteristics.Language, FunctionImplementationType> LANGUAGE_TO_IMPLEMENTATION_MAP = ImmutableMap.of(RoutineCharacteristics.Language.SQL, FunctionImplementationType.SQL);

    private final List<SqlParameter> parameters;
    private final Optional<String> comment;
    private final RoutineCharacteristics routineCharacteristics;
    private final String body;

    private final Signature signature;
    private final SqlFunctionId functionId;
    private final Optional<SqlInvokedRegularFunctionHandle> functionHandle;

    public SqlInvokedRegularFunction(
            FullyQualifiedName functionName,
            List<SqlParameter> parameters,
            TypeSignature returnType,
            Optional<String> comment,
            RoutineCharacteristics routineCharacteristics,
            String body,
            Optional<Long> version)
    {
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.routineCharacteristics = requireNonNull(routineCharacteristics, "routineCharacteristics is null");
        this.body = requireNonNull(body, "body is null");

        List<TypeSignature> argumentTypes = parameters.stream()
                .map(SqlParameter::getType)
                .collect(collectingAndThen(toList(), Collections::unmodifiableList));
        this.signature = new Signature(functionName, SCALAR, returnType, argumentTypes);
        this.functionId = new SqlFunctionId(functionName, argumentTypes);
        this.functionHandle = version.map(v -> new SqlInvokedRegularFunctionHandle(functionName, argumentTypes, v));
    }

    public static SqlInvokedRegularFunction versioned(SqlInvokedRegularFunction function, long version)
    {
        if (function.getVersion().isPresent()) {
            throw new IllegalArgumentException(format("function %s is already versioned", function.getVersion().get()));
        }
        return new SqlInvokedRegularFunction(
                function.getSignature().getName(),
                function.getParameters(),
                function.getSignature().getReturnType(),
                function.comment,
                function.getRoutineCharacteristics(),
                function.getBody(),
                Optional.of(version));
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return routineCharacteristics.isDeterministic();
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return routineCharacteristics.isCalledOnNullInput();
    }

    @Override
    public String getDescription()
    {
        return comment.orElse("");
    }

    public List<SqlParameter> getParameters()
    {
        return parameters;
    }

    public List<String> getParameterNames()
    {
        return parameters.stream().map(SqlParameter::getName).collect(toImmutableList());
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public RoutineCharacteristics getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }

    public FunctionImplementationType getFunctionImplementationType()
    {
        checkState(LANGUAGE_TO_IMPLEMENTATION_MAP.containsKey(routineCharacteristics.getLanguage()), "Language is not supported: %s", routineCharacteristics.getLanguage());
        return LANGUAGE_TO_IMPLEMENTATION_MAP.get(routineCharacteristics.getLanguage());
    }

    public String getBody()
    {
        return body;
    }

    public SqlFunctionId getFunctionId()
    {
        return functionId;
    }

    public SqlInvokedRegularFunctionHandle getRequiredFunctionHandle()
    {
        checkState(functionHandle.isPresent(), "missing function handle");
        return functionHandle.get();
    }

    public Optional<Long> getVersion()
    {
        return functionHandle.map(SqlInvokedRegularFunctionHandle::getVersion);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlInvokedRegularFunction o = (SqlInvokedRegularFunction) obj;
        return Objects.equals(parameters, o.parameters)
                && Objects.equals(comment, o.comment)
                && Objects.equals(routineCharacteristics, o.routineCharacteristics)
                && Objects.equals(body, o.body)
                && Objects.equals(signature, o.signature)
                && Objects.equals(functionId, o.functionId)
                && Objects.equals(functionHandle, o.functionHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parameters, comment, routineCharacteristics, body, signature, functionId, functionHandle);
    }

    @Override
    public String toString()
    {
        return format(
                "%s(%s):%s%s {%s} %s",
                signature.getName(),
                parameters.stream()
                        .map(Object::toString)
                        .collect(joining(",")),
                signature.getReturnType(),
                getVersion().map(version -> ":" + version).orElse(""),
                body,
                routineCharacteristics);
    }
}
