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
package com.facebook.presto.spi.function;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.function.RoutineCharacteristics.Language;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.function.FunctionVersion.notVersioned;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class FunctionMetadata
{
    private final QualifiedObjectName name;
    private final Optional<OperatorType> operatorType;
    private final List<TypeSignature> argumentTypes;
    private final Optional<List<String>> argumentNames;
    private final TypeSignature returnType;
    private final FunctionKind functionKind;
    private final Optional<Language> language;
    private final FunctionImplementationType implementationType;
    private final boolean deterministic;
    private final boolean calledOnNullInput;
    private final FunctionVersion version;

    public FunctionMetadata(
            QualifiedObjectName name,
            List<TypeSignature> argumentTypes,
            TypeSignature returnType,
            FunctionKind functionKind,
            FunctionImplementationType implementationType,
            boolean deterministic,
            boolean calledOnNullInput)
    {
        this(name, Optional.empty(), argumentTypes, Optional.empty(), returnType, functionKind, Optional.empty(), implementationType, deterministic, calledOnNullInput, notVersioned());
    }

    public FunctionMetadata(
            QualifiedObjectName name,
            List<TypeSignature> argumentTypes,
            List<String> argumentNames,
            TypeSignature returnType,
            FunctionKind functionKind,
            Language language,
            FunctionImplementationType implementationType,
            boolean deterministic,
            boolean calledOnNullInput,
            FunctionVersion version)
    {
        this(name, Optional.empty(), argumentTypes, Optional.of(argumentNames), returnType, functionKind, Optional.of(language), implementationType, deterministic, calledOnNullInput, version);
    }

    public FunctionMetadata(
            OperatorType operatorType,
            List<TypeSignature> argumentTypes,
            TypeSignature returnType,
            FunctionKind functionKind,
            FunctionImplementationType implementationType,
            boolean deterministic,
            boolean calledOnNullInput)
    {
        this(operatorType.getFunctionName(), Optional.of(operatorType), argumentTypes, Optional.empty(), returnType, functionKind, Optional.empty(), implementationType, deterministic, calledOnNullInput, notVersioned());
    }

    private FunctionMetadata(
            QualifiedObjectName name,
            Optional<OperatorType> operatorType,
            List<TypeSignature> argumentTypes,
            Optional<List<String>> argumentNames,
            TypeSignature returnType,
            FunctionKind functionKind,
            Optional<Language> language,
            FunctionImplementationType implementationType,
            boolean deterministic,
            boolean calledOnNullInput,
            FunctionVersion version)
    {
        this.name = requireNonNull(name, "name is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.argumentTypes = unmodifiableList(new ArrayList<>(requireNonNull(argumentTypes, "argumentTypes is null")));
        this.argumentNames = requireNonNull(argumentNames, "argumentNames is null").map(names -> unmodifiableList(new ArrayList<>(names)));
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.functionKind = requireNonNull(functionKind, "functionKind is null");
        this.language = requireNonNull(language, "language is null");
        this.implementationType = requireNonNull(implementationType, "implementationType is null");
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
        this.version = requireNonNull(version, "version is null");
    }

    public FunctionKind getFunctionKind()
    {
        return functionKind;
    }

    public QualifiedObjectName getName()
    {
        return name;
    }

    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    public Optional<List<String>> getArgumentNames()
    {
        return argumentNames;
    }

    public TypeSignature getReturnType()
    {
        return returnType;
    }

    public Optional<OperatorType> getOperatorType()
    {
        return operatorType;
    }

    public Optional<Language> getLanguage()
    {
        return language;
    }

    public FunctionImplementationType getImplementationType()
    {
        return implementationType;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }

    public FunctionVersion getVersion()
    {
        return version;
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
        FunctionMetadata other = (FunctionMetadata) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.operatorType, other.operatorType) &&
                Objects.equals(this.argumentTypes, other.argumentTypes) &&
                Objects.equals(this.argumentNames, other.argumentNames) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.functionKind, other.functionKind) &&
                Objects.equals(this.language, other.language) &&
                Objects.equals(this.implementationType, other.implementationType) &&
                Objects.equals(this.deterministic, other.deterministic) &&
                Objects.equals(this.calledOnNullInput, other.calledOnNullInput) &&
                Objects.equals(this.version, other.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, operatorType, argumentTypes, argumentNames, returnType, functionKind, language, implementationType, deterministic, calledOnNullInput, version);
    }
}
