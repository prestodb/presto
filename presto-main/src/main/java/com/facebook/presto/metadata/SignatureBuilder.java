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

import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;

import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.metadata.FunctionType.SCALAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Lists.transform;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public final class SignatureBuilder
{
    private String name;
    private FunctionType type;
    private List<TypeParameter> typeParameters = emptyList();
    private TypeSignature returnType;
    private List<TypeSignature> argumentTypes;
    private boolean variableArity;

    public SignatureBuilder() {}

    public SignatureBuilder name(String name)
    {
        this.name = requireNonNull(name, "name is null");
        return this;
    }

    public SignatureBuilder type(FunctionType type)
    {
        this.type = type;
        return this;
    }

    public SignatureBuilder operatorType(OperatorType operatorType)
    {
        this.name = mangleOperatorName(requireNonNull(operatorType, "operatorType is null"));
        this.type = SCALAR;
        return this;
    }

    public SignatureBuilder typeParameters(TypeParameter... typeParameters)
    {
        return typeParameters(asList(requireNonNull(typeParameters, "typeParameters is null")));
    }

    public SignatureBuilder typeParameters(List<TypeParameter> typeParameters)
    {
        this.typeParameters = copyOf(requireNonNull(typeParameters, "typeParameters is null"));
        return this;
    }

    public SignatureBuilder returnType(String returnType)
    {
        this.returnType = parseTypeSignature(requireNonNull(returnType, "returnType is null"));
        return this;
    }

    public SignatureBuilder argumentTypes(String... argumentTypes)
    {
        return argumentTypes(asList(requireNonNull(argumentTypes, "argumentTypes is Null")));
    }

    public SignatureBuilder argumentTypes(List<String> argumentTypes)
    {
        this.argumentTypes = transform(copyOf(requireNonNull(argumentTypes, "argumentTypes is null")), TypeSignature::parseTypeSignature);
        return this;
    }

    public SignatureBuilder setVariableArity(boolean variableArity)
    {
        this.variableArity = variableArity;
        return this;
    }

    public Signature build()
    {
        return new Signature(name, type, typeParameters, returnType, argumentTypes, variableArity);
    }
}
