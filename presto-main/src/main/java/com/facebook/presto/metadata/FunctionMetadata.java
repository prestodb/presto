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

import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FunctionMetadata
{
    private final Signature signature;
    private final boolean deterministic;

    public FunctionMetadata(Signature signature, boolean deterministic)
    {
        checkArgument(signature.getLongVariableConstraints().isEmpty() && signature.getTypeVariableConstraints().isEmpty());
        this.signature = requireNonNull(signature, "signature is null");
        this.deterministic = deterministic;
    }

    public FunctionKind getFunctionKind()
    {
        return signature.getKind();
    }

    public String getName()
    {
        return signature.getName();
    }

    public List<TypeSignature> getArgumentTypes()
    {
        return signature.getArgumentTypes();
    }

    public TypeSignature getReturnType()
    {
        return signature.getReturnType();
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }
}
