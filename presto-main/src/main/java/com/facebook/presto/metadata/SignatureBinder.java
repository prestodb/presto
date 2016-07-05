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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public abstract class SignatureBinder
{
    protected final TypeManager typeManager;
    protected final Signature declaredSignature;
    protected final boolean allowCoercion;

    public SignatureBinder(TypeManager typeManager, Signature declaredSignature, boolean allowCoercion)
    {
        this.allowCoercion = allowCoercion;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.declaredSignature = requireNonNull(declaredSignature, "parametrizedSignature is null");
    }

    public abstract Optional<Signature> bind(List<? extends Type> actualArgumentTypes);
    public abstract Optional<BoundVariables> bindVariables(List<? extends Type> actualArgumentTypes, Type actualReturnType);
}
