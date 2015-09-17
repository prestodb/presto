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
package com.facebook.presto.operator.window;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionType.WINDOW;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class ParametricWindowFunction
        implements ParametricFunction
{
    private final WindowFunctionSupplier supplier;

    public ParametricWindowFunction(WindowFunctionSupplier supplier)
    {
        this.supplier = requireNonNull(supplier, "supplier is null");
    }

    @Override
    public Signature getSignature()
    {
        return supplier.getSignature();
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return supplier.getDescription();
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "expected exactly one type: %s", types);
        String typeVariable = getOnlyElement(types.keySet());
        TypeSignature type = getOnlyElement(types.values()).getTypeSignature();

        List<TypeSignature> argumentTypes = supplier.getSignature().getArgumentTypes().stream()
                .map(argument -> argument.getBase().equals(typeVariable) ? type : argument)
                .collect(toImmutableList());

        Signature signature = new Signature(getSignature().getName(), WINDOW, type, argumentTypes);
        return new FunctionInfo(signature, getDescription(), supplier);
    }
}
