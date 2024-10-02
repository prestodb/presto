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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.ParametricImplementationsGroup;
import com.facebook.presto.operator.scalar.annotations.ParametricScalarImplementation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.google.common.annotations.VisibleForTesting;

import java.util.Optional;

import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_IMPLEMENTATION;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParametricScalar
        extends SqlScalarFunction
{
    private final ScalarHeader scalarHeader;
    private final ParametricImplementationsGroup<ParametricScalarImplementation> implementations;

    public ParametricScalar(
            Signature signature,
            ScalarHeader scalarHeader,
            ParametricImplementationsGroup<ParametricScalarImplementation> implementations)
    {
        super(signature);
        this.scalarHeader = requireNonNull(scalarHeader);
        this.implementations = requireNonNull(implementations);
    }

    @Override
    public SqlFunctionVisibility getVisibility()
    {
        return scalarHeader.getVisibility();
    }

    public ScalarHeader getScalarHeader()
    {
        return scalarHeader;
    }

    @Override
    public boolean isDeterministic()
    {
        return scalarHeader.isDeterministic();
    }

    @Override
    public boolean isCalledOnNullInput()
    {
        return scalarHeader.isCalledOnNullInput();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("signature", getSignature())
                .add("implementation", implementations)
                .add("scalarHeader", scalarHeader).toString();
    }

    @Override
    public String getDescription()
    {
        return scalarHeader.getDescription().isPresent() ? scalarHeader.getDescription().get() : "";
    }

    @VisibleForTesting
    public ParametricImplementationsGroup<ParametricScalarImplementation> getImplementations()
    {
        return implementations;
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Signature boundSignature = applyBoundVariables(getSignature(), boundVariables, arity);
        if (implementations.getExactImplementations().containsKey(boundSignature)) {
            ParametricScalarImplementation implementation = implementations.getExactImplementations().get(boundSignature);
            Optional<BuiltInScalarFunctionImplementation> scalarFunctionImplementation = implementation.specialize(boundSignature, boundVariables, functionAndTypeManager);
            checkCondition(scalarFunctionImplementation.isPresent(), FUNCTION_IMPLEMENTATION_ERROR, String.format("Exact implementation of %s do not match expected java types.", boundSignature.getNameSuffix()));
            return scalarFunctionImplementation.get();
        }

        BuiltInScalarFunctionImplementation selectedImplementation = null;
        for (ParametricScalarImplementation implementation : implementations.getSpecializedImplementations()) {
            Optional<BuiltInScalarFunctionImplementation> scalarFunctionImplementation = implementation.specialize(boundSignature, boundVariables, functionAndTypeManager);
            if (scalarFunctionImplementation.isPresent()) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getSignature(), boundVariables.getTypeVariables());
                selectedImplementation = scalarFunctionImplementation.get();
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }
        for (ParametricScalarImplementation implementation : implementations.getGenericImplementations()) {
            Optional<BuiltInScalarFunctionImplementation> scalarFunctionImplementation = implementation.specialize(boundSignature, boundVariables, functionAndTypeManager);
            if (scalarFunctionImplementation.isPresent()) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getSignature(), boundVariables.getTypeVariables());
                selectedImplementation = scalarFunctionImplementation.get();
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }

        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("Unsupported type parameters (%s) for %s", boundVariables, getSignature()));
    }
}
