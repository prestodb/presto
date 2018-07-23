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
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.ParametricImplementationsGroup;
import com.facebook.presto.operator.scalar.annotations.ScalarImplementation;
import com.facebook.presto.operator.scalar.annotations.ScalarImplementation.MethodHandleAndConstructor;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;

import java.util.Optional;

import static com.facebook.presto.metadata.SignatureBinder.applyBoundVariables;
import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_IMPLEMENTATION;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParametricScalar
        extends SqlScalarFunction
{
    private final ScalarHeader details;
    private final ParametricImplementationsGroup<ScalarImplementation> implementations;

    public ParametricScalar(
            Signature signature,
            ScalarHeader details,
            ParametricImplementationsGroup<ScalarImplementation> implementations)
    {
        super(signature);
        this.details = requireNonNull(details);
        this.implementations = requireNonNull(implementations);
    }

    @Override
    public boolean isHidden()
    {
        return details.isHidden();
    }

    @Override
    public boolean isDeterministic()
    {
        return details.isDeterministic();
    }

    @Override
    public String getDescription()
    {
        return details.getDescription().isPresent() ? details.getDescription().get() : "";
    }

    @VisibleForTesting
    public ParametricImplementationsGroup<ScalarImplementation> getImplementations()
    {
        return implementations;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        Signature boundSignature = applyBoundVariables(getSignature(), boundVariables, arity);
        if (implementations.getExactImplementations().containsKey(boundSignature)) {
            ScalarImplementation implementation = implementations.getExactImplementations().get(boundSignature);
            Optional<MethodHandleAndConstructor> methodHandleAndConstructor = implementation.specialize(boundSignature, boundVariables, typeManager, functionManager);
            checkCondition(methodHandleAndConstructor.isPresent(), FUNCTION_IMPLEMENTATION_ERROR, String.format("Exact implementation of %s do not match expected java types.", boundSignature.getName()));
            return new ScalarFunctionImplementation(
                    implementation.isNullable(),
                    implementation.getArgumentProperties(),
                    methodHandleAndConstructor.get().getMethodHandle(),
                    methodHandleAndConstructor.get().getConstructor(),
                    isDeterministic());
        }

        ScalarFunctionImplementation selectedImplementation = null;
        for (ScalarImplementation implementation : implementations.getSpecializedImplementations()) {
            Optional<MethodHandleAndConstructor> methodHandle = implementation.specialize(boundSignature, boundVariables, typeManager, functionManager);
            if (methodHandle.isPresent()) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getSignature(), boundVariables.getTypeVariables());
                selectedImplementation = new ScalarFunctionImplementation(
                        implementation.isNullable(),
                        implementation.getArgumentProperties(),
                        methodHandle.get().getMethodHandle(),
                        methodHandle.get().getConstructor(),
                        isDeterministic());
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }

        for (ScalarImplementation implementation : implementations.getGenericImplementations()) {
            Optional<MethodHandleAndConstructor> methodHandle = implementation.specialize(boundSignature, boundVariables, typeManager, functionManager);
            if (methodHandle.isPresent()) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getSignature(), boundVariables.getTypeVariables());
                selectedImplementation = new ScalarFunctionImplementation(
                        implementation.isNullable(),
                        implementation.getArgumentProperties(),
                        methodHandle.get().getMethodHandle(),
                        methodHandle.get().getConstructor(),
                        isDeterministic());
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }

        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("Unsupported type parameters (%s) for %s", boundVariables, getSignature()));
    }
}
