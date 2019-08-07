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

import com.facebook.presto.spi.function.FunctionFeature;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.LongVariableConstraint;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.TypeVariableConstraint;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.immutableEnumSet;
import static java.util.Collections.emptySet;

public abstract class SqlOperator
        extends SqlScalarFunction
{
    private final OperatorType operatorType;

    protected SqlOperator(OperatorType operatorType, List<TypeVariableConstraint> typeVariableConstraints, List<LongVariableConstraint> longVariableConstraints, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        // TODO This should take Signature!
        super(new Signature(
                operatorType.getFunctionName(),
                FunctionKind.SCALAR,
                typeVariableConstraints,
                longVariableConstraints,
                returnType,
                argumentTypes,
                false));
        this.operatorType = operatorType;
    }

    @Override
    public final boolean isHidden()
    {
        return true;
    }

    @Override
    public final boolean isDeterministic()
    {
        return true;
    }

    @Override
    public final boolean isCalledOnNullInput()
    {
        return operatorType.isCalledOnNullInput();
    }

    @Override
    public Set<FunctionFeature> getFunctionFeatures()
    {
        if (operatorType.isCanReturnNullOnNonNullInput()) {
            return immutableEnumSet(FunctionFeature.CAN_RETURN_NULL_FOR_NON_NULL_INPUT);
        }
        return emptySet();
    }

    @Override
    public final String getDescription()
    {
        // Operators are internal, and don't need a description
        return null;
    }
}
