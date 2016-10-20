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
package com.facebook.presto.operator.annotations;

import com.facebook.presto.metadata.TypeVariableConstraint;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.annotations.ImplementationDependency.isImplementationDependencyAnnotation;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class AnnotationHelpers
{
    private static final Set<OperatorType> COMPARABLE_TYPE_OPERATORS = ImmutableSet.of(EQUAL, NOT_EQUAL, HASH_CODE);
    private static final Set<OperatorType> ORDERABLE_TYPE_OPERATORS = ImmutableSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, BETWEEN);

    private AnnotationHelpers()
    {}

    public static boolean containsImplementationDependencyAnnotation(Annotation[] annotations)
    {
        for (Annotation annotation : annotations) {
            if (isImplementationDependencyAnnotation(annotation)) {
                return true;
            }
        }
        return false;
    }

    public static List<TypeVariableConstraint> createTypeVariableConstraints(Iterable<TypeParameter> typeParameters, List<ImplementationDependency> dependencies)
    {
        Set<String> orderableRequired = new HashSet<>();
        Set<String> comparableRequired = new HashSet<>();
        for (ImplementationDependency dependency : dependencies) {
            if (dependency instanceof OperatorImplementationDependency) {
                OperatorType operator = ((OperatorImplementationDependency) dependency).getOperator();
                if (operator == CAST) {
                    continue;
                }
                Set<String> argumentTypes = ((OperatorImplementationDependency) dependency).getSignature().getArgumentTypes().stream()
                        .map(TypeSignature::getBase)
                        .collect(toImmutableSet());
                checkArgument(argumentTypes.size() == 1, "Operator dependency must only have arguments of a single type");
                String argumentType = Iterables.getOnlyElement(argumentTypes);
                if (COMPARABLE_TYPE_OPERATORS.contains(operator)) {
                    comparableRequired.add(argumentType);
                }
                if (ORDERABLE_TYPE_OPERATORS.contains(operator)) {
                    orderableRequired.add(argumentType);
                }
            }
        }
        ImmutableList.Builder<TypeVariableConstraint> typeVariableConstraints = ImmutableList.builder();
        for (TypeParameter typeParameter : typeParameters) {
            String name = typeParameter.value();
            if (orderableRequired.contains(name)) {
                typeVariableConstraints.add(orderableTypeParameter(name));
            }
            else if (comparableRequired.contains(name)) {
                typeVariableConstraints.add(comparableTypeParameter(name));
            }
            else {
                typeVariableConstraints.add(typeVariable(name));
            }
        }
        return typeVariableConstraints.build();
    }
}
