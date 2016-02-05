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

import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.ParameterKind;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.type.UnknownType;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.type.TypeCalculation.calculateLiteralValue;
import static com.facebook.presto.type.TypeRegistry.canCoerce;
import static com.facebook.presto.type.TypeRegistry.getCommonSuperTypeSignature;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getLast;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class SignatureBinder
{
    private final TypeManager typeManager;
    private final Signature declaredSignature;
    private final boolean allowCoercion;
    private final Map<String, TypeVariableConstraint> typeVariableConstraints;

    public SignatureBinder(TypeManager typeManager, Signature declaredSignature, boolean allowCoercion)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.declaredSignature = requireNonNull(declaredSignature, "parametrizedSignature is null");
        this.allowCoercion = allowCoercion;
        this.typeVariableConstraints = declaredSignature.getTypeVariableConstraints()
                .stream()
                .collect(toMap(TypeVariableConstraint::getName, t -> t));
    }

    public Optional<Signature> bind(List<? extends Type> actualArgumentTypes)
    {
        Optional<BoundVariables> boundVariables = bindVariables(actualArgumentTypes);
        if (!boundVariables.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(bindVariables(declaredSignature, boundVariables.get(), actualArgumentTypes.size()));
    }

    public Optional<BoundVariables> bindVariables(List<? extends Type> actualArgumentTypes)
    {
        List<TypeSignature> expectedArgumentSignatures = declaredSignature.getArgumentTypes();
        boolean variableArity = declaredSignature.isVariableArity();
        BoundVariables.Builder variableBinder = BoundVariables.builder();
        if (!matchArguments(expectedArgumentSignatures, actualArgumentTypes, variableArity, variableBinder)) {
            return Optional.empty();
        }
        calculateVariableValuesForLongConstraints(variableBinder);
        BoundVariables boundVariables = variableBinder.build();
        if (!allTypeVariablesBound(boundVariables)) {
            return Optional.empty();
        }
        return Optional.of(boundVariables);
    }

    public Optional<BoundVariables> bindVariables(List<? extends Type> actualArgumentTypes, Type actualReturnType)
    {
        BoundVariables.Builder variableBinder = BoundVariables.builder();

        TypeSignature expectedReturnTypeSignature = declaredSignature.getReturnType();
        if (!bind(expectedReturnTypeSignature, actualReturnType, variableBinder)) {
            return Optional.empty();
        }

        List<TypeSignature> expectedArgumentSignatures = declaredSignature.getArgumentTypes();
        boolean variableArity = declaredSignature.isVariableArity();
        if (!matchArguments(expectedArgumentSignatures, actualArgumentTypes, variableArity, variableBinder)) {
            return Optional.empty();
        }

        calculateVariableValuesForLongConstraints(variableBinder);
        BoundVariables boundVariables = variableBinder.build();
        if (!allTypeVariablesBound(boundVariables)) {
            return Optional.empty();
        }
        return Optional.of(boundVariables);
    }

    private boolean matchArguments(
            List<TypeSignature> expectedTypes,
            List<? extends Type> actualTypes,
            boolean variableArity,
            BoundVariables.Builder variableBinder)
    {
        if (variableArity && (actualTypes.size() < (expectedTypes.size() - 1))) {
            return false;
        }

        if (!variableArity && expectedTypes.size() != actualTypes.size()) {
            return false;
        }

        // Bind the variable arity argument first, to make sure it's bound to the common super type
        if (variableArity && actualTypes.size() >= expectedTypes.size()) {
            List<? extends Type> tail = actualTypes.subList(expectedTypes.size() - 1, actualTypes.size());
            Optional<Type> commonType = typeManager.getCommonSuperType(tail);
            if (!commonType.isPresent()) {
                return false;
            }
            TypeSignature expected = expectedTypes.get(expectedTypes.size() - 1);
            if (!bind(expected, commonType.get(), variableBinder)) {
                return false;
            }
        }

        for (int i = 0; i < actualTypes.size(); i++) {
            // Get the current argument signature, or the last one, if this is a varargs function
            TypeSignature expectedArgumentSignature = expectedTypes.get(Math.min(i, expectedTypes.size() - 1));
            Type actualArgumentType = actualTypes.get(i);
            if (!bind(expectedArgumentSignature, actualArgumentType, variableBinder)) {
                return false;
            }
        }

        return true;
    }

    private boolean bind(
            TypeSignature expected,
            Type actual,
            BoundVariables.Builder variableBinder)
    {
        if (isTypeVariable(expected)) {
            String variable = expected.getBase();
            return matchAndBindTypeVariable(variable, actual, variableBinder);
        }

        TypeSignature actualArgumentSignature = actual.getTypeSignature();

        if (baseTypesAreEqual(expected, actualArgumentSignature)
                && matchAndBindTypeParameters(expected, actual, variableBinder)) {
            return true;
        }
        else if (allowCoercion) {
            // UNKNOWN matches to all the types, but based on UNKNOWN we can't determine the actual type parameters
            if (actual.equals(UnknownType.UNKNOWN) && isTypeParametrized(expected)) {
                return true;
            }
            Optional<Type> coercedType = coerceType(actualArgumentSignature, expected);
            if (coercedType.isPresent()) {
                return matchAndBindTypeParameters(expected, coercedType.get(), variableBinder);
            }
        }

        return false;
    }

    private boolean matchAndBindTypeVariable(
            String variable,
            Type actualType,
            BoundVariables.Builder variableBinder)
    {
        TypeVariableConstraint typeVariableConstraint = typeVariableConstraints.get(variable);
        if (!variableBinder.containsTypeVariable(variable)) {
            if (typeVariableConstraint.canBind(actualType)) {
                variableBinder.setTypeVariable(variable, actualType);
                return true;
            }

            // UNKNOWN matches to all the types, but based on UNKNOWN we can't determine the actual type parameters
            if (allowCoercion && actualType.equals(UnknownType.UNKNOWN)) {
                return true;
            }

            // TODO Refactor type registry and support coercion to variadic bound fully
        }
        // type variable is already bound. just check that bound types are compatible.
        else {
            Type currentBoundType = variableBinder.getTypeVariable(variable);
            if (currentBoundType.equals(actualType)) {
                return true;
            }
            if (allowCoercion) {
                Optional<Type> commonSuperType = typeManager.getCommonSuperType(currentBoundType, actualType);
                if (commonSuperType.isPresent() && typeVariableConstraint.canBind(commonSuperType.get())) {
                    variableBinder.setTypeVariable(variable, commonSuperType.get());
                    return true;
                }
            }
        }
        return false;
    }

    private boolean matchAndBindTypeParameters(
            TypeSignature expectedArgumentSignature,
            Type actualArgumentType,
            BoundVariables.Builder variableBinder)
    {
        TypeSignature actualArgumentSignature = actualArgumentType.getTypeSignature();
        checkState(baseTypesAreEqual(expectedArgumentSignature, actualArgumentSignature), "equal base types are expected here");

        List<TypeSignatureParameter> expectedTypeParameters = expectedArgumentSignature.getParameters();
        List<TypeSignatureParameter> actualTypeParameters = actualArgumentSignature.getParameters();

        // expected no parameters, but actual type has parameters bound
        // actually such situation may happen only for varchar
        // in future we should replace not parametrized VARCHAR type with the parametrized in function signatures
        if (expectedTypeParameters.isEmpty() && !actualTypeParameters.isEmpty()) {
            checkState(expectedArgumentSignature.getBase().equals(StandardTypes.VARCHAR),
                    "This is just legacy trick for VARCHAR. " +
                            "newly added methods for parametrized types must declare literal parameters in signatures");
            return true;
        }

        if (expectedTypeParameters.size() != actualTypeParameters.size()) {
            return false;
        }

        for (int typeParameterIndex = 0; typeParameterIndex < expectedTypeParameters.size(); typeParameterIndex++) {
            TypeSignatureParameter expectedTypeParameter = expectedTypeParameters.get(typeParameterIndex);
            TypeSignatureParameter actualTypeParameter = actualTypeParameters.get(typeParameterIndex);
            if (!matchAndBindTypeParameter(expectedTypeParameter, actualTypeParameter, variableBinder)) {
                return false;
            }
        }

        return true;
    }

    private boolean matchAndBindTypeParameter(TypeSignatureParameter expected, TypeSignatureParameter actual, BoundVariables.Builder variableBinder)
    {
        switch (expected.getKind()) {
            case VARIABLE: {
                String variable = expected.getVariable();
                checkState(actual.getKind() == ParameterKind.LONG,
                        "LONG parameter kind is expected here");
                Long variableValue = actual.getLongLiteral();
                return matchAndBindLongVariable(variable, variableValue, variableBinder);
            }
            case LONG: {
                checkState(actual.getKind() == ParameterKind.LONG,
                        "LONG parameter kind is expected here");
                return actual.getLongLiteral().equals(expected.getLongLiteral());
            }
            case TYPE: {
                checkState(actual.getKind() == ParameterKind.TYPE,
                        "TYPE parameter kind is expected here");
                TypeSignature expectedTypeSignature = expected.getTypeSignature();
                TypeSignature actualTypeSignature = actual.getTypeSignature();
                Type actualType = typeManager.getType(actualTypeSignature);
                return bind(expectedTypeSignature, actualType, variableBinder);
            }
            case NAMED_TYPE: {
                checkState(actual.getKind() == ParameterKind.NAMED_TYPE,
                        "NAMED_TYPE parameter kind is expected here");
                NamedTypeSignature expectedNamedTypeSignature = expected.getNamedTypeSignature();
                NamedTypeSignature actualNamedTypeSignature = actual.getNamedTypeSignature();
                if (!expectedNamedTypeSignature.getName().equals(actualNamedTypeSignature.getName())) {
                    return false;
                }
                TypeSignature expectedTypeSignature = expectedNamedTypeSignature.getTypeSignature();
                TypeSignature actualTypeSignature = actualNamedTypeSignature.getTypeSignature();
                Type actualType = typeManager.getType(actualTypeSignature);
                return bind(expectedTypeSignature, actualType, variableBinder);
            }
            default:
                throw new IllegalStateException("Unknown parameter kind: " + expected.getKind());
        }
    }

    private boolean matchAndBindLongVariable(String variable, Long value, BoundVariables.Builder variableBinder)
    {
        if (variableBinder.containsLongVariable(variable)) {
            Long currentVariableValue = variableBinder.getLongVariable(variable);
            return value.equals(currentVariableValue);
        }
        else {
            variableBinder.setLongVariable(variable, value);
            return true;
        }
    }

    private void calculateVariableValuesForLongConstraints(BoundVariables.Builder variableBinder)
    {
        for (LongVariableConstraint longVariableConstraint : declaredSignature.getLongVariableConstraints()) {
            String calculation = longVariableConstraint.getExpression();
            String variableName = longVariableConstraint.getName();
            Long calculatedValue = calculateLiteralValue(calculation, variableBinder.getLongVariables());
            if (variableBinder.containsLongVariable(variableName)) {
                Long currentValue = variableBinder.getLongVariable(variableName);
                checkState(Objects.equals(currentValue, calculatedValue),
                        "variable '%s' is already set to %s when trying to set %s", variableName, currentValue, calculatedValue);
            }
            variableBinder.setLongVariable(variableName, calculatedValue);
        }
    }

    private boolean isTypeVariable(TypeSignature signature)
    {
        if (typeVariableConstraints.containsKey(signature.getBase())) {
            checkState(signature.getParameters().isEmpty(),
                    "TypeSignature that represent type variable shouldn't be parametrized");
            return true;
        }
        return false;
    }

    private Optional<Type> coerceType(TypeSignature actual, TypeSignature expected)
    {
        // TODO: this must be calculated in TypeRegistry
        // TODO: Going to be removed after TypeRegistry refactor
        if (actual.getBase().equals(UnknownType.NAME)) {
            if (expected.getBase().equals(StandardTypes.DECIMAL)) {
                return Optional.of(createDecimalType(1, 0));
            }
            if (expected.getBase().equals(StandardTypes.VARCHAR)) {
                return Optional.of(createVarcharType(0));
            }
        }
        if (expected.isCalculated()) {
            return Optional.empty();
        }

        if (!canCoerce(actual, expected)) {
            return Optional.empty();
        }

        Optional<TypeSignature> commonType = getCommonSuperTypeSignature(actual, expected);
        checkState(commonType.isPresent(), "common supper type signature hasn't been found");
        checkState(baseTypesAreEqual(expected, commonType.get()),
                "base types are supposed to be equal after coercion");
        Type type = typeManager.getType(commonType.get());
        checkState(type != null, "type signature for concrete type must be calculated here");
        return Optional.of(type);
    }

    private boolean isTypeParametrized(TypeSignature expectedSignature)
    {
        if (isTypeVariable(expectedSignature)) {
            return true;
        }

        for (TypeSignatureParameter parameter : expectedSignature.getParameters()) {
            Optional<TypeSignature> typeSignature = parameter.getTypeSignatureOrNamedTypeSignature();
            if (typeSignature.isPresent() && isTypeParametrized(typeSignature.get())) {
                return true;
            }
        }

        return false;
    }

    private boolean baseTypesAreEqual(TypeSignature first, TypeSignature second)
    {
        return first.getBase().equals(second.getBase());
    }

    private boolean allTypeVariablesBound(BoundVariables boundVariables)
    {
        return boundVariables.getTypeVariables().keySet().equals(typeVariableConstraints.keySet());
    }

    public static Signature bindVariables(Signature signature, BoundVariables boundVariables, int arity)
    {
        List<TypeSignature> argumentSignatures = fillInMissingVariableArguments(signature.getArgumentTypes(), arity);
        List<TypeSignature> boundArgumentSignatures = bindVariables(argumentSignatures, boundVariables);
        TypeSignature boundReturnTypeSignature = bindVariables(signature.getReturnType(), boundVariables);

        return new Signature(
                signature.getName(),
                signature.getKind(),
                ImmutableList.of(),
                ImmutableList.of(),
                boundReturnTypeSignature,
                boundArgumentSignatures,
                false
        );
    }

    private static List<TypeSignature> fillInMissingVariableArguments(List<TypeSignature> argumentSignatures, int arity)
    {
        int variableArityArgumentsCount = arity - argumentSignatures.size();
        if (variableArityArgumentsCount > 0 && !argumentSignatures.isEmpty()) {
            ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
            builder.addAll(argumentSignatures);
            for (int i = 0; i < variableArityArgumentsCount; i++) {
                builder.add(getLast(argumentSignatures));
            }
            return builder.build();
        }
        return argumentSignatures;
    }

    public static List<TypeSignature> bindVariables(List<TypeSignature> typeSignatures, BoundVariables boundVariables)
    {
        ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
        for (TypeSignature typeSignature : typeSignatures) {
            builder.add(bindVariables(typeSignature, boundVariables));
        }
        return builder.build();
    }

    public static TypeSignature bindVariables(
            TypeSignature typeSignature,
            BoundVariables boundVariables)
    {
        String baseType = typeSignature.getBase();
        if (boundVariables.containsTypeVariable(baseType)) {
            checkState(typeSignature.getParameters().isEmpty(), "Type parameters cannot have parameters");
            return boundVariables.getTypeVariable(baseType).getTypeSignature();
        }

        List<TypeSignatureParameter> parameters = typeSignature.getParameters().stream()
                .map(typeSignatureParameter -> bindVariables(typeSignatureParameter, boundVariables))
                .collect(toList());

        return new TypeSignature(baseType, parameters);
    }

    private static TypeSignatureParameter bindVariables(
            TypeSignatureParameter parameter,
            BoundVariables boundVariables)
    {
        ParameterKind parameterKind = parameter.getKind();
        switch (parameterKind) {
            case TYPE: {
                TypeSignature typeSignature = parameter.getTypeSignature();
                return TypeSignatureParameter.of(bindVariables(typeSignature, boundVariables));
            }
            case NAMED_TYPE: {
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                TypeSignature typeSignature = namedTypeSignature.getTypeSignature();
                return TypeSignatureParameter.of(new NamedTypeSignature(
                        namedTypeSignature.getName(),
                        bindVariables(typeSignature, boundVariables)));
            }
            case VARIABLE: {
                String variableName = parameter.getVariable();
                checkState(boundVariables.containsLongVariable(variableName),
                        "Variable is not bound: %s", variableName);
                Long variableValue = boundVariables.getLongVariable(variableName);
                return TypeSignatureParameter.of(variableValue);
            }
            case LONG: {
                return parameter;
            }
            default:
                throw new IllegalStateException("Unknown parameter kind: " + parameter.getKind());
        }
    }
}
