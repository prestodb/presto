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
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.FunctionRegistry.getCommonSuperTypeSignature;
import static com.facebook.presto.metadata.FunctionRegistry.isCovariantParameterPosition;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

final class TypeUnification
{
    private TypeUnification()
    {
    }

    public static class TypeUnificationResult
    {
        private final Map<String, TypeSignature> resolvedTypeParameters;
        private final List<TypeSignature> resolvedArguments;

        public TypeUnificationResult(Map<String, TypeSignature> resolvedTypeParameters, List<TypeSignature> resolvedArguments)
        {
            this.resolvedTypeParameters = resolvedTypeParameters;
            this.resolvedArguments = resolvedArguments;
        }

        public Map<String, TypeSignature> getResolvedTypeParameters()
        {
            return resolvedTypeParameters;
        }

        public List<TypeSignature> getResolvedArguments()
        {
            return resolvedArguments;
        }
    }

    private static class SymbolsAndConstraints
    {
        private final Set<TypeSymbol> symbols;
        private final Set<TypeConstraint> concreteConstraints;
        private final Set<TypeConstraint> otherConstraints;

        private SymbolsAndConstraints(Set<TypeSymbol> symbols, Set<TypeConstraint> concreteConstraints, Set<TypeConstraint> otherConstraints)
        {
            this.symbols = symbols;
            this.concreteConstraints = concreteConstraints;
            this.otherConstraints = otherConstraints;
        }

        public static SymbolsAndConstraints of(Set<TypeSymbol> symbols, Set<TypeConstraint> constraints, Set<String> typeParameters)
        {
            Set<TypeConstraint> concreteConstraints = new HashSet<>();
            Set<TypeConstraint> otherConstraints = new HashSet<>();
            for (TypeConstraint constraint : constraints) {
                if (constraint.isConcrete(typeParameters)) {
                    concreteConstraints.add(constraint);
                }
                else {
                    otherConstraints.add(constraint);
                }
            }
            return new SymbolsAndConstraints(symbols, concreteConstraints, otherConstraints);
        }

        public SymbolsAndConstraints replace(Set<String> typeParameters, Set<String> typeParametersToReplace, TypeSignature resolvedTypeSignature)
        {
            Set<TypeConstraint> newConcreteConstraints = new HashSet<>();
            Set<TypeConstraint> newOtherConstraints = new HashSet<>();
            newConcreteConstraints.addAll(concreteConstraints);
            for (TypeConstraint constraint : otherConstraints) {
                TypeConstraint replacedConstraint = constraint.replace(typeParametersToReplace, resolvedTypeSignature);
                if (replacedConstraint.isConcrete(typeParameters)) {
                    newConcreteConstraints.add(replacedConstraint);
                }
                else {
                    newOtherConstraints.add(replacedConstraint);
                }
            }
            return new SymbolsAndConstraints(symbols, newConcreteConstraints, newOtherConstraints);
        }
    }

    private static class Equivalences
    {
        private final Map<String, TypeSymbol> typeParameterToSymbolMap;
        private final DisjointSetsWithData<TypeSymbol, TypeConstraint> symbolsToConstraints;

        public Equivalences(Map<String, TypeSymbol> typeParameterToSymbolMap)
        {
            this.typeParameterToSymbolMap = typeParameterToSymbolMap;
            this.symbolsToConstraints = new DisjointSetsWithData<>();
        }

        public Equivalences(Map<String, TypeSymbol> typeParameterToSymbolMap, DisjointSetsWithData<TypeSymbol, TypeConstraint> symbolsToConstraints)
        {
            this.typeParameterToSymbolMap = typeParameterToSymbolMap;
            this.symbolsToConstraints = symbolsToConstraints;
        }

        public Equivalences clone()
        {
            return new Equivalences(typeParameterToSymbolMap, symbolsToConstraints.clone());
        }

        /**
         * @return <tt>true</tt> if this structure did not already contain the specified equivalence
         */
        public boolean add(TypeSymbol leftTypeSymbol, TypeSymbol rightTypeSymbol)
        {
            return symbolsToConstraints.union(leftTypeSymbol, rightTypeSymbol);
        }

        /**
         * @return <tt>true</tt> if this structure did not already contain the specified equivalence
         */
        public boolean add(TypeSymbol typeSymbol, TypeConstraint constraint)
        {
            TypeSignature constraintTypeSignature = constraint.typeSignature;
            if (typeParameterToSymbolMap.containsKey(constraintTypeSignature.getBase())) {
                return add(typeSymbol, typeParameterToSymbolMap.get(constraintTypeSignature.getBase()));
            }
            else {
                return symbolsToConstraints.put(typeSymbol, constraint);
            }
        }

        /**
         * @return <tt>true</tt> if this structure did not already contain the specified equivalence
         */
        public boolean add(TypeConstraint leftConstraint, TypeConstraint rightConstraint)
                throws TypeUnificationFailure
        {
            if (typeParameterToSymbolMap.containsKey(leftConstraint.typeSignature.getBase())) {
                return add(typeParameterToSymbolMap.get(leftConstraint.typeSignature.getBase()), rightConstraint);
            }
            else if (typeParameterToSymbolMap.containsKey(rightConstraint.typeSignature.getBase())) {
                return add(typeParameterToSymbolMap.get(rightConstraint.typeSignature.getBase()), leftConstraint);
            }
            // do nothing if neither side is a symbol
            return false;
        }

        public List<SymbolsAndConstraints> toSymbolsAndConstraints(Set<String> typeParameters)
        {
            ImmutableList.Builder<SymbolsAndConstraints> result = new ImmutableList.Builder<>();
            for (Map.Entry<Set<TypeSymbol>, Set<TypeConstraint>> entry : symbolsToConstraints.toMap().entrySet()) {
                SymbolsAndConstraints symbolsAndConstraints = SymbolsAndConstraints.of(entry.getKey(), entry.getValue(), typeParameters);
                result.add(symbolsAndConstraints);
            }
            return result.build();
        }
    }

    private static class TypeSymbol
    {
    }

    private static class TypeConstraint
    {
        private final TypeSignature typeSignature;
        private final boolean canCoerce;

        public TypeConstraint(TypeSignature typeSignature, boolean canCoerce)
        {
            this.typeSignature = requireNonNull(typeSignature);
            this.canCoerce = canCoerce;
        }

        public boolean isConcrete(Set<String> typeParameters)
        {
            return isConcrete(typeSignature, typeParameters);
        }

        public static boolean isConcrete(TypeSignature typeSignature, Set<String> typeParameters)
        {
            if (typeParameters.contains(typeSignature.getBase())) {
                return false;
            }
            for (TypeSignature subSignature : typeSignature.getParameters()) {
                if (!isConcrete(subSignature, typeParameters)) {
                    return false;
                }
            }
            return true;
        }

        public TypeConstraint replace(Set<String> typeParametersToReplace, TypeSignature resolvedTypeSignature)
        {
            return new TypeConstraint(replace(typeSignature, typeParametersToReplace, resolvedTypeSignature), canCoerce);
        }

        public static TypeSignature replace(TypeSignature typeSignature, Set<String> typeParametersToReplace, TypeSignature resolvedTypeSignature)
        {
            if (typeParametersToReplace.contains(typeSignature.getBase())) {
                return resolvedTypeSignature;
            }
            ImmutableList.Builder<TypeSignature> newSubSignatures = new ImmutableList.Builder<>();
            for (TypeSignature subSignature : typeSignature.getParameters()) {
                newSubSignatures.add(replace(subSignature, typeParametersToReplace, resolvedTypeSignature));
            }
            return new TypeSignature(typeSignature.getBase(), newSubSignatures.build(), typeSignature.getLiteralParameters());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TypeConstraint that = (TypeConstraint) o;

            if (canCoerce != that.canCoerce) {
                return false;
            }
            if (!typeSignature.equals(that.typeSignature)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return 31 * typeSignature.hashCode() + (canCoerce ? 1 : 0);
        }
    }

    private static class TypeUnificationFailure
            extends Exception
    {
    }

    private static void checkCondition(boolean expr)
            throws TypeUnificationFailure
    {
        if (!expr) {
            throw new TypeUnificationFailure();
        }
    }

    public static TypeUnificationResult unify(
            Map<String, TypeParameter> typeParameterMap,
            List<? extends TypeSignature> expectedTypes,
            List<? extends TypeSignature> actualTypes,
            boolean allowCoercion,
            boolean variableArity,
            TypeManager typeManager)
    {
        Set<String> typeParameters = typeParameterMap.keySet();
        Map<TypeSymbol, TypeParameter> typeSymbolToParameterMap = new HashMap<>();
        Map<String, TypeSymbol> typeParameterToSymbolMap = new HashMap<>();
        for (Map.Entry<String, TypeParameter> entry : typeParameterMap.entrySet()) {
            TypeSymbol symbol = new TypeSymbol();
            typeParameterToSymbolMap.put(entry.getKey(), symbol);
            typeSymbolToParameterMap.put(symbol, entry.getValue());
        }

        Equivalences equivalences = new Equivalences(typeParameterToSymbolMap);
        ArrayList<TypeSymbol> symbolForActualTypes = new ArrayList<>(actualTypes.size());

        try {
            if (variableArity) {
                checkCondition(expectedTypes.size() - 1 <= actualTypes.size());
            }
            else {
                checkCondition(expectedTypes.size() == actualTypes.size());
            }

            for (int i = 0; i < Math.min(actualTypes.size(), expectedTypes.size()); i++) {
                TypeSymbol symbol = new TypeSymbol();
                symbolForActualTypes.add(symbol);
                TypeSignature actualType = actualTypes.get(i);
                equivalences.add(symbol, new TypeConstraint(actualType, allowCoercion));
                equivalences.add(symbol, new TypeConstraint(expectedTypes.get(i), false));
            }

            // this for loop only runs if expectedTypes.size() + 1 <= actualTypes.size()
            for (int i = expectedTypes.size(); i < actualTypes.size(); i++) {
                equivalences.add(symbolForActualTypes.get(expectedTypes.size() - 1), new TypeConstraint(actualTypes.get(i), allowCoercion));
            }

            equivalences = deriveEquivalences(equivalences);

            Map<TypeSymbol, TypeSignature> resolvedSymbols = analyzeEquivalences(equivalences.toSymbolsAndConstraints(typeParameters), typeParameters, typeSymbolToParameterMap, typeManager);

            List<TypeSignature> resolvedArguments = symbolForActualTypes.stream()
                    .map(resolvedSymbols::get)
                    .collect(Collectors.toList());
            ImmutableMap.Builder<String, TypeSignature> resolvedTypeParameters = new ImmutableMap.Builder<>();
            for (String typeParameter : typeParameterMap.keySet()) {
                TypeSignature resolvedTo = resolvedSymbols.get(typeParameterToSymbolMap.get(typeParameter));
                if (resolvedTo != null) {
                    resolvedTypeParameters.put(typeParameter, resolvedTo);
                }
            }
            return new TypeUnificationResult(resolvedTypeParameters.build(), resolvedArguments);
        }
        catch (TypeUnificationFailure e) {
            return null;
        }
    }

    public static Equivalences deriveEquivalences(Equivalences inputEquivalence)
            throws TypeUnificationFailure
    {
        Equivalences oldEquivalences = inputEquivalence;
        while (true) {
            Equivalences newEquivalences = oldEquivalences.clone();
            boolean newEquivalenceDerived = false;
            for (Map.Entry<TypeSymbol, Set<TypeConstraint>> entry : oldEquivalences.symbolsToConstraints.values().entrySet()) {
                TypeSymbol typeSymbol = entry.getKey();
                Set<TypeConstraint> constraints = entry.getValue();
                Set<TypeConstraint> newConstraints = newEquivalences.symbolsToConstraints.get(typeSymbol);
                for (TypeConstraint constraint : constraints) {
                    for (TypeConstraint newConstraint : newConstraints) {
                        newEquivalenceDerived = doDeriveEquivalence(newEquivalences, constraint, newConstraint) || newEquivalenceDerived;
                    }
                }
            }
            if (!newEquivalenceDerived) {
                break;
            }
            oldEquivalences = newEquivalences;
        }
        return oldEquivalences;
    }

    /**
     * @return <tt>true</tt> if any new equivalence or constraint is derived.
     */
    public static boolean doDeriveEquivalence(Equivalences equiv, TypeConstraint leftConstraint, TypeConstraint rightConstraint)
            throws TypeUnificationFailure
    {
        List<TypeSignature> leftTypeTypeParameters = leftConstraint.typeSignature.getParameters();
        List<TypeSignature> rightTypeTypeParameters = rightConstraint.typeSignature.getParameters();
        if ((leftTypeTypeParameters.size() == 0) != (rightTypeTypeParameters.size() == 0)) {
            // exactly one of expectedTypeParameters and actualTypeParameters has size 0
            return equiv.add(leftConstraint, rightConstraint);
        }

        // now that either: (1) both side are empty or, (2) neither side is empty
        checkCondition(leftTypeTypeParameters.size() == rightTypeTypeParameters.size());
        int parameterCount = leftTypeTypeParameters.size();

        boolean newEquivalenceDerived = false;
        if (parameterCount == 0) {
            newEquivalenceDerived = equiv.add(leftConstraint, rightConstraint) || newEquivalenceDerived;
        }
        else {
            // It is known at this point that base types of both sides are concrete types because T cannot have type parameters. e.g. T<int> is not allowed.
            // Knowing this, it is not this method's concern how and whether leftConstraint.typeSignature.getBase() match with rightConstraint.typeSignature.getBase()
            for (int i = 0; i < leftTypeTypeParameters.size(); i++) {
                newEquivalenceDerived =
                        equiv.add(new TypeConstraint(leftTypeTypeParameters.get(i), leftConstraint.canCoerce && isCovariantParameterPosition(leftConstraint.typeSignature.getBase(), i)),
                        new TypeConstraint(rightTypeTypeParameters.get(i), rightConstraint.canCoerce && isCovariantParameterPosition(rightConstraint.typeSignature.getBase(), i)))
                        || newEquivalenceDerived;
            }
        }
        return newEquivalenceDerived;
    }

    public static Map<TypeSymbol, TypeSignature> analyzeEquivalences(
            List<SymbolsAndConstraints> equivalences,
            Set<String> typeParameters,
            Map<TypeSymbol, TypeParameter> typeSymbolToParameterMap,
            TypeManager typeManager)
            throws TypeUnificationFailure
    {
        Map<TypeSymbol, TypeSignature> result = new HashMap<>();
        Queue<SymbolsAndConstraints> queue = new ArrayDeque<>(equivalences.size());
        List<SymbolsAndConstraints> newEquivalences = new ArrayList<>(equivalences.size());
        for (SymbolsAndConstraints symbolsAndConstraints : equivalences) {
            if (symbolsAndConstraints.otherConstraints.isEmpty()) {
                queue.add(symbolsAndConstraints);
            }
            else {
                newEquivalences.add(symbolsAndConstraints);
            }
        }
        equivalences = newEquivalences;

        while (!queue.isEmpty()) {
            SymbolsAndConstraints symbolsAndConstraints = queue.remove();
            Set<TypeConstraint> concreteConstraints = symbolsAndConstraints.concreteConstraints;

            if (concreteConstraints.size() == 0) {
                continue; // Still unbound after inference
            }
            TypeSignature resolvedTypeSignature;
            if (concreteConstraints.size() == 1) {
                resolvedTypeSignature = getOnlyElement(concreteConstraints).typeSignature;
            }
            else {
                Optional<TypeSignature> commonSuperTypeSignature = getCommonSuperTypeSignature(concreteConstraints.stream()
                        .map(typeConstraint -> typeConstraint.typeSignature)
                        .collect(ImmutableCollectors.toImmutableList()));
                checkCondition(commonSuperTypeSignature.isPresent());
                resolvedTypeSignature = commonSuperTypeSignature.get();
                for (TypeConstraint concreteConstraint : concreteConstraints) {
                    checkCondition(concreteConstraint.canCoerce || resolvedTypeSignature.equals(concreteConstraint.typeSignature));
                }
            }
            Type resolvedType = typeManager.getType(resolvedTypeSignature);
            Set<String> typeParametersToReplace = new HashSet<>();
            for (TypeSymbol typeSymbol : symbolsAndConstraints.symbols) {
                TypeParameter typeParameter = typeSymbolToParameterMap.get(typeSymbol);
                if (typeParameter != null) {
                    checkCondition(typeParameter.canBind(resolvedType));
                    typeParametersToReplace.add(typeParameter.getName());
                }
                result.put(typeSymbol, resolvedTypeSignature);
            }

            newEquivalences = new ArrayList<>(equivalences.size());
            for (SymbolsAndConstraints beforeReplace : equivalences) {
                SymbolsAndConstraints afterReplace = beforeReplace.replace(typeParameters, typeParametersToReplace, resolvedTypeSignature);
                if (afterReplace.otherConstraints.isEmpty()) {
                    queue.add(afterReplace);
                }
                else {
                    newEquivalences.add(afterReplace);
                }
            }
            equivalences = newEquivalences;
        }

        return result;
    }
}
