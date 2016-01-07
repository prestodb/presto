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
package com.facebook.presto.type;

import com.facebook.presto.metadata.TypeParameter;
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

import static com.facebook.presto.type.TypeRegistry.isCovariantParameterPosition;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public final class TypeUnification
{
    private TypeUnification()
    {
    }

    public static TypeUnificationResult unify(
            Map<String, TypeParameter> typeParameterMap,
            List<? extends TypeSignature> declaredTypes,
            List<? extends TypeSignature> actualTypes,
            boolean allowCoercion,
            boolean variableArity,
            TypeManager typeManager)
    {
        // Allocate a type symbol for every type parameter
        // Split String->TypeParameter map into 2 maps: String->TypeSymbol and TypeSymbol->TypeParameter
        ImmutableMap.Builder<String, TypeSymbol> typeParameterToTypeSymbolBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<TypeSymbol, TypeParameter> typeSymbolToTypeParameterBuilder = ImmutableMap.builder();
        for (Map.Entry<String, TypeParameter> entry : typeParameterMap.entrySet()) {
            TypeSymbol symbol = new TypeSymbol();
            typeParameterToTypeSymbolBuilder.put(entry.getKey(), symbol);
            typeSymbolToTypeParameterBuilder.put(symbol, entry.getValue());
        }
        ImmutableMap<String, TypeSymbol> typeParameterToTypeSymbolMap = typeParameterToTypeSymbolBuilder.build();
        ImmutableMap<TypeSymbol, TypeParameter> typeSymbolToTypeParameterMap = typeSymbolToTypeParameterBuilder.build();

        try {
            // Verify number of actual argument types match number of declared argument types
            if (variableArity) {
                checkCondition(declaredTypes.size() - 1 <= actualTypes.size());
            }
            else {
                checkCondition(declaredTypes.size() == actualTypes.size());
            }

            // Allocate a type symbol for every actual argument and add base constraints to each
            ArrayList<TypeSymbol> symbolForActualTypes = new ArrayList<>(actualTypes.size());
            Equivalences equivalences = new Equivalences(typeParameterToTypeSymbolMap);
            for (int i = 0; i < Math.min(actualTypes.size(), declaredTypes.size()); i++) {
                TypeSymbol symbol = new TypeSymbol();
                symbolForActualTypes.add(symbol);
                TypeSignature actualType = actualTypes.get(i);
                equivalences.add(symbol, new TypeConstraint(actualType, allowCoercion));
                equivalences.add(symbol, new TypeConstraint(declaredTypes.get(i), false));
            }

            for (int i = declaredTypes.size(); i < actualTypes.size(); i++) {
                // this for loop only runs when there are at least two arguments for vararg parameter
                equivalences.add(symbolForActualTypes.get(declaredTypes.size() - 1), new TypeConstraint(actualTypes.get(i), allowCoercion));
            }

            // Do the heavy lifting work
            equivalences = deriveEquivalences(equivalences);

            Map<TypeSymbol, TypeSignature> resolvedSymbols =
                    analyzeEquivalences(equivalences.toSymbolsAndConstraints(), typeParameterToTypeSymbolMap.keySet(), typeSymbolToTypeParameterMap, typeManager);

            // Turn the result in terms of type symbols into result in terms of type parameters and actual arguments.
            List<TypeSignature> resolvedArguments = symbolForActualTypes.stream()
                    .map(resolvedSymbols::get)
                    .collect(Collectors.toList());
            ImmutableMap.Builder<String, TypeSignature> resolvedTypeParameters = new ImmutableMap.Builder<>();
            for (Map.Entry<String, TypeSymbol> entry : typeParameterToTypeSymbolMap.entrySet()) {
                TypeSignature resolvedTo = resolvedSymbols.get(entry.getValue());
                if (resolvedTo != null) {
                    resolvedTypeParameters.put(entry.getKey(), resolvedTo);
                }
            }
            return new TypeUnificationResult(resolvedTypeParameters.build(), resolvedArguments);
        }
        catch (TypeUnificationFailure e) {
            return null;
        }
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
        private final Set<TypeConstraint> inconcreteConstraints;

        private SymbolsAndConstraints(Set<TypeSymbol> symbols, Set<TypeConstraint> concreteConstraints, Set<TypeConstraint> inconcreteConstraints)
        {
            this.symbols = symbols;
            this.concreteConstraints = concreteConstraints;
            this.inconcreteConstraints = inconcreteConstraints;
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

        /**
         * @param typeParameters a type constraint is considered concrete if it doesn't concrete any of the type parameters
         * @param typeParametersToReplace replace occurrences of these type parameters in inconcrete type constraints
         * @param resolvedTypeSignature replacement for {@code typeParametersToReplace}
         */
        public SymbolsAndConstraints replace(Set<String> typeParameters, Set<String> typeParametersToReplace, TypeSignature resolvedTypeSignature)
        {
            Set<TypeConstraint> newConcreteConstraints = new HashSet<>(concreteConstraints);
            Set<TypeConstraint> newOtherConstraints = new HashSet<>();
            for (TypeConstraint constraint : inconcreteConstraints) {
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
        private final ImmutableMap<String, TypeSymbol> typeParameterToSymbol;
        private final UnionFindMap<TypeSymbol, TypeConstraint> symbolsToConstraints;

        public Equivalences(ImmutableMap<String, TypeSymbol> typeParameterToSymbol)
        {
            this.typeParameterToSymbol = typeParameterToSymbol;
            this.symbolsToConstraints = new UnionFindMap<>();
        }

        public Equivalences(ImmutableMap<String, TypeSymbol> typeParameterToSymbol, UnionFindMap<TypeSymbol, TypeConstraint> symbolsToConstraints)
        {
            this.typeParameterToSymbol = typeParameterToSymbol;
            this.symbolsToConstraints = symbolsToConstraints;
        }

        /**
         * @return <tt>true</tt> if the specified equivalence is new
         */
        public boolean add(TypeSymbol leftTypeSymbol, TypeSymbol rightTypeSymbol)
        {
            return symbolsToConstraints.union(leftTypeSymbol, rightTypeSymbol);
        }

        /**
         * @return <tt>true</tt> if the specified equivalence is new
         */
        public boolean add(TypeSymbol typeSymbol, TypeConstraint constraint)
        {
            TypeSignature constraintTypeSignature = constraint.typeSignature;
            if (typeParameterToSymbol.containsKey(constraintTypeSignature.getBase())) {
                return add(typeSymbol, typeParameterToSymbol.get(constraintTypeSignature.getBase()));
            }
            else {
                return symbolsToConstraints.put(typeSymbol, constraint);
            }
        }

        /**
         * @return <tt>true</tt> if the specified equivalence is new and useful
         */
        public boolean add(TypeConstraint leftConstraint, TypeConstraint rightConstraint)
                throws TypeUnificationFailure
        {
            if (typeParameterToSymbol.containsKey(leftConstraint.typeSignature.getBase())) {
                return add(typeParameterToSymbol.get(leftConstraint.typeSignature.getBase()), rightConstraint);
            }
            else if (typeParameterToSymbol.containsKey(rightConstraint.typeSignature.getBase())) {
                return add(typeParameterToSymbol.get(rightConstraint.typeSignature.getBase()), leftConstraint);
            }
            // do nothing if neither side is a symbol
            return false;
        }

        public List<SymbolsAndConstraints> toSymbolsAndConstraints()
        {
            Set<String> typeParameters = typeParameterToSymbol.keySet();
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
        // when true, implicit coerce rules apply ; when false, must be exact match
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

        private static boolean isConcrete(TypeSignature typeSignature, Set<String> typeParameters)
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

        private static TypeSignature replace(TypeSignature typeSignature, Set<String> typeParametersToReplace, TypeSignature resolvedTypeSignature)
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

            return canCoerce == that.canCoerce && typeSignature.equals(that.typeSignature);
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

    private static Equivalences deriveEquivalences(Equivalences inputEquivalence)
            throws TypeUnificationFailure
    {
        Equivalences oldEquivalences = inputEquivalence;
        while (true) {
            Equivalences newEquivalences = new Equivalences(oldEquivalences.typeParameterToSymbol, new UnionFindMap<>(oldEquivalences.symbolsToConstraints));
            boolean newEquivalenceDerived = false;
            for (Map.Entry<TypeSymbol, Set<TypeConstraint>> entry : oldEquivalences.symbolsToConstraints.values().entrySet()) {
                TypeSymbol typeSymbol = entry.getKey();
                Set<TypeConstraint> oldConstraints = entry.getValue();
                Set<TypeConstraint> newConstraints = newEquivalences.symbolsToConstraints.get(typeSymbol);
                for (TypeConstraint oldConstraint : oldConstraints) {
                    for (TypeConstraint newConstraint : newConstraints) {
                        if (doDeriveEquivalence(newEquivalences, oldConstraint, newConstraint)) {
                            newEquivalenceDerived = true;
                        }
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
    private static boolean doDeriveEquivalence(Equivalences equiv, TypeConstraint leftConstraint, TypeConstraint rightConstraint)
            throws TypeUnificationFailure
    {
        List<TypeSignature> leftTypeTypeParameters = leftConstraint.typeSignature.getParameters();
        List<TypeSignature> rightTypeTypeParameters = rightConstraint.typeSignature.getParameters();
        if ((leftTypeTypeParameters.size() == 0) != (rightTypeTypeParameters.size() == 0)) {
            // exactly one of leftTypeTypeParameters and rightTypeTypeParameters has size 0
            return equiv.add(leftConstraint, rightConstraint);
        }

        // now that either: (1) both side are empty or, (2) neither side is empty
        checkCondition(leftTypeTypeParameters.size() == rightTypeTypeParameters.size());
        int parameterCount = leftTypeTypeParameters.size();

        if (parameterCount == 0) {
            return equiv.add(leftConstraint, rightConstraint);
        }

        // It is known at this point that base types of both sides are concrete types because T cannot have type parameters. e.g. T<int> is not allowed.
        // Knowing this, it is not this method's concern how and whether leftConstraint.typeSignature.getBase() match with rightConstraint.typeSignature.getBase()
        boolean newEquivalenceDerived = false;
        for (int i = 0; i < leftTypeTypeParameters.size(); i++) {
            if (equiv.add(new TypeConstraint(leftTypeTypeParameters.get(i), leftConstraint.canCoerce && isCovariantParameterPosition(leftConstraint.typeSignature.getBase(), i)),
                    new TypeConstraint(rightTypeTypeParameters.get(i), rightConstraint.canCoerce && isCovariantParameterPosition(rightConstraint.typeSignature.getBase(), i)))) {
                newEquivalenceDerived = true;
            }
        }
        return newEquivalenceDerived;
    }

    private static Map<TypeSymbol, TypeSignature> analyzeEquivalences(
            List<SymbolsAndConstraints> equivalences,
            Set<String> typeParameters,
            Map<TypeSymbol, TypeParameter> typeSymbolToParameterMap,
            TypeManager typeManager)
            throws TypeUnificationFailure
    {
        Map<TypeSymbol, TypeSignature> result = new HashMap<>();
        Queue<SymbolsAndConstraints> concreteQueue = new ArrayDeque<>(equivalences.size());
        List<SymbolsAndConstraints> inconcreteList = new ArrayList<>(equivalences.size());
        for (SymbolsAndConstraints symbolsAndConstraints : equivalences) {
            if (symbolsAndConstraints.inconcreteConstraints.isEmpty()) {
                concreteQueue.add(symbolsAndConstraints);
            }
            else {
                inconcreteList.add(symbolsAndConstraints);
            }
        }

        while (!concreteQueue.isEmpty()) {
            SymbolsAndConstraints symbolsAndConstraints = concreteQueue.remove();
            Set<TypeConstraint> concreteConstraints = symbolsAndConstraints.concreteConstraints;

            if (concreteConstraints.size() == 0) {
                continue; // Still unbound after inference
            }
            TypeSignature resolvedTypeSignature;
            if (concreteConstraints.size() == 1) {
                resolvedTypeSignature = getOnlyElement(concreteConstraints).typeSignature;
            }
            else {
                Optional<TypeSignature> commonSuperTypeSignature = TypeRegistry.getCommonSuperTypeSignature(
                        concreteConstraints.stream()
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

            List<SymbolsAndConstraints> newInconcreteList = new ArrayList<>(inconcreteList.size());
            for (SymbolsAndConstraints beforeReplace : inconcreteList) {
                SymbolsAndConstraints afterReplace = beforeReplace.replace(typeParameters, typeParametersToReplace, resolvedTypeSignature);
                if (afterReplace.inconcreteConstraints.isEmpty()) {
                    concreteQueue.add(afterReplace);
                }
                else {
                    newInconcreteList.add(afterReplace);
                }
            }
            inconcreteList = newInconcreteList;
        }

        return result;
    }
}
