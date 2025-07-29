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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.analyzer.SignatureMatchingException;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_CALL;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.type.TypeUtils.resolveTypes;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class FunctionSignatureMatcher
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public FunctionSignatureMatcher(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    public Optional<Signature> match(Collection<? extends SqlFunction> candidates, List<TypeSignatureProvider> parameterTypes, boolean coercionAllowed)
    {
        List<SqlFunction> exactCandidates = candidates.stream()
                .filter(function -> function.getSignature().getTypeVariableConstraints().isEmpty())
                .collect(Collectors.toList());

        Optional<Signature> match = matchFunctionExact(exactCandidates, parameterTypes);
        if (match.isPresent()) {
            return match;
        }

        List<SqlFunction> genericCandidates = candidates.stream()
                .filter(function -> !function.getSignature().getTypeVariableConstraints().isEmpty())
                .collect(Collectors.toList());

        match = matchFunctionGeneric(genericCandidates, parameterTypes);
        if (match.isPresent()) {
            return match;
        }

        if (coercionAllowed) {
            match = matchFunctionWithCoercion(candidates, parameterTypes);
            if (match.isPresent()) {
                return match;
            }
        }

        return Optional.empty();
    }

    private Optional<Signature> matchFunctionExact(List<SqlFunction> candidates, List<TypeSignatureProvider> actualParameters)
    {
        return matchFunction(candidates, actualParameters, false);
    }

    private Optional<Signature> matchFunctionGeneric(List<SqlFunction> candidates, List<TypeSignatureProvider> actualParameters)
    {
        List<ApplicableFunction> applicableFunctions = identifyApplicableFunctions(candidates, actualParameters, false);
        if (applicableFunctions.isEmpty()) {
            return Optional.empty();
        }

        if (applicableFunctions.size() == 1) {
            return Optional.of(getOnlyElement(applicableFunctions).getBoundSignature());
        }

        List<Signature> deduplicatedSignatures = applicableFunctions.stream()
                .map(applicableFunction -> applicableFunction.boundSignature)
                .distinct()
                .collect(toImmutableList());
        if (deduplicatedSignatures.size() == 1) {
            return Optional.of(getOnlyElement(deduplicatedSignatures));
        }

        throw new PrestoException(AMBIGUOUS_FUNCTION_CALL, getErrorMessage(applicableFunctions));
    }

    private Optional<Signature> matchFunctionWithCoercion(Collection<? extends SqlFunction> candidates, List<TypeSignatureProvider> actualParameters)
    {
        return matchFunction(candidates, actualParameters, true);
    }

    private Optional<Signature> matchFunction(Collection<? extends SqlFunction> candidates, List<TypeSignatureProvider> parameters, boolean coercionAllowed)
    {
        List<ApplicableFunction> applicableFunctions = identifyApplicableFunctions(candidates, parameters, coercionAllowed);
        if (applicableFunctions.isEmpty()) {
            return Optional.empty();
        }

        if (coercionAllowed) {
            applicableFunctions = selectMostSpecificFunctions(applicableFunctions, parameters);
            checkState(!applicableFunctions.isEmpty(), "at least single function must be left");
        }

        if (applicableFunctions.size() == 1) {
            return Optional.of(getOnlyElement(applicableFunctions).getBoundSignature());
        }

        throw new PrestoException(AMBIGUOUS_FUNCTION_CALL, getErrorMessage(applicableFunctions));
    }

    private String getErrorMessage(List<ApplicableFunction> applicableFunctions)
    {
        StringBuilder errorMessageBuilder = new StringBuilder();
        errorMessageBuilder.append("Could not choose a best candidate operator. Explicit type casts must be added.\n");
        errorMessageBuilder.append("Candidates are:\n");
        for (ApplicableFunction function : applicableFunctions) {
            errorMessageBuilder.append("\t * ");
            errorMessageBuilder.append(function.getBoundSignature().toString());
            errorMessageBuilder.append("\n");
        }

        return errorMessageBuilder.toString();
    }

    private List<ApplicableFunction> identifyApplicableFunctions(Collection<? extends SqlFunction> candidates, List<TypeSignatureProvider> actualParameters, boolean allowCoercion)
    {
        ImmutableList.Builder<ApplicableFunction> applicableFunctions = ImmutableList.builder();
        ImmutableList.Builder<SemanticException> semanticExceptions = ImmutableList.builder();
        for (SqlFunction function : candidates) {
            Signature declaredSignature = function.getSignature();
            try {
                Optional<Signature> boundSignature = new SignatureBinder(functionAndTypeManager, declaredSignature, allowCoercion)
                        .bind(actualParameters);
                boundSignature.ifPresent(signature -> applicableFunctions.add(new ApplicableFunction(declaredSignature, signature, function.isCalledOnNullInput())));
            }
            catch (SemanticException e) {
                semanticExceptions.add(e);
            }
        }

        List<ApplicableFunction> applicableFunctionsList = applicableFunctions.build();
        List<SemanticException> semanticExceptionList = semanticExceptions.build();
        if (applicableFunctionsList.isEmpty() && !semanticExceptionList.isEmpty()) {
            decideAndThrow(semanticExceptionList,
                    candidates.stream().findFirst()
                            .map(function -> function.getSignature().getName().getObjectName())
                            .orElse(""));
        }
        return applicableFunctionsList;
    }

    private List<ApplicableFunction> selectMostSpecificFunctions(List<ApplicableFunction> applicableFunctions, List<TypeSignatureProvider> parameters)
    {
        checkArgument(!applicableFunctions.isEmpty());

        List<ApplicableFunction> mostSpecificFunctions = selectMostSpecificFunctions(applicableFunctions);
        if (mostSpecificFunctions.size() <= 1) {
            return mostSpecificFunctions;
        }

        Optional<List<Type>> optionalParameterTypes = toTypes(parameters);
        if (!optionalParameterTypes.isPresent()) {
            // give up and return all remaining matches
            return mostSpecificFunctions;
        }

        List<Type> parameterTypes = optionalParameterTypes.get();
        if (!someParameterIsUnknown(parameterTypes)) {
            // give up and return all remaining matches
            return mostSpecificFunctions;
        }

        // look for functions that only cast the unknown arguments
        List<ApplicableFunction> unknownOnlyCastFunctions = getUnknownOnlyCastFunctions(applicableFunctions, parameterTypes);
        if (!unknownOnlyCastFunctions.isEmpty()) {
            mostSpecificFunctions = unknownOnlyCastFunctions;
            if (mostSpecificFunctions.size() == 1) {
                return mostSpecificFunctions;
            }
        }

        // If the return type for all the selected function is the same, and the parameters are declared as RETURN_NULL_ON_NULL
        // all the functions are semantically the same. We can return just any of those.
        if (returnTypeIsTheSame(mostSpecificFunctions) && allReturnNullOnGivenInputTypes(mostSpecificFunctions, parameterTypes)) {
            // make it deterministic
            ApplicableFunction selectedFunction = Ordering.usingToString()
                    .reverse()
                    .sortedCopy(mostSpecificFunctions)
                    .get(0);
            return ImmutableList.of(selectedFunction);
        }

        return mostSpecificFunctions;
    }

    private List<ApplicableFunction> selectMostSpecificFunctions(List<ApplicableFunction> candidates)
    {
        List<ApplicableFunction> representatives = new ArrayList<>();

        for (ApplicableFunction current : candidates) {
            boolean found = false;
            for (int i = 0; i < representatives.size(); i++) {
                ApplicableFunction representative = representatives.get(i);
                if (isMoreSpecificThan(current, representative)) {
                    found = true;
                    representatives.set(i, current);
                }
                if (isMoreSpecificThan(representative, current)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                representatives.add(current);
            }
        }

        return representatives.stream()
                .distinct()
                .collect(toImmutableList());
    }

    private List<ApplicableFunction> getUnknownOnlyCastFunctions(List<ApplicableFunction> applicableFunction, List<Type> actualParameters)
    {
        return applicableFunction.stream()
                .filter((function) -> onlyCastsUnknown(function, actualParameters))
                .collect(toImmutableList());
    }

    private boolean onlyCastsUnknown(ApplicableFunction applicableFunction, List<Type> actualParameters)
    {
        List<Type> boundTypes = resolveTypes(applicableFunction.getBoundSignature().getArgumentTypes(), functionAndTypeManager);
        checkState(actualParameters.size() == boundTypes.size(), "type lists are of different lengths");
        for (int i = 0; i < actualParameters.size(); i++) {
            if (!boundTypes.get(i).equals(actualParameters.get(i)) && actualParameters.get(i) != UNKNOWN) {
                return false;
            }
        }
        return true;
    }

    private boolean returnTypeIsTheSame(List<ApplicableFunction> applicableFunctions)
    {
        Set<Type> returnTypes = applicableFunctions.stream()
                .map(function -> functionAndTypeManager.getType(function.getBoundSignature().getReturnType()))
                .collect(Collectors.toSet());
        return returnTypes.size() == 1;
    }

    /**
     * One method is more specific than another if invocation handled by the first method could be passed on to the other one
     */
    private boolean isMoreSpecificThan(ApplicableFunction left, ApplicableFunction right)
    {
        List<TypeSignatureProvider> resolvedTypes = fromTypeSignatures(left.getBoundSignature().getArgumentTypes());
        Optional<BoundVariables> boundVariables = new SignatureBinder(functionAndTypeManager, right.getDeclaredSignature(), true)
                .bindVariables(resolvedTypes);
        return boundVariables.isPresent();
    }

    private Optional<List<Type>> toTypes(List<TypeSignatureProvider> typeSignatureProviders)
    {
        ImmutableList.Builder<Type> resultBuilder = ImmutableList.builder();
        for (TypeSignatureProvider typeSignatureProvider : typeSignatureProviders) {
            if (typeSignatureProvider.hasDependency()) {
                return Optional.empty();
            }
            resultBuilder.add(functionAndTypeManager.getType(typeSignatureProvider.getTypeSignature()));
        }
        return Optional.of(resultBuilder.build());
    }

    private static boolean someParameterIsUnknown(List<Type> parameters)
    {
        return parameters.stream().anyMatch(type -> type.equals(UNKNOWN));
    }

    private static boolean allReturnNullOnGivenInputTypes(List<ApplicableFunction> applicableFunctions, List<Type> parameters)
    {
        return applicableFunctions.stream().allMatch(x -> returnsNullOnGivenInputTypes(x, parameters));
    }

    private static boolean returnsNullOnGivenInputTypes(ApplicableFunction applicableFunction, List<Type> parameterTypes)
    {
        Signature boundSignature = applicableFunction.getBoundSignature();
        FunctionKind functionKind = boundSignature.getKind();
        // Window and Aggregation functions have fixed semantic where NULL values are always skipped
        if (functionKind != SCALAR) {
            return true;
        }

        for (int i = 0; i < parameterTypes.size(); i++) {
            Type parameterType = parameterTypes.get(i);
            if (parameterType.equals(UNKNOWN)) {
                // The original implementation checks only whether the particular argument has @SqlNullable.
                // However, RETURNS NULL ON NULL INPUT / CALLED ON NULL INPUT is a function level metadata according
                // to SQL spec. So there is a loss of precision here.
                if (applicableFunction.isCalledOnNullInput()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Decides which exception to throw based on the number of failed attempts.
     * If there's only one SemanticException, it throws that SemanticException directly.
     * If there are multiple SemanticExceptions, it throws the SignatureMatchingException.
     */
    private static void decideAndThrow(List<SemanticException> failedExceptions, String functionName)
            throws SemanticException
    {
        if (failedExceptions.size() == 1) {
            throw failedExceptions.get(0);
        }
        else {
            throw new SignatureMatchingException(format("Failed to find matching function signature for %s, matching failures: ", functionName), failedExceptions);
        }
    }

    static String constructFunctionNotFoundErrorMessage(QualifiedObjectName functionName, List<TypeSignatureProvider> parameterTypes, Collection<? extends SqlFunction> candidates)
    {
        String name = toConciseFunctionName(functionName);
        List<String> expectedParameters = new ArrayList<>();
        for (SqlFunction function : candidates) {
            expectedParameters.add(format("%s(%s) %s",
                    name,
                    Joiner.on(", ").join(function.getSignature().getArgumentTypes()),
                    Joiner.on(", ").join(function.getSignature().getTypeVariableConstraints())));
        }
        String parameters = Joiner.on(", ").join(parameterTypes);
        String message = format("Function %s not registered", name);
        if (!expectedParameters.isEmpty()) {
            String expected = Joiner.on(", ").join(expectedParameters);
            message = format("Unexpected parameters (%s) for function %s. Expected: %s", parameters, name, expected);
        }
        return message;
    }

    private static String toConciseFunctionName(QualifiedObjectName functionName)
    {
        if (functionName.getCatalogSchemaName().equals(JAVA_BUILTIN_NAMESPACE)) {
            return functionName.getObjectName();
        }
        return functionName.toString();
    }

    private static class ApplicableFunction
    {
        private final Signature declaredSignature;
        private final Signature boundSignature;
        private final boolean calledOnNullInput;

        private ApplicableFunction(Signature declaredSignature, Signature boundSignature, boolean calledOnNullInput)
        {
            this.declaredSignature = declaredSignature;
            this.boundSignature = boundSignature;
            this.calledOnNullInput = calledOnNullInput;
        }

        public Signature getDeclaredSignature()
        {
            return declaredSignature;
        }

        public Signature getBoundSignature()
        {
            return boundSignature;
        }

        public boolean isCalledOnNullInput()
        {
            return calledOnNullInput;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("declaredSignature", declaredSignature)
                    .add("boundSignature", boundSignature)
                    .add("calledOnNullInput", calledOnNullInput)
                    .toString();
        }
    }
}
