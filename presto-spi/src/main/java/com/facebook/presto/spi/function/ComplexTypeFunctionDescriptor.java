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
package com.facebook.presto.spi.function;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.TypeSignature;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

/**
 * Contains properties that describe how the function operates on Map or Array inputs.
 */
public class ComplexTypeFunctionDescriptor
{
    public static final List<String> MAP_AND_ARRAY = unmodifiableList(Arrays.asList("map", "array"));
    public static final ComplexTypeFunctionDescriptor DEFAULT = new ComplexTypeFunctionDescriptor(
            true,
            emptyList(),
            Optional.of(emptySet()),
            Optional.of(SubfieldPathTransformationFunctions::allSubfieldsRequired));

    /**
     * Indicates whether the function accessing subfields.
     */
    private final boolean isAccessingInputValues;

    /**
     * Set of indices of the function arguments containing map or array arguments. Those arguments are important because all accessed subfields collected so far relate only to
     * those map or array arguments and will be passed only to those arguments during the expression analysis phase.
     * If <code>argumentIndicesContainingMapOrArray</code> is <code>Optional.empty()</code>, it indicates that accessed subfields collected so far relate to all function arguments
     * are of the map or array types. For the vast majority of function, this value should be used.
     * If the value of <code>argumentIndicesContainingMapOrArray</code> is present, it indicates that accessed subfields collected so far relate only to subset of the arguments.
     * For example, in <code>MapConstructor</code> function accessed map value subfield from outer call relate only to second argument and therefore for this
     * <code>argumentIndicesContainingMapOrArray</code> needs to be set to <code>Optional.of(ImmutableSet.of(1))</code>.
     */
    private final Optional<Set<Integer>> argumentIndicesContainingMapOrArray;

    /**
     * Contains the transformation function to convert the output back to the input elements of the array or map.
     * If <code>outputToInputTransformationFunction</code> is <code>Optional.empty()</code>, it indicates that transformation is not required and equivalent to the identity function
     */
    private final Optional<Function<Set<Subfield>, Set<Subfield>>> outputToInputTransformationFunction;

    /**
     * Contains the description of all lambdas that this function accepts.
     * If function does not accept any lambda parameter, then <code>lambdaDescriptors</code> should be an empty list.
     */
    private final List<LambdaDescriptor> lambdaDescriptors;

    public ComplexTypeFunctionDescriptor(
            boolean isAccessingInputValues,
            List<LambdaDescriptor> lambdaDescriptors,
            Optional<Set<Integer>> argumentIndicesContainingMapOrArray,
            Optional<Function<Set<Subfield>, Set<Subfield>>> outputToInputTransformationFunction,
            Signature signature)
    {
        this(isAccessingInputValues, lambdaDescriptors, argumentIndicesContainingMapOrArray, outputToInputTransformationFunction, signature.getArgumentTypes());
    }
    public ComplexTypeFunctionDescriptor(
            boolean isAccessingInputValues,
            List<LambdaDescriptor> lambdaDescriptors,
            Optional<Set<Integer>> argumentIndicesContainingMapOrArray,
            Optional<Function<Set<Subfield>, Set<Subfield>>> outputToInputTransformationFunction,
            List<TypeSignature> argumentTypes)
    {
        this(isAccessingInputValues, lambdaDescriptors, argumentIndicesContainingMapOrArray, outputToInputTransformationFunction);
        if (argumentIndicesContainingMapOrArray.isPresent()) {
            checkArgument(argumentIndicesContainingMapOrArray.get().stream().allMatch(index -> index >= 0 &&
                    index < argumentTypes.size() &&
                    MAP_AND_ARRAY.contains(argumentTypes.get(index).getBase().toLowerCase(Locale.ENGLISH))));
        }
        for (LambdaDescriptor lambdaDescriptor : lambdaDescriptors) {
            checkArgument(lambdaDescriptor.getCallArgumentIndex() >= 0 && argumentTypes.get(lambdaDescriptor.getCallArgumentIndex()).isFunction());
            checkArgument(lambdaDescriptor.getLambdaArgumentDescriptors().keySet().stream().allMatch(
                    argumentIndex -> argumentIndex >= 0 && argumentIndex < argumentTypes.size()));
            for (Integer lambdaArgumentIndex : lambdaDescriptor.getLambdaArgumentDescriptors().keySet()) {
                checkArgument(lambdaArgumentIndex >= 0 &&
                        lambdaArgumentIndex < argumentTypes.get(lambdaDescriptor.getCallArgumentIndex()).getParameters().size() - 1);
                LambdaArgumentDescriptor lambdaArgumentDescriptor = lambdaDescriptor.getLambdaArgumentDescriptors().get(lambdaArgumentIndex);
                checkArgument(lambdaArgumentDescriptor.getCallArgumentIndex() >= 0 &&
                        lambdaArgumentDescriptor.getCallArgumentIndex() < argumentTypes.size());
            }
        }
    }

    public ComplexTypeFunctionDescriptor(
            boolean isAccessingInputValues,
            List<LambdaDescriptor> lambdaDescriptors,
            Optional<Set<Integer>> argumentIndicesContainingMapOrArray,
            Optional<Function<Set<Subfield>, Set<Subfield>>> outputToInputTransformationFunction)
    {
        requireNonNull(argumentIndicesContainingMapOrArray, "argumentIndicesContainingMapOrArray is null");
        this.isAccessingInputValues = isAccessingInputValues;
        this.lambdaDescriptors = unmodifiableList(requireNonNull(lambdaDescriptors, "lambdaDescriptors is null"));
        this.argumentIndicesContainingMapOrArray = argumentIndicesContainingMapOrArray.isPresent() ?
                Optional.of(unmodifiableSet(argumentIndicesContainingMapOrArray.get())) :
                Optional.empty();
        this.outputToInputTransformationFunction = requireNonNull(outputToInputTransformationFunction, "outputToInputTransformationFunction is null");
    }

    public static ComplexTypeFunctionDescriptor defaultFunctionDescriptor()
    {
        return DEFAULT;
    }

    public boolean isAccessingInputValues()
    {
        return isAccessingInputValues;
    }

    public Optional<Set<Integer>> getArgumentIndicesContainingMapOrArray()
    {
        return argumentIndicesContainingMapOrArray;
    }

    public List<LambdaDescriptor> getLambdaDescriptors()
    {
        return lambdaDescriptors;
    }

    public boolean isAcceptingLambdaArgument()
    {
        return !lambdaDescriptors.isEmpty();
    }

    public Optional<Function<Set<Subfield>, Set<Subfield>>> getOutputToInputTransformationFunction()
    {
        return outputToInputTransformationFunction;
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
        ComplexTypeFunctionDescriptor that = (ComplexTypeFunctionDescriptor) o;
        return isAccessingInputValues == that.isAccessingInputValues &&
                Objects.equals(argumentIndicesContainingMapOrArray, that.argumentIndicesContainingMapOrArray) &&
                Objects.equals(outputToInputTransformationFunction, that.outputToInputTransformationFunction) &&
                Objects.equals(lambdaDescriptors, that.lambdaDescriptors);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(isAccessingInputValues, argumentIndicesContainingMapOrArray, outputToInputTransformationFunction, lambdaDescriptors);
    }
}
