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
package com.facebook.presto.operator.scalar.annotations;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ComplexTypeFunctionDescriptor;
import com.facebook.presto.spi.function.LambdaArgumentDescriptor;
import com.facebook.presto.spi.function.LambdaDescriptor;
import com.facebook.presto.spi.function.ScalarFunctionDescriptor;
import com.facebook.presto.spi.function.ScalarFunctionLambdaArgumentDescriptor;
import com.facebook.presto.spi.function.ScalarFunctionLambdaDescriptor;
import com.facebook.presto.spi.function.SubfieldPathTransformationFunctions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class FunctionDescriptorParser
{
    private FunctionDescriptorParser() {}

    public static ComplexTypeFunctionDescriptor parseFunctionDescriptor(ScalarFunctionDescriptor descriptor)
    {
        return new ComplexTypeFunctionDescriptor(
                descriptor.isAccessingInputValues(),
                parseLambdaDescriptors(descriptor.lambdaDescriptors()),
                descriptor.argumentIndicesContainingMapOrArray().length == 1 ?
                        Optional.of(ImmutableSet.copyOf(Arrays.stream(descriptor.argumentIndicesContainingMapOrArray()[0].value()).iterator())) : Optional.empty(),
                descriptor.outputToInputTransformationFunction().equals("identity") ?
                        Optional.empty() :
                        Optional.of(parseSubfieldTransformationFunction(descriptor.outputToInputTransformationFunction())));
    }

    private static Function<Set<Subfield>, Set<Subfield>> parseSubfieldTransformationFunction(String subfieldPathTransformationFunction)
    {
        Method subfieldTransformationMethod;
        try {
            subfieldTransformationMethod = SubfieldPathTransformationFunctions.class.getDeclaredMethod(subfieldPathTransformationFunction, Set.class);
        }
        catch (NoSuchMethodException cause) {
            throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR,
                    format("Could not find subfield transformation function '%s'", subfieldPathTransformationFunction), cause);
        }
        checkSubfieldTransformFunctionTypeSignature(subfieldTransformationMethod);

        return (Set<Subfield> subfields) -> {
            try {
                return (Set<Subfield>) subfieldTransformationMethod.invoke(null, subfields);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                return SubfieldPathTransformationFunctions.allSubfieldsRequired(subfields);
            }
        };
    }

    private static void checkSubfieldTransformFunctionTypeSignature(Method subfieldTransformationMethod)
    {
        {
            String errorMessage = "Subfield transformation function must accept a single parameter of type java.util.Set<com.facebook.presto.common.Subfield>";
            Type[] inputTypes = subfieldTransformationMethod.getGenericParameterTypes();
            checkArgument(inputTypes.length == 1, errorMessage);
            checkTypeIsSetOfSubfields(inputTypes[0], errorMessage);
        }
        {
            String errorMessage = "Subfield transformation function return type must be java.util.Set<com.facebook.presto.common.Subfield>";
            Type type = subfieldTransformationMethod.getGenericReturnType();
            checkTypeIsSetOfSubfields(type, errorMessage);
        }
    }

    private static void checkTypeIsSetOfSubfields(Type type, String errorMessage)
    {
        final ParameterizedType setType = (ParameterizedType) type;
        checkArgument(setType.getRawType().equals(Set.class), errorMessage);
        checkArgument(setType.getActualTypeArguments()[0].equals(Subfield.class), errorMessage);
    }

    private static List<LambdaDescriptor> parseLambdaDescriptors(ScalarFunctionLambdaDescriptor[] lambdaDescriptors)
    {
        ImmutableList.Builder<LambdaDescriptor> lambdaDescriptorBuilder = ImmutableList.builder();
        for (ScalarFunctionLambdaDescriptor lambdaDescriptor : lambdaDescriptors) {
            lambdaDescriptorBuilder.add(new LambdaDescriptor(parseLambdaArgumentDescriptors(lambdaDescriptor.lambdaArgumentDescriptors())));
        }
        return lambdaDescriptorBuilder.build();
    }

    private static Map<Integer, LambdaArgumentDescriptor> parseLambdaArgumentDescriptors(ScalarFunctionLambdaArgumentDescriptor[] lambdaArgumentToCallArgumentIndexMapEntries)
    {
        ImmutableMap.Builder<Integer, LambdaArgumentDescriptor> lambdaArgumentToCallArgumentIndexMap = ImmutableMap.builder();
        for (int i = 0; i < lambdaArgumentToCallArgumentIndexMapEntries.length; ++i) {
            if (lambdaArgumentToCallArgumentIndexMapEntries[i].callArgumentIndex() == -1) {
                continue;
            }
            ScalarFunctionLambdaArgumentDescriptor entry = lambdaArgumentToCallArgumentIndexMapEntries[i];
            lambdaArgumentToCallArgumentIndexMap.put(i,
                    new LambdaArgumentDescriptor(entry.callArgumentIndex(), parseSubfieldTransformationFunction(entry.lambdaArgumentToInputTransformationFunction())));
        }
        return lambdaArgumentToCallArgumentIndexMap.build();
    }
}
