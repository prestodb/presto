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
package com.facebook.presto.common.type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;

public final class TypeSignatureUtils
{
    private TypeSignatureUtils() {}

    public static TypeSignature resolveIntermediateType(TypeSignature actualTypeSignature, List<TypeSignature> actualTypeSignatureParameters, List<TypeSignature> expectedTypeSignatureParameters)
    {
        Map<TypeSignature, TypeSignature> typeSignatureMap = getTypeSignatureMap(actualTypeSignatureParameters, expectedTypeSignatureParameters);
        return resolveTypeSignatures(actualTypeSignature, typeSignatureMap).getTypeSignature();
    }

    private static Map<TypeSignature, TypeSignature> getTypeSignatureMap(List<TypeSignature> parameters, List<TypeSignature> argumentTypes)
    {
        Map<TypeSignature, TypeSignature> typeSignatureMap = new HashMap<>();
        if (argumentTypes.size() != parameters.size()) {
            throw new IllegalStateException(
                    "Parameters size: " + parameters.size() + " and argumentTypes size: " + argumentTypes.size() + " do not match !");
        }

        for (int i = 0; i < argumentTypes.size(); i++) {
            TypeSignature parameter = parameters.get(i);
            TypeSignature argumentType = argumentTypes.get(i);

            // Realistically, there are only two cases,
            // 1. When parameter.getParameters() is empty :
            //    - Eg: parameter type = generic type(T) and argumentType = array(double), we can directly put
            //          map.put(parameter, argumentType) there is no need to loop over the argumentType.
            //    - Eg: param type = non-generic type(bigint) , in this case argumentType will also be of the
            //          same non-generic type hence  map.put(parameter, argumentType) is valid here too.
            // 2. When parameter.getParameters() is not empty:
            //    - Eg: parameter type = generic type (array(T)) and argumentType = array(double), we recursively run
            //      this function until we reach condition 1.
            //      Example calls for parameter type = generic type (array(T)) and argumentType = array(double)
            //         Iteration 1:
            //            Parameter:
            //              Type signature base = array
            //              parameters = T
            //            ArgumentType :
            //              Type signature base = array
            //              parameters = double
            //         Iteration 2:
            //            Parameter:
            //              Type signature base = T
            //              parameters = empty
            //            ArgumentType :
            //              Type signature base = double
            //              parameters = empty
            //          return typeSignatureMap = {"T": "double"}

            // If parameter params are of type long e.g decimal(15, 2) or
            // of type varchar e.g decimal(i4, i5), we don't need to recursively call the function on its params
            if (parameter.getParameters().isEmpty() || !(areParametersTypeSignatureOrNamedTypedSignature(parameter.getParameters()))) {
                typeSignatureMap.put(parameter, argumentType);
            }
            else {
                typeSignatureMap.putAll(getTypeSignatureMap(
                        parameter.getTypeOrNamedTypeParametersAsTypeSignatures(),
                        argumentType.getTypeOrNamedTypeParametersAsTypeSignatures()));
            }
        }
        return typeSignatureMap;
    }

    // A utility function to resolve intermediate type signatures.
    // Realistically, these are the different cases that we can face:
    // 1. If there are no params/argTypes in the first call itself, return the resolvedType from the map directly.
    // 2. If params != empty, we loop over the params:
    //    - Check whether the param is present in the typeSignatureMap, if its present,
    //      add the resolvedTypeParameterSignature mapping as a param directly and continue to the next param.
    //      - The idea behind this logic is that if param type is present in the typeSignatureMap, it means that for
    //        that particular param, we could just resolve the type from the map and no need to recursively call as its completely resolved.
    //      - Eg: param : T , map : {"T":"array(double)"}.
    //    - If the mapping isn't present, we recursively call the resolveTypeSignatures() again.
    //      - Example calls for param: array(T) , map : {"T" : "array(double)"}
    //           resolvedIntermediateType = null
    //           Iteration 1:
    //               Parameter:
    //                  key to lookup in map: array(T), key found : false
    //           Iteration 2:
    //               Parameter:
    //                  key to lookup in map: T, key found : true
    //                  resolvedIntermediateType = array(double)
    //           return resolvedIntermediateType = array(array(double))
    //
    //
    private static NamedTypeSignature resolveTypeSignatures(TypeSignature typeSignature, Map<TypeSignature, TypeSignature> typeSignatureMap)
    {
        if (typeSignatureMap.containsKey(typeSignature)) {
            TypeSignature resolvedTypeSignature = typeSignatureMap.get(typeSignature);
            return new NamedTypeSignature(Optional.empty(), resolvedTypeSignature);
        }
        List<NamedTypeSignature> namedTypeSignatures = new ArrayList<>();
        List<TypeSignature> typeSignatures = new ArrayList<>();
        for (TypeSignature typeParameterSignature : typeSignature.getTypeOrNamedTypeParametersAsTypeSignatures()) {
            // if base is "row" typeSignature, all typeParameterSignatures need to be of type NamedTypeSignature.
            boolean isRowTypeSignatureBase = typeSignature.getBase().equals("row");
            if (typeSignatureMap.containsKey(typeParameterSignature)) {
                TypeSignature resolvedTypeParameterSignature = typeSignatureMap.get(typeParameterSignature);
                if (isRowTypeSignatureBase) {
                    namedTypeSignatures.add(new NamedTypeSignature(Optional.empty(), resolvedTypeParameterSignature));
                }
                else {
                    typeSignatures.add(resolvedTypeParameterSignature);
                }
            }
            else {
                NamedTypeSignature namedTypeSignature = resolveTypeSignatures(typeParameterSignature, typeSignatureMap);
                if (isRowTypeSignatureBase) {
                    namedTypeSignatures.add(namedTypeSignature);
                }
                else {
                    typeSignatures.add(namedTypeSignature.getTypeSignature());
                }
            }
        }

        List<TypeSignatureParameter> parameters;
        if (!typeSignatures.isEmpty()) {
            parameters = typeSignatures.stream().map(TypeSignatureParameter::of).collect(Collectors.toList());
        }
        else {
            parameters = namedTypeSignatures.stream().map(TypeSignatureParameter::of).collect(Collectors.toList());
        }

        return new NamedTypeSignature(Optional.empty(), new TypeSignature(typeSignature.getBase(), unmodifiableList(parameters)));
    }

    private static boolean areParametersTypeSignatureOrNamedTypedSignature(List<TypeSignatureParameter> parameters)
    {
        return !parameters.isEmpty() && parameters.stream()
                .map(TypeSignatureParameter::getKind)
                .allMatch(parameterKind ->
                        parameterKind.equals(ParameterKind.NAMED_TYPE) || parameterKind.equals(ParameterKind.TYPE));
    }
}
