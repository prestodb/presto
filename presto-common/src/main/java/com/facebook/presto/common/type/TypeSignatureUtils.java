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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class TypeSignatureUtils
{
    private TypeSignatureUtils() {}

    public static TypeSignature resolveIntermediateType(TypeSignature typeSignature, List<TypeSignature> parameters, List<TypeSignature> argumentTypes)
    {
        Map<TypeSignature, TypeSignature> typeSignatureMap = getTypeSignatureMap(parameters, argumentTypes);
        return resolveTypeSignatures(typeSignature, typeSignatureMap).getTypeSignature();
    }

    // todo: Change to ImmutableList when open sourcing

    private static NamedTypeSignature resolveTypeSignatures(TypeSignature typeSignature, Map<TypeSignature, TypeSignature> typeSignatureMap)
    {
        TypeSignature resolvedTypeSignature = typeSignatureMap.getOrDefault(typeSignature, typeSignature);
        List<NamedTypeSignature> namedTypeSignatures = new ArrayList<>();
        List<TypeSignature> typeSignatures = new ArrayList<>();
        List<TypeSignatureParameter> typeSignaturesList = typeSignature.getParameters();
        for (TypeSignatureParameter typeSignatureParameter : typeSignaturesList) {
            TypeSignature typeSignatureOrNamedTypeSignature = typeSignatureParameter.getTypeSignatureOrNamedTypeSignature().orElseThrow(() ->
                    new IllegalStateException("Could not get type signature for type parameter [" + typeSignatureParameter + "]"));
            TypeSignature resolvedTypeParameterSignature = typeSignatureMap.getOrDefault(typeSignatureOrNamedTypeSignature, typeSignatureOrNamedTypeSignature);
            if (resolvedTypeSignature.getBase().equals("row")) {
                if (!typeSignatureOrNamedTypeSignature.getParameters().isEmpty()) {
                    namedTypeSignatures.add(resolveTypeSignatures(resolvedTypeParameterSignature, typeSignatureMap));
                }
                else {
                    namedTypeSignatures.add(new NamedTypeSignature(Optional.empty(), new TypeSignature(resolvedTypeParameterSignature.getBase(), Collections.emptyList())));
                }
            }
            else {
                if (!typeSignatureOrNamedTypeSignature.getParameters().isEmpty()) {
                    typeSignatures.add(resolveTypeSignatures(resolvedTypeParameterSignature, typeSignatureMap).getTypeSignature());
                }
                else {
                    typeSignatures.add(new TypeSignature(resolvedTypeParameterSignature.getBase(), Collections.emptyList()));
                }
            }
        }
        return new NamedTypeSignature(Optional.empty(), new TypeSignature(resolvedTypeSignature.getBase(),
                (typeSignatures.isEmpty() ? namedTypeSignatures : typeSignatures).stream().map(
                        signature -> signature instanceof NamedTypeSignature ?
                                TypeSignatureParameter.of((NamedTypeSignature) signature)
                                : TypeSignatureParameter.of((TypeSignature) signature)).collect(Collectors.toList())));
    }

    /**
     * Parameter and argument type mapping must be consistent
     */

    public static Map<TypeSignature, TypeSignature> getTypeSignatureMap(List<TypeSignature> parameters, List<TypeSignature> argumentTypes)
    {
        HashMap<TypeSignature, TypeSignature> typeSignatureMap = new HashMap<>();
        if (argumentTypes.size() != parameters.size()) {
            throw new IllegalStateException("Parameters size and argumentTypes size do not match!");
        }
        for (int i = 0; i < argumentTypes.size(); i++) {
            TypeSignature parameter = parameters.get(i);
            TypeSignature argumentType = argumentTypes.get(i);
            if (argumentTypes.get(i).getParameters().isEmpty()) {
                typeSignatureMap.put(parameter, argumentType);
            }
            else {
                typeSignatureMap.putAll(getTypeSignatureMap(parameter.getTypeParametersAsTypeSignatures(), argumentType.getTypeParametersAsTypeSignatures()));
            }
        }
        return typeSignatureMap;
    }
}
