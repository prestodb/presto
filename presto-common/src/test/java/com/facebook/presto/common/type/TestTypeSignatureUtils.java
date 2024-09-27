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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeSignatureUtils.resolveIntermediateType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTypeSignatureUtils
{
    @Test
    public void resolveIntermediateTypeWithNonGenericBaseAndNoParams()
    {
        TypeSignature expectedIntermediateType = parseTypeSignature("bigint");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(parseTypeSignature("bigint"), ImmutableList.of(), ImmutableList.of());
        assertEquals(resolvedIntermediateType.getTypeSignatureBase(), expectedIntermediateType.getTypeSignatureBase());
        assertEquals(resolvedIntermediateType.getParameters().size(), expectedIntermediateType.getParameters().size());
    }

    @Test
    public void resolveIntermediateTypeWithGenericBaseAndNoParams()
    {
        TypeSignature expectedIntermediateType = parseTypeSignature("bigint");
        TypeSignature intermediateType = parseTypeSignature("T");
        TypeSignature resolvedIntermediateType = resolveIntermediateType(
                intermediateType,
                ImmutableList.of(intermediateType),
                ImmutableList.of(expectedIntermediateType));
        assertEquals(resolvedIntermediateType.getTypeSignatureBase(), expectedIntermediateType.getTypeSignatureBase());
        assertEquals(resolvedIntermediateType.getParameters().size(), expectedIntermediateType.getParameters().size());
    }

    @Test
    public void resolveIntermediateTypeWithGenericParams()
    {
        TypeSignature expectedIntermediateType =
                parseTypeSignature("row(array(bigint),boolean,double,integer,varchar(100),array(bigint),array(integer))");
        TypeSignature intermediateType =
                parseTypeSignature("row(array(T),boolean,double,integer,E,array(T),array(integer))");

        TypeSignature resolvedIntermediateType =
                resolveIntermediateType(
                        intermediateType,
                        ImmutableList.of(
                                parseTypeSignature("T"),
                                parseTypeSignature("E"),
                                parseTypeSignature("boolean"),
                                parseTypeSignature("integer")),
                        ImmutableList.of(
                                parseTypeSignature("bigint"),
                                parseTypeSignature("varchar(100)"),
                                parseTypeSignature("boolean"),
                                parseTypeSignature("integer")));

        assertEquals(resolvedIntermediateType.getTypeSignatureBase(), expectedIntermediateType.getTypeSignatureBase());
        assertTrue(verifyMatchingIntermediateTypes(
                resolvedIntermediateType.getParameters(),
                expectedIntermediateType.getParameters()));
    }

    private static boolean verifyMatchingIntermediateTypes(
            List<TypeSignatureParameter> argumentTypes,
            List<TypeSignatureParameter> parameters)
    {
        if (argumentTypes.size() != parameters.size()) {
            return false;
        }
        for (int i = 0; i < argumentTypes.size(); i++) {
            TypeSignatureParameter argumentType = argumentTypes.get(i);
            TypeSignatureParameter parameter = parameters.get(i);
            if (argumentType.getKind() != parameter.getKind()) {
                return false;
            }
            TypeSignature expectedTypeSignature = parameter.getTypeSignatureOrNamedTypeSignature().orElseThrow(() ->
                    new IllegalStateException("Could not get type signature for type parameter [" + parameter + "]"));
            TypeSignature resolvedTypeSignature = argumentType.getTypeSignatureOrNamedTypeSignature().orElseThrow(() ->
                    new IllegalStateException("Could not get type signature for type parameter [" + argumentType + "]"));
            // Todo: hack for varchar
            if (expectedTypeSignature.getParameters().isEmpty() || expectedTypeSignature.getBase().equals("varchar")) {
                if (!expectedTypeSignature.getBase().equals(resolvedTypeSignature.getBase())) {
                    return false;
                }
            }
            else {
                return verifyMatchingIntermediateTypes(resolvedTypeSignature.getParameters(), expectedTypeSignature.getParameters());
            }
        }
        return true;
    }
}
