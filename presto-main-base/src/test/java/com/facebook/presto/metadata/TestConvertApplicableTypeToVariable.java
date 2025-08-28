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

import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.builtin.tools.WorkerFunctionUtil.convertApplicableTypeToVariable;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestConvertApplicableTypeToVariable
{
    @Test
    public void testConvertApplicableTypeToVariableDecimalType()
    {
        TypeSignature actualTypeSignature = new TypeSignature(
                "decimal",
                TypeSignatureParameter.of(parseTypeSignature("i4")),
                TypeSignatureParameter.of(parseTypeSignature("i5")));
        TypeSignature expectedTypeSignature = new TypeSignature(
                "decimal",
                TypeSignatureParameter.of("i4"),
                TypeSignatureParameter.of("i5"));
        for (int i = 0; i < actualTypeSignature.getParameters().size(); i++) {
            assertTrue(actualTypeSignature.getParameters().get(i).isTypeSignature());
            assertTrue(expectedTypeSignature.getParameters().get(i).isVariable());
        }
        TypeSignature resolvedTypeSignature = convertApplicableTypeToVariable(actualTypeSignature);
        assertEquals(expectedTypeSignature, resolvedTypeSignature);
    }

    @Test
    public void testConvertApplicableTypeToVariableDecimalTypeSignature()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("decimal(i4, i5)");
        TypeSignature expectedTypeSignature = new TypeSignature(
                "decimal",
                TypeSignatureParameter.of("i4"),
                TypeSignatureParameter.of("i5"));
        TypeSignature resolvedTypeSignature = convertApplicableTypeToVariable(actualTypeSignature);
        assertEquals(expectedTypeSignature, resolvedTypeSignature);
    }

    @Test
    public void testConvertApplicableTypeToVariableVarcharType()
    {
        TypeSignature actualTypeSignature = new TypeSignature(
                "varchar",
                TypeSignatureParameter.of(50)); // a random number
        TypeSignature expectedTypeSignature = new TypeSignature(
                "varchar",
                TypeSignatureParameter.of(50));
        TypeSignature resolvedTypeSignature = convertApplicableTypeToVariable(actualTypeSignature);
        assertEquals(expectedTypeSignature, resolvedTypeSignature);
    }

    @Test
    public void testConvertApplicableTypeToVariableBigintType()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("bigint");
        TypeSignature expectedTypeSignature = parseTypeSignature("bigint");
        TypeSignature resolvedTypeSignature = convertApplicableTypeToVariable(actualTypeSignature);
        assertEquals(expectedTypeSignature, resolvedTypeSignature);
    }

    @Test
    public void testConvertApplicableTypeToVariableBigintArgType()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("test(bigint)");
        TypeSignature expectedTypeSignature = parseTypeSignature("test(bigint)");
        List<TypeSignature> resolvedTypeSignaturesList =
                convertApplicableTypeToVariable(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), resolvedTypeSignaturesList);
    }

    @Test
    public void testConvertApplicableTypeToVariableArrayArgType()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("test(array(decimal(i4, i5)))");
        TypeSignature expectedTypeSignature = new TypeSignature(
                "test",
                TypeSignatureParameter.of(
                        new TypeSignature(
                                "array",
                                TypeSignatureParameter.of(
                                        new TypeSignature(
                                                "decimal",
                                                TypeSignatureParameter.of("i4"),
                                                TypeSignatureParameter.of("i5"))))));
        List<TypeSignature> resolvedTypeSignaturesList =
                convertApplicableTypeToVariable(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), resolvedTypeSignaturesList);
    }

    @Test
    public void testConvertApplicableTypeToVariableArrayConstructor()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("array_constructor(T)");
        TypeSignature expectedTypeSignature = parseTypeSignature("array_constructor(T)");
        TypeSignature resolvedTypeSignature = convertApplicableTypeToVariable(actualTypeSignature);
        assertEquals(expectedTypeSignature, resolvedTypeSignature);
    }

    @Test
    public void testConvertApplicableTypeToVariableMapArgType()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("test(map(varchar, decimal(i4, i5)))");
        TypeSignature expectedTypeSignature = new TypeSignature(
                "test",
                TypeSignatureParameter.of(
                        new TypeSignature(
                                "map",
                                TypeSignatureParameter.of(parseTypeSignature("varchar")),
                                TypeSignatureParameter.of(
                                        new TypeSignature(
                                                "decimal",
                                                TypeSignatureParameter.of("i4"),
                                                TypeSignatureParameter.of("i5"))))));
        List<TypeSignature> resolvedTypeSignaturesList =
                convertApplicableTypeToVariable(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), resolvedTypeSignaturesList);
    }

    @Test
    public void testConvertApplicableTypeToVariableRowType()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("test(row(varchar, decimal(i4, i5)))");
        TypeSignature expectedTypeSignature = new TypeSignature(
                "test",
                TypeSignatureParameter.of(
                        new TypeSignature(
                                "row",
                                TypeSignatureParameter.of(
                                        new NamedTypeSignature(
                                                Optional.empty(),
                                                parseTypeSignature("varchar"))),
                                TypeSignatureParameter.of(
                                        new NamedTypeSignature(
                                                Optional.empty(),
                                                new TypeSignature(
                                                        "decimal",
                                                        TypeSignatureParameter.of("i4"),
                                                        TypeSignatureParameter.of("i5")))))));
        List<TypeSignature> resolvedTypeSignaturesList =
                convertApplicableTypeToVariable(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), resolvedTypeSignaturesList);
    }

    @Test
    public void testConvertApplicableTypeToVariableRowTypeRowArgType()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("test(row(varchar, decimal(i4, i5), row(bigint, decimal(i4, i5))))");
        TypeSignature expectedTypeSignature = new TypeSignature(
                "test",
                TypeSignatureParameter.of(
                        new TypeSignature(
                                "row",
                                TypeSignatureParameter.of(
                                        new NamedTypeSignature(
                                                Optional.empty(),
                                                parseTypeSignature("varchar"))),
                                TypeSignatureParameter.of(
                                        new NamedTypeSignature(
                                                Optional.empty(),
                                                new TypeSignature(
                                                        "decimal",
                                                        TypeSignatureParameter.of("i4"),
                                                        TypeSignatureParameter.of("i5")))),
                                TypeSignatureParameter.of(
                                        new NamedTypeSignature(
                                                Optional.empty(),
                                                new TypeSignature(
                                                        "row",
                                                        TypeSignatureParameter.of(
                                                                new NamedTypeSignature(
                                                                        Optional.empty(),
                                                                        parseTypeSignature("bigint"))),
                                                        TypeSignatureParameter.of(
                                                                new NamedTypeSignature(
                                                                        Optional.empty(),
                                                                        new TypeSignature(
                                                                                "decimal",
                                                                                TypeSignatureParameter.of("i4"),
                                                                                TypeSignatureParameter.of("i5"))))))))));
        List<TypeSignature> resolvedTypeSignaturesList =
                convertApplicableTypeToVariable(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), resolvedTypeSignaturesList);
    }

    @Test
    public void testConvertApplicableTypeToVariableRowTypeRowComplexArgType()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("row(array(decimal(i4, i5)), decimal(i4, i5), row(bigint, varchar))");
        TypeSignature expectedTypeSignature = new TypeSignature(
                "row",
                TypeSignatureParameter.of(
                        new NamedTypeSignature(
                                Optional.empty(),
                                new TypeSignature(
                                        "array",
                                        TypeSignatureParameter.of(
                                                new TypeSignature(
                                                        "decimal",
                                                        TypeSignatureParameter.of("i4"),
                                                        TypeSignatureParameter.of("i5")))))),
                TypeSignatureParameter.of(
                        new NamedTypeSignature(
                                Optional.empty(),
                                new TypeSignature(
                                        "decimal",
                                        TypeSignatureParameter.of("i4"),
                                        TypeSignatureParameter.of("i5")))),
                TypeSignatureParameter.of(
                        new NamedTypeSignature(
                                Optional.empty(),
                                new TypeSignature(
                                        "row",
                                        TypeSignatureParameter.of(
                                                new NamedTypeSignature(
                                                        Optional.empty(),
                                                        parseTypeSignature("bigint"))),
                                        TypeSignatureParameter.of(
                                                new NamedTypeSignature(
                                                        Optional.empty(),
                                                        parseTypeSignature("varchar")))))));
        List<TypeSignature> resolvedTypeSignaturesList =
                convertApplicableTypeToVariable(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), resolvedTypeSignaturesList);
    }

    @Test
    public void testConvertApplicableTypeToVariableGenericArrayParamType()
    {
        TypeSignature actualTypeSignature = parseTypeSignature("test(array(decimal(i4, i5)), T)");
        TypeSignature expectedTypeSignature = new TypeSignature(
                "test",
                TypeSignatureParameter.of(
                        new TypeSignature(
                                "array",
                                TypeSignatureParameter.of(
                                        new TypeSignature(
                                                "decimal",
                                                TypeSignatureParameter.of("i4"),
                                                TypeSignatureParameter.of("i5"))))),
                TypeSignatureParameter.of(parseTypeSignature("T")));
        List<TypeSignature> resolvedTypeSignaturesList =
                convertApplicableTypeToVariable(actualTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures());
        assertEquals(expectedTypeSignature.getTypeOrNamedTypeParametersAsTypeSignatures(), resolvedTypeSignaturesList);
    }
}
