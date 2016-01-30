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

import com.facebook.presto.spi.type.TypeSignature;

import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TypeSignaturePair
{
    private final TypeSignature firstType;
    private final TypeSignature secondType;

    public TypeSignaturePair(TypeSignature firstType, TypeSignature secondType)
    {
        this.firstType = requireNonNull(firstType, "firstType is null");
        this.secondType = requireNonNull(secondType, "secondType is null");
    }

    public TypeSignature getFirstType()
    {
        return firstType;
    }

    public TypeSignature getSecondType()
    {
        return secondType;
    }

    public boolean is(String baseTypeOne, String baseTypeTwo)
    {
        return (firstType.getBase().equals(baseTypeOne) && secondType.getBase().equals(baseTypeTwo))
                || (secondType.getBase().equals(baseTypeOne) && firstType.getBase().equals(baseTypeTwo));
    }

    public boolean containsAnyOf(String... baseTypes)
    {
        for (String baseType : baseTypes) {
            if (firstType.getBase().equals(baseType) || secondType.getBase().equals(baseType)) {
                return true;
            }
        }
        return false;
    }

    public TypeSignature get(String baseType)
    {
        return getSingleParameterByPredicate((type) -> type.getBase().equals(baseType));
    }

    public boolean bothTypesAreCalculated()
    {
        return firstType.isCalculated() && secondType.isCalculated();
    }

    public boolean bothTypesAreUnparametrized()
    {
        return firstType.getParameters().isEmpty() && secondType.getParameters().isEmpty();
    }

    public boolean bothTypesAreWithLongLiteralParameters()
    {
        return firstType.isWithLongLiteralParameters() && secondType.isWithLongLiteralParameters();
    }

    public TypeSignature getTypeWithLongLiteralParameters()
    {
        return getSingleParameterByPredicate(TypeSignature::isWithLongLiteralParameters);
    }

    private TypeSignature getSingleParameterByPredicate(Predicate<TypeSignature> predicate)
    {
        boolean firstTypeMatch = predicate.test(firstType);
        boolean secondTypeMatch = predicate.test(secondType);
        checkState(firstTypeMatch || secondTypeMatch, "No types selected");
        checkState(!(firstTypeMatch && secondTypeMatch), "More than one type selected");
        return firstTypeMatch ? firstType : secondType;
    }
}
