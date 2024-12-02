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
package com.facebook.presto.spi;

import com.facebook.presto.common.type.DistinctTypeInfo;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;

import java.util.Objects;

/**
 * TypeSignature but has overridden equals(). Here, we compare exact signature of any underlying distinct
 * types. Some distinct types may have extra information on their lazily loaded parents, and same parent
 * information is compared in equals(). This is needed to cache types in parametricTypesCache.
 */
public class ExactTypeSignature
{
    private final TypeSignature typeSignature;

    public ExactTypeSignature(TypeSignature typeSignature)
    {
        this.typeSignature = typeSignature;
    }

    public TypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeSignature);
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

        ExactTypeSignature other = (ExactTypeSignature) o;
        return equals(typeSignature, other.typeSignature);
    }

    private static boolean equals(TypeSignature left, TypeSignature right)
    {
        if (!left.equals(right)) {
            return false;
        }

        if (left.isDistinctType() && right.isDistinctType()) {
            return equals(left.getDistinctTypeInfo(), right.getDistinctTypeInfo());
        }
        int index = 0;
        for (TypeSignatureParameter leftParameter : left.getParameters()) {
            TypeSignatureParameter rightParameter = right.getParameters().get(index++);
            if (!leftParameter.getKind().equals(rightParameter.getKind())) {
                return false;
            }

            switch (leftParameter.getKind()) {
                case TYPE:
                    if (!equals(leftParameter.getTypeSignature(), rightParameter.getTypeSignature())) {
                        return false;
                    }
                    break;
                case NAMED_TYPE:
                    if (!equals(leftParameter.getNamedTypeSignature().getTypeSignature(), rightParameter.getNamedTypeSignature().getTypeSignature())) {
                        return false;
                    }
                    break;
                case DISTINCT_TYPE:
                    if (!equals(leftParameter.getDistinctTypeInfo(), rightParameter.getDistinctTypeInfo())) {
                        return false;
                    }
                    break;
            }
        }
        return true;
    }

    private static boolean equals(DistinctTypeInfo left, DistinctTypeInfo right)
    {
        return Objects.equals(left.getName(), right.getName()) &&
                Objects.equals(left.getBaseType(), right.getBaseType()) &&
                Objects.equals(left.isOrderable(), right.isOrderable()) &&
                Objects.equals(left.getTopMostAncestor(), right.getTopMostAncestor()) &&
                Objects.equals(left.getOtherAncestors(), right.getOtherAncestors());
    }
}
