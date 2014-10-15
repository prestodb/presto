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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkNotNull;

public final class TypeUtils
{
    private TypeUtils()
    {
    }

    public static int hashPosition(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return 0;
        }
        return type.hash(block, position);
    }

    public static boolean positionEqualsPosition(Type type, Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        boolean leftIsNull = leftBlock.isNull(leftPosition);
        boolean rightIsNull = rightBlock.isNull(rightPosition);
        if (leftIsNull || rightIsNull) {
            return leftIsNull && rightIsNull;
        }
        return type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    public static Function<Type, TypeSignature> typeSignatureGetter()
    {
        return new Function<Type, TypeSignature>() {
            @Override
            public TypeSignature apply(Type input)
            {
                return input.getTypeSignature();
            }
        };
    }

    public static Function<String, TypeSignature> typeSignatureParser()
    {
        return new Function<String, TypeSignature>() {
            @Override
            public TypeSignature apply(String input)
            {
                return parseTypeSignature(input);
            }
        };
    }

    public static List<Type> resolveTypes(List<TypeSignature> typeNames, final TypeManager typeManager)
    {
        return FluentIterable.from(typeNames).transform(new Function<TypeSignature, Type>() {
            @Override
            public Type apply(TypeSignature type)
            {
                return checkNotNull(typeManager.getType(type), "Type '%s' not found", type);
            }
        }).toList();
    }

    public static TypeSignature parameterizedTypeName(String base, TypeSignature... argumentNames)
    {
        return new TypeSignature(base, ImmutableList.copyOf(argumentNames), ImmutableList.of());
    }
}
