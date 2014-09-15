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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;

import java.util.List;

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

    public static Function<Type, String> nameGetter()
    {
        return new Function<Type, String>() {
            @Override
            public String apply(Type input)
            {
                return input.getName();
            }
        };
    }

    public static List<Type> resolveTypes(List<String> typeNames, final TypeManager typeManager)
    {
        return FluentIterable.from(typeNames).transform(new Function<String, Type>() {
            @Override
            public Type apply(String type)
            {
                return checkNotNull(typeManager.getType(type), "Type '%s' not found", type);
            }
        }).toList();
    }

    public static String parameterizedTypeName(String base, String... argumentNames)
    {
        return base + "<" + Joiner.on(",").join(argumentNames) + ">";
    }
}
