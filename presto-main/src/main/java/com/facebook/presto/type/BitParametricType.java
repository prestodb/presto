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

import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeParameter;

import java.util.List;

import static com.facebook.presto.type.BitType.bitType;
import static com.google.common.base.Preconditions.checkArgument;

public class BitParametricType
        implements ParametricType
{
    public static final BitParametricType BIT = new BitParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.BIT;
    }

    @Override
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        checkArgument(parameters.size() == 1, "Expected exactly one parameter for BIT");
        TypeParameter parameter = parameters.get(0);

        checkArgument(parameter.isLongLiteral(), "BIT length must be a number");
        long length = parameter.getLongLiteral();

        checkArgument((length >= 0) && (length <= Integer.MAX_VALUE), "Invalid BIT length: %s", length);
        return bitType((int) length);
    }
}
