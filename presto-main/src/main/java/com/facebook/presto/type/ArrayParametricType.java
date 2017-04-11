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

import com.facebook.presto.spi.type.ParameterKind;
import com.facebook.presto.spi.type.ParametricType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeParameter;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class ArrayParametricType
        implements ParametricType
{
    public static final ArrayParametricType ARRAY = new ArrayParametricType();

    private ArrayParametricType()
    {
    }

    @Override
    public String getName()
    {
        return StandardTypes.ARRAY;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        checkArgument(parameters.size() == 1, "Array type expects exactly one type as a parameter, got %s", parameters);
        checkArgument(
                parameters.get(0).getKind() == ParameterKind.TYPE,
                "Array expects type as a parameter, got %s",
                parameters);
        return new ArrayType(parameters.get(0).getType());
    }
}
