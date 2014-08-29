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

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

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
    public ArrayType createType(List<Type> types)
    {
        checkArgument(types.size() == 1, "Expected only one type, got %s", types);
        return new ArrayType(types.get(0));
    }
}
