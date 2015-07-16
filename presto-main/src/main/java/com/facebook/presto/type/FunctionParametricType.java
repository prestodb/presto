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

import com.facebook.presto.spi.type.Type;

import java.util.List;

import static com.facebook.presto.type.FunctionType.NAME;
import static com.google.common.base.Preconditions.checkArgument;

public final class FunctionParametricType
        implements ParametricType
{
    public static final FunctionParametricType FUNCTION = new FunctionParametricType();

    private FunctionParametricType()
    {
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public FunctionType createType(List<Type> types, List<Object> literals)
    {
        checkArgument(types.size() >= 1, "Function type must have at least one parameter, got %s", types);
        checkArgument(literals.isEmpty(), "Unexpected literals: %s", literals);
        return new FunctionType(types.subList(0, types.size() - 1), types.get(types.size() - 1));
    }
}
