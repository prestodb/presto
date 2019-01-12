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
package io.prestosql.type;

import io.prestosql.spi.type.ParameterKind;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeParameter;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.type.FunctionType.NAME;
import static java.util.stream.Collectors.toList;

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
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        checkArgument(parameters.size() >= 1, "Function type must have at least one parameter, got %s", parameters);
        checkArgument(
                parameters.stream().allMatch(parameter -> parameter.getKind() == ParameterKind.TYPE),
                "Expected only types as a parameters, got %s",
                parameters);
        List<Type> types = parameters.stream().map(TypeParameter::getType).collect(toList());

        return new FunctionType(types.subList(0, types.size() - 1), types.get(types.size() - 1));
    }
}
