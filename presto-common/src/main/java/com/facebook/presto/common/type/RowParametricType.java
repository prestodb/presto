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
package com.facebook.presto.common.type;

import java.util.List;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class RowParametricType
        implements ParametricType
{
    public static final RowParametricType ROW = new RowParametricType();

    private RowParametricType()
    {
    }

    @Override
    public String getName()
    {
        return StandardTypes.ROW;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        checkArgument(!parameters.isEmpty(), "Row type must have at least one parameter");
        checkArgument(
                parameters.stream().allMatch(parameter -> parameter.getKind() == ParameterKind.NAMED_TYPE),
                format("Expected only named types as a parameters, got %s", parameters));

        List<RowType.Field> fields = parameters.stream()
                .map(TypeParameter::getNamedType)
                .map(parameter -> new RowType.Field(parameter.getName().map(RowFieldName::getName), parameter.getType(), parameter.getName().map(RowFieldName::isDelimited).orElse(false)))
                .collect(toList());

        return RowType.from(fields);
    }
}
