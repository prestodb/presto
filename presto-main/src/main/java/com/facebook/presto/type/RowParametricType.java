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

import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.operator.scalar.RowFieldReference;
import com.facebook.presto.spi.type.NamedType;
import com.facebook.presto.spi.type.ParameterKind;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeParameter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.type.RowType.RowField;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
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
                "Expected only named types as a parameters, got %s",
                parameters);
        List<NamedType> namedTypes = parameters.stream().map(TypeParameter::getNamedType).collect(toList());

        return new RowType(
                namedTypes.stream().map(NamedType::getType).collect(toList()),
                Optional.of(namedTypes.stream().map(NamedType::getName).collect(toList())));
    }

    public List<SqlFunction> createFunctions(Type type)
    {
        RowType rowType = checkType(type, RowType.class, "type");
        ImmutableList.Builder<SqlFunction> builder = ImmutableList.builder();
        List<RowField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            RowField field = fields.get(i);
            int index = i;
            field.getName()
                    .ifPresent(name -> builder.add(new RowFieldReference(rowType, field.getType(), index, field.getName().get())));
        }
        return builder.build();
    }
}
