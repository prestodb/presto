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
package com.facebook.presto.common.type.semantic;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.NamedTypeSignature;
import com.facebook.presto.common.type.RowFieldName;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.RowType.Field;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.UserDefinedType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.RowType.field;
import static com.facebook.presto.common.type.semantic.SemanticType.SemanticTypeCategory.STRUCTURED_TYPE;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class StructuredType
        extends SemanticType
{
    private final QualifiedObjectName name;
    private final List<String> memberNames;
    private final List<SemanticType> memberTypes;

    public StructuredType(QualifiedObjectName name, List<String> memberNames, List<SemanticType> memberTypes)
    {
        super(
                STRUCTURED_TYPE,
                new TypeSignature(format("%s:row", name.toString()), IntStream.range(0, memberNames.size())
                .mapToObj(i -> TypeSignatureParameter.of(new NamedTypeSignature(Optional.of(new RowFieldName(memberNames.get(i), false)), memberTypes.get(i).getTypeSignature())))
                .collect(toList())),
                RowType.from(IntStream.range(0, memberNames.size())
                        .mapToObj(i -> field(memberNames.get(i), memberTypes.get(i)))
                        .collect(toList())));
        this.name = requireNonNull(name, "name is null");
        if (memberNames.size() != memberTypes.size()) {
            throw new IllegalStateException(format("size of memberNames (%d) and memberTypes (%d) doesn't match for type %s", memberNames.size(), memberTypes.size(), name));
        }
        this.memberNames = unmodifiableList(memberNames);
        this.memberTypes = unmodifiableList(memberTypes);
    }

    public StructuredType(QualifiedObjectName name, RowType type)
    {
        super(STRUCTURED_TYPE, new TypeSignature(new UserDefinedType(name, type.getTypeSignature())), type);
        this.name = requireNonNull(name, "name is null");
        this.memberNames = unmodifiableList(type.getFields().stream().map(Field::getName).map(Optional::get).collect(toList()));
        this.memberTypes = unmodifiableList(type.getFields().stream().map(Field::getSemanticType).collect(toList()));
    }

    @Override
    public String getName()
    {
        return name.toString();
    }

    @Override
    public String getDisplayName()
    {
        return name.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StructuredType other = (StructuredType) obj;

        return super.equals(other) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.memberNames, other.memberNames) &&
                Objects.equals(this.memberTypes, other.memberTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, memberNames, memberTypes);
    }
}
