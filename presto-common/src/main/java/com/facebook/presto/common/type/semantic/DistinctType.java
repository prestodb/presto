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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UserDefinedType;

import static com.facebook.presto.common.type.semantic.SemanticType.SemanticTypeCategory.DISTINCT_TYPE;
import static java.util.Objects.requireNonNull;

public class DistinctType
        extends SemanticType
{
    private final QualifiedObjectName name;

    public DistinctType(QualifiedObjectName name, Type type)
    {
        super(DISTINCT_TYPE, new TypeSignature(new UserDefinedType(name, type.getTypeSignature())), type);
        // TODO We should disallow RowType here, as that should be a StructuredType instead.
        this.name = requireNonNull(name, "name is null");
    }

    public String getName()
    {
        return name.toString();
    }

    @Override
    public String getDisplayName()
    {
        return name.toString();
    }
}
