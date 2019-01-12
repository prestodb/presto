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
package io.prestosql.sql.analyzer;

import io.prestosql.spi.type.Type;

import javax.annotation.concurrent.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class ResolvedField
{
    private final Scope scope;
    private final Field field;
    private final int hierarchyFieldIndex;
    private final int relationFieldIndex;
    private final boolean local;

    public ResolvedField(Scope scope, Field field, int hierarchyFieldIndex, int relationFieldIndex, boolean local)
    {
        this.scope = requireNonNull(scope, "scope is null");
        this.field = requireNonNull(field, "field is null");
        this.hierarchyFieldIndex = hierarchyFieldIndex;
        this.relationFieldIndex = relationFieldIndex;
        this.local = local;
    }

    public Type getType()
    {
        return field.getType();
    }

    public Scope getScope()
    {
        return scope;
    }

    public boolean isLocal()
    {
        return local;
    }

    public int getHierarchyFieldIndex()
    {
        return hierarchyFieldIndex;
    }

    public int getRelationFieldIndex()
    {
        return relationFieldIndex;
    }

    public Field getField()
    {
        return field;
    }
}
