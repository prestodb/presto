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
package com.facebook.presto.sql.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class SymbolReference
        extends Expression
{
    private final String name;
    private final Set<String> fields;

    public SymbolReference(String name)
    {
        super(Optional.empty());
        this.name = name;
        this.fields = ImmutableSet.of();
    }

    public SymbolReference(String name, Set<String> fields)
    {
        super(Optional.empty());
        this.name = name;
        this.fields = ImmutableSet.copyOf(fields);
    }

    public String getName()
    {
        return name;
    }

    public Set<String> getFields()
    {
        return fields;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSymbolReference(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SymbolReference that = (SymbolReference) o;
        return Objects.equals(name, that.name) && Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, fields);
    }

    public boolean contains(SymbolReference o)
    {
        if (!this.getName().equals(o.getName())) {
            return false;
        }

        if (this.getFields().isEmpty()) {
            return true;
        }

        if (o.getFields().isEmpty()) {
            return false;
        }

        return this.getFields().containsAll(o.getFields());
    }
}
