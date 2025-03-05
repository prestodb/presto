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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Descriptor
        extends Node
{
    private final List<DescriptorField> fields;

    public Descriptor(NodeLocation location, List<DescriptorField> fields)
    {
        super(Optional.of(location));
        requireNonNull(fields, "fields is null");
        checkArgument(!fields.isEmpty(), "fields list is empty");
        this.fields = fields;
    }

    public List<DescriptorField> getFields()
    {
        return fields;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDescriptor(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return fields;
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
        return Objects.equals(fields, ((Descriptor) o).fields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fields);
    }

    @Override
    public String toString()
    {
        return fields.stream()
                .map(DescriptorField::toString)
                .collect(Collectors.joining(", ", "DESCRIPTOR(", ")"));
    }

    @Override
    public boolean shallowEquals(Node o)
    {
        return sameClass(this, o);
    }
}
