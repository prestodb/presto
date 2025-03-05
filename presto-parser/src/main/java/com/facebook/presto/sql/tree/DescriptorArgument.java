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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DescriptorArgument
        extends Node
{
    private final Optional<Descriptor> descriptor;

    public static DescriptorArgument descriptorArgument(NodeLocation location, Descriptor descriptor)
    {
        requireNonNull(descriptor, "descriptor is null");
        return new DescriptorArgument(location, Optional.of(descriptor));
    }

    public static DescriptorArgument nullDescriptorArgument(NodeLocation location)
    {
        return new DescriptorArgument(location, Optional.empty());
    }

    private DescriptorArgument(NodeLocation location, Optional<Descriptor> descriptor)
    {
        super(Optional.of(location));
        this.descriptor = descriptor;
    }

    public Optional<Descriptor> getDescriptor()
    {
        return descriptor;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDescriptorArgument(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return descriptor.map(ImmutableList::of).orElse(ImmutableList.of());
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
        return Objects.equals(descriptor, ((DescriptorArgument) o).descriptor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(descriptor);
    }

    @Override
    public String toString()
    {
        return descriptor.map(Descriptor::toString).orElse("CAST (NULL AS DESCRIPTOR)");
    }

    @Override
    public boolean shallowEquals(Node o)
    {
        return sameClass(this, o);
    }
}
