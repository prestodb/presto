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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WindowInline
        extends Window
{
    private final WindowSpecification specification;

    public WindowInline(WindowSpecification specification)
    {
        this(Optional.empty(), specification);
    }

    public WindowInline(NodeLocation location, WindowSpecification specification)
    {
        this(Optional.of(location), specification);
    }

    private WindowInline(Optional<NodeLocation> location, WindowSpecification specification)
    {
        super(location);
        this.specification = requireNonNull(specification, "specification is null");
    }

    public WindowSpecification getSpecification()
    {
        return specification;
    }

    public Optional<WindowFrame> getFrame()
    {
        return specification.getFrame();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWindowInline(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(specification);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        WindowInline o = (WindowInline) obj;
        return Objects.equals(specification, o.specification);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(specification);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("specification", specification)
                .toString();
    }
}
