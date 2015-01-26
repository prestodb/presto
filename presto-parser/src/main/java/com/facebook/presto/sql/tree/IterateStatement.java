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

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class IterateStatement
        extends ControlStatement
{
    private final String label;

    public IterateStatement(String label)
    {
        this(Optional.empty(), label);
    }

    public IterateStatement(NodeLocation location, String label)
    {
        this(Optional.of(location), label);
    }

    public IterateStatement(Optional<NodeLocation> location, String label)
    {
        super(location);
        this.label = requireNonNull(label, "label is null");
    }

    public String getLabel()
    {
        return label;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIterateStatement(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label);
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
        IterateStatement o = (IterateStatement) obj;
        return Objects.equals(label, o.label);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("label", label)
                .toString();
    }
}
