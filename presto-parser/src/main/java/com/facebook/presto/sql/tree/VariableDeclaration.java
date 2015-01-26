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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class VariableDeclaration
        extends ControlStatement
{
    private final List<String> names;
    private final String type;
    private final Optional<Expression> defaultValue;

    public VariableDeclaration(List<String> names, String type, Optional<Expression> defaultValue)
    {
        this(Optional.empty(), names, type, defaultValue);
    }

    public VariableDeclaration(NodeLocation location, List<String> names, String type, Optional<Expression> defaultValue)
    {
        this(Optional.of(location), names, type, defaultValue);
    }

    public VariableDeclaration(Optional<NodeLocation> location, List<String> names, String type, Optional<Expression> defaultValue)
    {
        super(location);
        this.names = requireNonNull(names, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
    }

    public List<String> getNames()
    {
        return names;
    }

    public String getType()
    {
        return type;
    }

    public Optional<Expression> getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitVariableDeclaration(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(names, type, defaultValue);
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
        VariableDeclaration o = (VariableDeclaration) obj;
        return Objects.equals(names, o.names) &&
                Objects.equals(type, o.type) &&
                Objects.equals(defaultValue, o.defaultValue);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("names", names)
                .add("type", type)
                .add("defaultValue", defaultValue)
                .toString();
    }
}
