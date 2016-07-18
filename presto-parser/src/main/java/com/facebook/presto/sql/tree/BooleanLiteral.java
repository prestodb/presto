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

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.Optional;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class BooleanLiteral
        extends Literal
{
    public static final BooleanLiteral TRUE_LITERAL = new BooleanLiteral(Optional.empty(), "true");
    public static final BooleanLiteral FALSE_LITERAL = new BooleanLiteral(Optional.empty(), "false");

    private final boolean value;

    public BooleanLiteral(String value)
    {
        this(Optional.empty(), value);
    }

    public BooleanLiteral(NodeLocation location, String value)
    {
        this(Optional.of(location), value);
    }

    private BooleanLiteral(Optional<NodeLocation> location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        Preconditions.checkArgument(value.toLowerCase(ENGLISH).equals("true") || value.toLowerCase(ENGLISH).equals("false"));

        this.value = value.toLowerCase(ENGLISH).equals("true");
    }

    public boolean getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBooleanLiteral(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
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
        BooleanLiteral other = (BooleanLiteral) obj;
        return Objects.equals(this.value, other.value);
    }
}
