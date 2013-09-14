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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class BooleanLiteral
        extends Literal
{
    public static final BooleanLiteral TRUE_LITERAL = new BooleanLiteral("true");
    public static final BooleanLiteral FALSE_LITERAL = new BooleanLiteral("false");

    private final boolean value;

    public BooleanLiteral(String value)
    {
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkArgument(value.toLowerCase().equals("true") || value.toLowerCase().equals("false"));

        this.value = value.toLowerCase().equals("true");
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
        return Objects.hashCode(value);
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
        final BooleanLiteral other = (BooleanLiteral) obj;
        return Objects.equal(this.value, other.value);
    }
}
