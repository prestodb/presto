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
package io.prestosql.sql.tree;

import io.prestosql.sql.parser.ParsingException;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LongLiteral
        extends Literal
{
    private final long value;

    public LongLiteral(String value)
    {
        this(Optional.empty(), value);
    }

    public LongLiteral(NodeLocation location, String value)
    {
        this(Optional.of(location), value);
    }

    private LongLiteral(Optional<NodeLocation> location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        try {
            this.value = Long.parseLong(value);
        }
        catch (NumberFormatException e) {
            throw new ParsingException("Invalid numeric literal: " + value);
        }
    }

    public long getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLongLiteral(this, context);
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

        LongLiteral that = (LongLiteral) o;

        if (value != that.value) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (value ^ (value >>> 32));
    }
}
