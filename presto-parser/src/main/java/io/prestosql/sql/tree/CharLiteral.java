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

import com.google.common.base.CharMatcher;
import io.airlift.slice.Slice;

import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class CharLiteral
        extends Literal
{
    private final String value;
    private final Slice slice;

    public CharLiteral(String value)
    {
        this(Optional.empty(), value);
    }

    public CharLiteral(NodeLocation location, String value)
    {
        this(Optional.of(location), value);
    }

    public CharLiteral(Optional<NodeLocation> location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        this.value = value;
        this.slice = utf8Slice(CharMatcher.is(' ').trimTrailingFrom(value));
    }

    public String getValue()
    {
        return value;
    }

    public Slice getSlice()
    {
        return slice;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCharLiteral(this, context);
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
        CharLiteral that = (CharLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }
}
