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
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;

public class Identifier
        extends Expression
{
    private static final Pattern NAME_PATTERN = Pattern.compile("[a-zA-Z_]([a-zA-Z0-9_:@])*");

    private final String value;
    private final boolean delimited;

    public Identifier(NodeLocation location, String value, boolean delimited)
    {
        this(Optional.of(location), value, delimited);
    }

    public Identifier(String value, boolean delimited)
    {
        this(Optional.empty(), value, delimited);
    }
    public Identifier(Optional<NodeLocation> location, String value)
    {
        this(location, value, !NAME_PATTERN.matcher(value).matches());
    }

    public Identifier(String value)
    {
        this(Optional.empty(), value, !NAME_PATTERN.matcher(value).matches());
    }

    private Identifier(Optional<NodeLocation> location, String value, boolean delimited)
    {
        super(location);
        this.value = value;
        this.delimited = delimited;

        checkArgument(delimited || NAME_PATTERN.matcher(value).matches(), "value contains illegal characters: %s", value);
    }

    public String getValue()
    {
        return value;
    }

    public String getValueLowerCase()
    {
        return value.toLowerCase(ENGLISH);
    }

    public boolean isDelimited()
    {
        return delimited;
    }

    public String getCanonicalValue()
    {
        if (isDelimited()) {
            return value;
        }

        return value.toUpperCase(ENGLISH);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIdentifier(this, context);
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

        Identifier that = (Identifier) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
