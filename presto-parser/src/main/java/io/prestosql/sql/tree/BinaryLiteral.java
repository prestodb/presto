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

import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.sql.parser.ParsingException;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class BinaryLiteral
        extends Literal
{
    // the grammar could possibly include whitespace in the value it passes to us
    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("[ \\r\\n\\t]");
    private static final Pattern NOT_HEX_DIGIT_PATTERN = Pattern.compile(".*[^A-F0-9].*");

    private final Slice value;

    public BinaryLiteral(String value)
    {
        this(Optional.empty(), value);
    }

    public BinaryLiteral(Optional<NodeLocation> location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        String hexString = WHITESPACE_PATTERN.matcher(value).replaceAll("").toUpperCase();
        if (NOT_HEX_DIGIT_PATTERN.matcher(hexString).matches()) {
            throw new ParsingException("Binary literal can only contain hexadecimal digits", location.get());
        }
        if (hexString.length() % 2 != 0) {
            throw new ParsingException("Binary literal must contain an even number of digits", location.get());
        }
        this.value = Slices.wrappedBuffer(BaseEncoding.base16().decode(hexString));
    }

    public BinaryLiteral(NodeLocation location, String value)
    {
        this(Optional.of(location), value);
    }

    /**
     * Return the valued as a hex-formatted string with upper-case characters
     */
    public String toHexString()
    {
        return BaseEncoding.base16().encode(value.getBytes());
    }

    public Slice getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBinaryLiteral(this, context);
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

        BinaryLiteral that = (BinaryLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }
}
