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

import com.facebook.presto.sql.parser.ParsingException;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class BinaryLiteral
        extends Literal
{
    private final Slice slice;

    public BinaryLiteral(String value)
    {
        this(Optional.empty(), value);
    }

    public BinaryLiteral(Optional<NodeLocation> location, String value)
    {
        super(location);
        requireNonNull(value, "value is null");
        // the grammar could possibly include White Space in the value it passes to us
        String pureHexString = value.replaceAll("[ \\r\\n\\t]", "").toUpperCase();
        if (pureHexString.matches(".*[^A-F0-9].*")) {
            throw new ParsingException("Binary literal can only contain hexadecimal digits", location.get());
        }
        if (pureHexString.length() % 2 != 0) {
            throw new ParsingException("Binary literal must contain an even number of digits", location.get());
        }
        this.slice = Slices.wrappedBuffer(BaseEncoding.base16().decode(pureHexString));
    }

    public BinaryLiteral(NodeLocation location, String value)
    {
        this(Optional.of(location), value);
    }

    // exports in hex IN UPPER CASE
    public String toHexString()
    {
        return BaseEncoding.base16().encode(slice.getBytes());
    }

    public Slice getSlice()
    {
        return slice;
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

        if (!slice.equals(that.slice)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return slice.hashCode();
    }
}
