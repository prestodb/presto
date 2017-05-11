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
package com.facebook.presto.spi.predicate;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ValueEntry
        implements Comparable<ValueEntry>
{
    private final Type type;
    private final Block block;
    private final int position;

    public ValueEntry(Type type, Block block)
    {
        this(type, block, 0);
    }

    @JsonCreator
    public ValueEntry(
            @JsonProperty("type") Type type,
            @JsonProperty("block") Block block,
            @JsonProperty("position") int position)
    {
        this.type = requireNonNull(type, "type is null");
        this.block = requireNonNull(block, "block is null");
        this.position = position;
    }

    public static ValueEntry create(Type type, Object value)
    {
        return new ValueEntry(type, Utils.nativeValueToBlock(type, value));
    }

    public static ValueEntry create(Type type, Block block, int position)
    {
        return new ValueEntry(type, block, position);
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Block getBlock()
    {
        return block;
    }

    @JsonProperty
    public int getPosition()
    {
        return position;
    }

    public Object getValue()
    {
        return Utils.blockToNativeValue(type, block);
    }

    @Override
    public int hashCode()
    {
        return (int) type.hash(block, position);
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
        final ValueEntry other = (ValueEntry) obj;
        return Objects.equals(this.type, other.type)
                && type.equalTo(this.block, this.position, other.block, other.position);
    }

    @Override
    public int compareTo(ValueEntry that)
    {
        if (!this.type.equals(that.type)) {
            throw new IllegalArgumentException(String.format("Mismatched types: %s vs %s", this.type, that.type));
        }
        return type.compareTo(this.block, this.position, that.block, that.position);
    }
}
