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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.fasterxml.jackson.annotation.JsonValue;
import io.airlift.slice.Slice;

import java.util.List;

public abstract class Type
{
    /**
     * Gets the name of this type which must be case insensitive globally unique.
     * The name of a user defined type must be a legal identifier in Presto.
     */
    @JsonValue
    public abstract TypeSignature getTypeSignature();

    /**
     * Returns the name of this type that should be displayed to end-users.
     */
    public abstract String getDisplayName();

    /**
     * True if the type supports equalTo and hash.
     */
    public abstract boolean isComparable();

    /**
     * True if the type supports compareTo.
     */
    public abstract boolean isOrderable();

    /**
     * Gets the Java class type used to represent this value on the stack during
     * expression execution.
     * <p>
     * Currently, this must be boolean, long, double, Slice or Block.
     */
    public abstract Class<?> getJavaType();

    /**
     * For parameterized types returns the list of parameters.
     */
    public abstract List<Type> getTypeParameters();

    /**
     * Creates the preferred block builder for this type. This is the builder used to
     * store values after an expression projection within the query.
     */
    public abstract BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry);

    /**
     * Creates the preferred block builder for this type. This is the builder used to
     * store values after an expression projection within the query.
     */
    public abstract BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries);

    /**
     * Gets an object representation of the type value in the {@code block}
     * {@code position}. This is the value returned to the user via the
     * REST endpoint and therefore must be JSON serializable.
     */
    public abstract Object getObjectValue(ConnectorSession session, Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as a boolean.
     */
    public abstract boolean getBoolean(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as a long.
     */
    public abstract long getLong(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as a double.
     */
    public abstract double getDouble(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as a Slice.
     */
    public abstract Slice getSlice(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as an Object.
     */
    public abstract Object getObject(Block block, int position);

    /**
     * Writes the boolean value into the {@code BlockBuilder}.
     */
    public abstract void writeBoolean(BlockBuilder blockBuilder, boolean value);

    /**
     * Writes the long value into the {@code BlockBuilder}.
     */
    public abstract void writeLong(BlockBuilder blockBuilder, long value);

    /**
     * Writes the double value into the {@code BlockBuilder}.
     */
    public abstract void writeDouble(BlockBuilder blockBuilder, double value);

    /**
     * Writes the Slice value into the {@code BlockBuilder}.
     */
    public abstract void writeSlice(BlockBuilder blockBuilder, Slice value);

    /**
     * Writes the Slice value into the {@code BlockBuilder}.
     */
    public abstract void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length);

    /**
     * Writes the Object value into the {@code BlockBuilder}.
     */
    public abstract void writeObject(BlockBuilder blockBuilder, Object value);

    /**
     * Append the value at {@code position} in {@code block} to {@code blockBuilder}.
     */
    public abstract void appendTo(Block block, int position, BlockBuilder blockBuilder);

    /**
     * Are the values in the specified blocks at the specified positions equal?
     *
     * This method assumes input is not null.
     */
    public abstract boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition);

    /**
     * Calculates the hash code of the value at the specified position in the
     * specified block.
     */
    public abstract long hash(Block block, int position);

    /**
     * Compare the values in the specified block at the specified positions equal.
     */
    public abstract int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition);
}
