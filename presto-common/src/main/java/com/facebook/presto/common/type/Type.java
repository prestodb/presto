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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.UncheckedBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.fasterxml.jackson.annotation.JsonValue;
import io.airlift.slice.Slice;

import java.util.List;

public interface Type
{
    /**
     * Gets the name of this type which must be case insensitive globally unique.
     * The name of a user defined type must be a legal identifier in Presto.
     */
    @JsonValue
    TypeSignature getTypeSignature();

    /**
     * Returns the name of this type that should be displayed to end-users.
     */
    String getDisplayName();

    /**
     * True if the type supports equalTo and hash.
     */
    boolean isComparable();

    /**
     * True if the type supports compareTo.
     */
    boolean isOrderable();

    /**
     * True if there can only be one canonical representation of data corresponding to this type.
     */
    default boolean equalValuesAreIdentical()
    {
        return true;
    }

    /**
     * Gets the Java class type used to represent this value on the stack during
     * expression execution.
     * <p>
     * Currently, this must be boolean, long, double, Slice or Block.
     */
    Class<?> getJavaType();

    /**
     * For parameterized types returns the list of parameters.
     */
    List<Type> getTypeParameters();

    /**
     * Creates the preferred block builder for this type. This is the builder used to
     * store values after an expression projection within the query.
     */
    BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry);

    /**
     * Creates the preferred block builder for this type. This is the builder used to
     * store values after an expression projection within the query.
     */
    BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries);

    /**
     * Gets an object representation of the type value in the {@code block}
     * {@code position}. This is the value returned to the user via the
     * REST endpoint and therefore must be JSON serializable.
     */
    Object getObjectValue(SqlFunctionProperties properties, Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as a boolean.
     */
    boolean getBoolean(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code internalPosition - offsetBase} as a boolean
     * without boundary checks.
     */
    boolean getBooleanUnchecked(UncheckedBlock block, int internalPosition);

    /**
     * Gets the value at the {@code block} {@code position} as a long.
     */
    long getLong(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code internalPosition - offsetBase} as a long
     * without boundary checks.
     */
    long getLongUnchecked(UncheckedBlock block, int internalPosition);

    /**
     * Gets the value at the {@code block} {@code position} as a double.
     */
    double getDouble(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code internalPosition - offsetBase} as a double
     * without boundary checks.
     */
    double getDoubleUnchecked(UncheckedBlock block, int internalPosition);

    /**
     * Gets the value at the {@code block} {@code position} as a Slice.
     */
    Slice getSlice(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code position} as a Slice
     * without boundary checks
     */
    Slice getSliceUnchecked(Block block, int internalPosition);

    /**
     * Gets the value at the {@code block} {@code position} as an Object.
     */
    Object getObject(Block block, int position);

    /**
     * Gets the value at the {@code block} {@code internalPosition - offsetBase} as a block
     * without boundary checks.
     */
    Block getBlockUnchecked(Block block, int internalPosition);

    /**
     * Writes the boolean value into the {@code BlockBuilder}.
     */
    void writeBoolean(BlockBuilder blockBuilder, boolean value);

    /**
     * Writes the long value into the {@code BlockBuilder}.
     */
    void writeLong(BlockBuilder blockBuilder, long value);

    /**
     * Writes the double value into the {@code BlockBuilder}.
     */
    void writeDouble(BlockBuilder blockBuilder, double value);

    /**
     * Writes the Slice value into the {@code BlockBuilder}.
     */
    void writeSlice(BlockBuilder blockBuilder, Slice value);

    /**
     * Writes the Slice value into the {@code BlockBuilder}.
     */
    void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length);

    /**
     * Writes the Object value into the {@code BlockBuilder}.
     */
    void writeObject(BlockBuilder blockBuilder, Object value);

    /**
     * Append the value at {@code position} in {@code block} to {@code blockBuilder}.
     */
    void appendTo(Block block, int position, BlockBuilder blockBuilder);

    /**
     * Are the values in the specified blocks at the specified positions equal?
     * <p>
     * This method assumes input is not null.
     */
    boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition);

    /**
     * Calculates the hash code of the value at the specified position in the
     * specified block.
     */
    long hash(Block block, int position);

    /**
     * Compare the values in the specified block at the specified positions equal.
     */
    int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition);
}
