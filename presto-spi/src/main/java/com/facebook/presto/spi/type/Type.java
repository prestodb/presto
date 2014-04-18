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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.fasterxml.jackson.annotation.JsonValue;

public interface Type
{
    /**
     * Gets the name of this type which must be case insensitive globally unique.
     * The name of a user defined type must be a legal identifier in Presto.
     */
    @JsonValue
    String getName();

    /**
     * Gets the Java class type used to represent this value on the stack during
     * expression execution. This value is used to determine which method should
     * be called on Cursor, RecordSet or RandomAccessBlock to fetch a value of
     * this type.
     *
     * Currently, this must be boolean, long, double, or Slice.
     */
    Class<?> getJavaType();

    /**
     * Creates a block builder for this type.  This is the builder used to
     * store values after an expression projection within the query.
     */
    BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus);
}
