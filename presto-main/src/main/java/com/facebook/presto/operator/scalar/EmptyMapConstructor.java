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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.type.UnknownType.UNKNOWN;

public final class EmptyMapConstructor
{
    private static final Block EMPTY_MAP = new InterleavedBlockBuilder(ImmutableList.of(UNKNOWN, UNKNOWN), new BlockBuilderStatus(), 0).build();

    private EmptyMapConstructor() {}

    @Description("Creates an empty map")
    @ScalarFunction
    @SqlType("map(unknown,unknown)")
    public static Block map()
    {
        return EMPTY_MAP;
    }
}
