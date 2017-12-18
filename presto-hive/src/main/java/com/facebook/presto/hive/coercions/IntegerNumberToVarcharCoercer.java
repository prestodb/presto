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
package com.facebook.presto.hive.coercions;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;

import java.util.function.Function;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class IntegerNumberToVarcharCoercer
        implements Function<Block, Block>
{
    private final Type fromType;
    private final Type toType;

    public IntegerNumberToVarcharCoercer(Type fromType, Type toType)
    {
        this.fromType = requireNonNull(fromType, "fromType is null");
        this.toType = requireNonNull(toType, "toType is null");
    }

    @Override
    public Block apply(Block block)
    {
        BlockBuilder blockBuilder = toType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
                continue;
            }
            toType.writeSlice(blockBuilder, utf8Slice(String.valueOf(fromType.getLong(block, i))));
        }
        return blockBuilder.build();
    }
}
