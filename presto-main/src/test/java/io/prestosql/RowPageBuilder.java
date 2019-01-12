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
package io.prestosql;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.util.StructuralTestUtil.appendToBlockBuilder;
import static java.util.Objects.requireNonNull;

public class RowPageBuilder
{
    private final List<Type> types;

    public static RowPageBuilder rowPageBuilder(Type... types)
    {
        return rowPageBuilder(ImmutableList.copyOf(types));
    }

    public static RowPageBuilder rowPageBuilder(Iterable<Type> types)
    {
        return new RowPageBuilder(types);
    }

    private final List<BlockBuilder> builders;
    private long rowCount;

    RowPageBuilder(Iterable<Type> types)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        ImmutableList.Builder<BlockBuilder> builders = ImmutableList.builder();
        for (Type type : types) {
            builders.add(type.createBlockBuilder(null, 1));
        }
        this.builders = builders.build();
        checkArgument(!this.builders.isEmpty(), "At least one value info is required");
    }

    public boolean isEmpty()
    {
        return rowCount == 0;
    }

    public RowPageBuilder row(Object... values)
    {
        checkArgument(values.length == builders.size(), "Expected %s values, but got %s", builders.size(), values.length);

        for (int channel = 0; channel < values.length; channel++) {
            append(channel, values[channel]);
        }
        rowCount++;
        return this;
    }

    public Page build()
    {
        Block[] blocks = new Block[builders.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = builders.get(i).build();
        }
        return new Page(blocks);
    }

    private void append(int channel, Object element)
    {
        BlockBuilder blockBuilder = builders.get(channel);
        Type type = types.get(channel);
        appendToBlockBuilder(type, element, blockBuilder);
    }
}
