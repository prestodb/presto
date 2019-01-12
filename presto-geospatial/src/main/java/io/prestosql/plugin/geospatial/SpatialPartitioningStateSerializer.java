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
package io.prestosql.plugin.geospatial;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.type.VarbinaryType.VARBINARY;

public class SpatialPartitioningStateSerializer
        implements AccumulatorStateSerializer<SpatialPartitioningState>
{
    @Override
    public Type getSerializedType()
    {
        // TODO: make serializer optional in case of non decomposable aggregation
        return VARBINARY;
    }

    @Override
    public void serialize(SpatialPartitioningState state, BlockBuilder out)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deserialize(Block block, int index, SpatialPartitioningState state)
    {
        throw new UnsupportedOperationException();
    }
}
