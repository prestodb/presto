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
package io.prestosql.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Optional;

public class LazyBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "LAZY";

    public LazyBlockEncoding() {}

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input)
    {
        // We write the actual underlying block, so we will never need to read a lazy block
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        // We implemented replacementBlockForWrite, so we will never need to write a lazy block
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Block> replacementBlockForWrite(Block block)
    {
        return Optional.of(block.getLoadedBlock());
    }
}
