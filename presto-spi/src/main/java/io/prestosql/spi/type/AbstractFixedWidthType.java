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
package io.prestosql.spi.type;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.FixedWidthBlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;

public abstract class AbstractFixedWidthType
        extends AbstractType
        implements FixedWidthType
{
    private final int fixedSize;

    protected AbstractFixedWidthType(TypeSignature signature, Class<?> javaType, int fixedSize)
    {
        super(signature, javaType);
        this.fixedSize = fixedSize;
    }

    @Override
    public final int getFixedSize()
    {
        return fixedSize;
    }

    @Override
    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new FixedWidthBlockBuilder(
                getFixedSize(),
                blockBuilderStatus,
                fixedSize == 0 ? expectedEntries : Math.min(expectedEntries, maxBlockSizeInBytes / fixedSize));
    }

    @Override
    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, fixedSize);
    }

    @Override
    public final BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new FixedWidthBlockBuilder(getFixedSize(), positionCount);
    }

    @Override
    public final Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, getFixedSize());
    }
}
