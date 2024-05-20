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
package com.facebook.presto.hive;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.LazyBlock;
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class RowIDCoercer
        implements HiveCoercer
{
    private final ByteBuffer byteBuffer;
    private final Slice slice;

    public RowIDCoercer(byte[] partitionComponent, String rowGroupID)
    {
        requireNonNull(partitionComponent, "partitionComponent is null");
        requireNonNull(rowGroupID, "rowGroupID is null");

        // precompute content of the row_id slice
        byte[] rowGroupIdComponent = rowGroupID.getBytes(UTF_8);
        byte[] bytes = new byte[Long.BYTES + rowGroupIdComponent.length + partitionComponent.length];
        this.byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        this.slice = Slices.wrappedBuffer(bytes);
        this.slice.setBytes(Long.BYTES, rowGroupIdComponent);
        this.slice.setBytes(Long.BYTES + rowGroupIdComponent.length, partitionComponent);
    }

    @Override
    public TupleDomainFilter toCoercingFilter(TupleDomainFilter filter, Subfield subfield)
    {
        checkArgument(subfield.getPath().isEmpty(), "Subfields on primitive types are not allowed");
        return filter;
    }

    @Override
    public Type getToType()
    {
        return VARBINARY;
    }

    @Override
    public Block apply(Block rowNumberBlock)
    {
        requireNonNull(rowNumberBlock, "rowNumberBlock is null");

        int positionCount = rowNumberBlock.getPositionCount();
        Block lazyBlock = new LazyBlock(positionCount, (block) -> {
            BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(null, positionCount, slice.length());
            for (int i = 0; i < positionCount; i++) {
                long rowNumber = BIGINT.getLong(rowNumberBlock, i);
                byteBuffer.putLong(0, rowNumber);
                VARBINARY.writeSlice(blockBuilder, slice);
            }
            block.setBlock(blockBuilder.build());
        });
        return lazyBlock;
    }
}
