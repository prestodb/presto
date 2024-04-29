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
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class RowIDCoercer
        implements HiveCoercer
{
    private final byte[] rowIDPartitionComponent;
    private final byte[] rowGroupID; // file name

    RowIDCoercer(byte[] rowIDPartitionComponent, String rowGroupID)
    {
        this.rowIDPartitionComponent = requireNonNull(rowIDPartitionComponent);
        this.rowGroupID = rowGroupID.getBytes(StandardCharsets.UTF_8);
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
        return VarbinaryType.VARBINARY;
    }

    @Override
    public Block apply(Block in)
    {
        BlockBuilder out = VarbinaryType.VARBINARY.createBlockBuilder(null, in.getPositionCount());
        for (int i = 0; i < in.getPositionCount(); i++) {
            if (in.isNull(i)) {
                out.appendNull();
                continue;
            }
            long rowNumber = BigintType.BIGINT.getLong(in, i);
            ByteBuffer rowID = ByteBuffer.allocateDirect(this.rowIDPartitionComponent.length + this.rowGroupID.length + 8).order(ByteOrder.LITTLE_ENDIAN);
            rowID.putLong(rowNumber);
            rowID.put(this.rowGroupID);
            rowID.put(this.rowIDPartitionComponent);
            rowID.flip();
            VarbinaryType.VARBINARY.writeSlice(out, Slices.wrappedBuffer(rowID));
        }
        return out.build();
    }
}
