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

import static java.util.Objects.requireNonNull;

class RowIDCoercer
        implements HiveCoercer
{
    private final byte[] rowIdPartitionComponent;

    RowIDCoercer(byte[] rowIdPartitionComponent) {
        // TODO should I copy this to avoid mutable internal state?
        this.rowIdPartitionComponent = requireNonNull(rowIdPartitionComponent);
    }

    @Override
    public TupleDomainFilter toCoercingFilter(TupleDomainFilter filter, Subfield subfield)
    {
        return null;
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
            // TODO also need row group ID
            // TODO make little endian
            ByteBuffer rowID = ByteBuffer.allocateDirect(this.rowIdPartitionComponent.length + 8);
            rowID.putLong(rowNumber);
            rowID.put(this.rowIdPartitionComponent);
            rowID.flip();
            VarbinaryType.VARBINARY.writeSlice(out, Slices.wrappedBuffer(rowID));
        }
        return out.build();
    }
}
