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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.ColumnIdentity;
import com.facebook.presto.spi.PrestoException;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import static com.facebook.presto.spi.StandardErrorCode.CORRUPT_SERIALIZED_IDENTITY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.io.ByteStreams.newDataInput;
import static com.google.common.io.ByteStreams.newDataOutput;
import static java.util.Objects.hash;

public final class RaptorColumnIdentity
        implements ColumnIdentity
{
    private static final byte CURRENT_VERSION = 1;
    private static final int SERIALIZED_SIZE = Byte.BYTES + Long.BYTES;

    private final long columnId;

    public RaptorColumnIdentity(long columnId)
    {
        this.columnId = columnId;
    }

    public static RaptorColumnIdentity deserialize(byte[] bytes)
    {
        checkArgument(bytes.length >= Byte.BYTES, "bytes for RaptorColumnIdentity is corrupt");

        ByteArrayDataInput input = newDataInput(bytes);
        byte version = input.readByte();
        if ((version == CURRENT_VERSION) && (bytes.length == SERIALIZED_SIZE)) {
            long columnId = input.readLong();
            return new RaptorColumnIdentity(columnId);
        }

        throw new PrestoException(CORRUPT_SERIALIZED_IDENTITY, "RaptorColumnIdentity is corrupt: " + base16().lowerCase().encode(bytes));
    }

    @Override
    public byte[] serialize()
    {
        ByteArrayDataOutput output = newDataOutput(SERIALIZED_SIZE);
        output.write(CURRENT_VERSION);
        output.writeLong(columnId);
        return output.toByteArray();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RaptorColumnIdentity that = (RaptorColumnIdentity) o;
        return columnId == that.columnId;
    }

    @Override
    public int hashCode()
    {
        return hash(columnId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnId", columnId)
                .toString();
    }
}
