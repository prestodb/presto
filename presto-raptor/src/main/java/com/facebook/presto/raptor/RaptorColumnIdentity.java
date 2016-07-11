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
import com.google.common.primitives.Longs;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.primitives.Longs.fromByteArray;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.util.Arrays.copyOfRange;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

public class RaptorColumnIdentity
        implements ColumnIdentity
{
    private static final byte CURRENT_VERSION = 1;

    private final byte version;
    private final long columnId;

    public RaptorColumnIdentity(long columnId)
    {
        this(CURRENT_VERSION, columnId);
    }

    private RaptorColumnIdentity(byte version, long columnId)
    {
        checkArgument(version > 0 && version <= CURRENT_VERSION, "version must be a positive integer less than or equal to %s", CURRENT_VERSION);
        this.version = version;
        this.columnId = columnId;
    }

    public static RaptorColumnIdentity deserialize(byte[] bytes)
    {
        requireNonNull(bytes, "bytes for RaptorColumnIdentity is null");
        checkArgument(bytes.length >= Byte.BYTES, "bytes for RaptorColumnIdentity is empty");

        // The first byte is always for version.
        byte version = bytes[0];
        if (version == CURRENT_VERSION && bytes.length == Byte.BYTES + Long.BYTES) {
            // The following 8 bytes are for columnId.
            long columnId = fromByteArray(copyOfRange(bytes, Byte.BYTES, Byte.BYTES + Long.BYTES));
            return new RaptorColumnIdentity(version, columnId);
        }

        throw new PrestoException(
                GENERIC_INTERNAL_ERROR,
                format("RaptorColumnIdentity is corrupt: %s", base16().upperCase().encode(bytes)));
    }

    @Override
    public byte[] serialize()
    {
        byte[] result = new byte[Byte.BYTES + Long.BYTES];
        result[0] = version;
        arraycopy(Longs.toByteArray(columnId), 0, result, Byte.BYTES, Long.BYTES);
        return result;
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
                .add("version", version)
                .add("columnId", columnId)
                .toString();
    }
}
