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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableIdentity;
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

public class RaptorTableIdentity
        implements TableIdentity
{
    private static final byte CURRENT_VERSION = 1;

    private final byte version;
    private final long tableId;

    public RaptorTableIdentity(long tableId)
    {
        this(CURRENT_VERSION, tableId);
    }

    private RaptorTableIdentity(byte version, long tableId)
    {
        checkArgument(version > 0 && version <= CURRENT_VERSION, "invalid version %s", version);
        checkArgument(tableId > 0, "tableId must be a positive integer");
        this.version = version;
        this.tableId = tableId;
    }

    public static RaptorTableIdentity deserialize(byte[] bytes)
    {
        requireNonNull(bytes, "bytes for RaptorTableIdentity is null");
        checkArgument(bytes.length >= Byte.BYTES, "bytes for RaptorTableIdentity is empty");

        // The first byte is always for version.
        byte version = bytes[0];
        if (version == CURRENT_VERSION && bytes.length == Byte.BYTES + Long.BYTES) {
            // The following 8 bytes are for tableId.
            long tableId = fromByteArray(copyOfRange(bytes, Byte.BYTES, Byte.BYTES + Long.BYTES));
            return new RaptorTableIdentity(version, tableId);
        }

        throw new PrestoException(
                GENERIC_INTERNAL_ERROR,
                format("RaptorTableIdentity is corrupt: %s", base16().upperCase().encode(bytes)));
    }

    @Override
    public byte[] serialize()
    {
        byte[] result = new byte[Byte.BYTES + Long.BYTES];
        result[0] = version;
        arraycopy(Longs.toByteArray(tableId), 0, result, Byte.BYTES, Long.BYTES);
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
        RaptorTableIdentity that = (RaptorTableIdentity) o;
        return tableId == that.tableId;
    }

    @Override
    public int hashCode()
    {
        return hash(tableId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("version", version)
                .add("tableId", tableId)
                .toString();
    }
}
