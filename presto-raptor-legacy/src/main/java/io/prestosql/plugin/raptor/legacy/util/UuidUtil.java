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
package io.prestosql.plugin.raptor.legacy.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.skife.jdbi.v2.ResultSetMapperFactory;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.TypedMapper;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;

public final class UuidUtil
{
    private UuidUtil() {}

    public static final class UuidArgumentFactory
            implements ArgumentFactory<UUID>
    {
        @Override
        public boolean accepts(Class<?> expectedType, Object value, StatementContext ctx)
        {
            return value instanceof UUID;
        }

        @Override
        public Argument build(Class<?> expectedType, UUID value, StatementContext ctx)
        {
            return new UuidArgument(value);
        }
    }

    public static final class UuidArgument
            implements Argument
    {
        private final UUID uuid;

        public UuidArgument(UUID uuid)
        {
            this.uuid = uuid;
        }

        @Override
        public void apply(int position, PreparedStatement statement, StatementContext ctx)
                throws SQLException
        {
            if (uuid == null) {
                statement.setNull(position, Types.VARBINARY);
            }
            else {
                statement.setBytes(position, uuidToBytes(uuid));
            }
        }

        @Override
        public String toString()
        {
            return String.valueOf(uuid);
        }
    }

    @SuppressWarnings("rawtypes")
    public static class UuidMapperFactory
            implements ResultSetMapperFactory
    {
        @Override
        public boolean accepts(Class type, StatementContext ctx)
        {
            return type == UUID.class;
        }

        @Override
        public ResultSetMapper mapperFor(Class type, StatementContext ctx)
        {
            return new UuidMapper();
        }
    }

    public static final class UuidMapper
            extends TypedMapper<UUID>
    {
        @Override
        protected UUID extractByName(ResultSet r, String name)
                throws SQLException
        {
            return uuidFromBytes(r.getBytes(name));
        }

        @Override
        protected UUID extractByIndex(ResultSet r, int index)
                throws SQLException
        {
            return uuidFromBytes(r.getBytes(index));
        }
    }

    public static UUID uuidFromBytes(byte[] bytes)
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        return new UUID(msb, lsb);
    }

    public static byte[] uuidToBytes(UUID uuid)
    {
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    /**
     * @param uuidSlice textual representation of UUID
     * @return byte representation of UUID
     * @throws IllegalArgumentException if uuidSlice is not a valid string representation of UUID
     */
    public static Slice uuidStringToBytes(Slice uuidSlice)
    {
        UUID uuid = UUID.fromString(uuidSlice.toStringUtf8());
        byte[] bytes = uuidToBytes(uuid);
        return Slices.wrappedBuffer(bytes);
    }
}
