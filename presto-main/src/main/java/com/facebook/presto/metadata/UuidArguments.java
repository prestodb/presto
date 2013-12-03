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
package com.facebook.presto.metadata;

import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.Argument;
import org.skife.jdbi.v2.tweak.ArgumentFactory;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public final class UuidArguments
{
    private UuidArguments() {}

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
        private final byte[] value;

        public UuidArgument(UUID value)
        {
            this.value = uuidToBytes(checkNotNull(value, "value is null"));
        }

        @Override
        public void apply(int position, PreparedStatement statement, StatementContext ctx)
                throws SQLException
        {
            statement.setBytes(position, value);
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
}
