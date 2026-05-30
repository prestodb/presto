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
package com.facebook.presto.cassandra;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Custom codec for converting between Cassandra TIMESTAMP and java.sql.Timestamp.
 * Driver 4.x removed built-in support for java.sql.Timestamp, only supporting java.time.Instant.
 * This codec bridges the gap for legacy code that uses java.sql.Timestamp.
 */
public class TimestampCodec
        implements TypeCodec<Timestamp>
{
    public static final TimestampCodec INSTANCE = new TimestampCodec();

    private TimestampCodec()
    {
        // Singleton
    }

    @Nonnull
    @Override
    public GenericType<Timestamp> getJavaType()
    {
        return GenericType.of(Timestamp.class);
    }

    @Nonnull
    @Override
    public DataType getCqlType()
    {
        return DataTypes.TIMESTAMP;
    }

    @Nullable
    @Override
    public ByteBuffer encode(@Nullable Timestamp value, @Nonnull ProtocolVersion protocolVersion)
    {
        if (value == null) {
            return null;
        }
        // Convert Timestamp to Instant, then encode as milliseconds since epoch
        Instant instant = value.toInstant();
        long millisSinceEpoch = instant.toEpochMilli();
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(0, millisSinceEpoch);
        return buffer;
    }

    @Nullable
    @Override
    public Timestamp decode(@Nullable ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion)
    {
        if (bytes == null || bytes.remaining() == 0) {
            return null;
        }
        // Decode milliseconds since epoch and convert to Timestamp
        long millisSinceEpoch = bytes.getLong(bytes.position());
        return new Timestamp(millisSinceEpoch);
    }

    @Nonnull
    @Override
    public String format(@Nullable Timestamp value)
    {
        if (value == null) {
            return "NULL";
        }
        return value.toInstant().toString();
    }

    @Nullable
    @Override
    public Timestamp parse(@Nullable String value)
    {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
            return null;
        }
        try {
            // Parse as ISO-8601 instant
            Instant instant = Instant.parse(value);
            return Timestamp.from(instant);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse timestamp value: " + value, e);
        }
    }
}
