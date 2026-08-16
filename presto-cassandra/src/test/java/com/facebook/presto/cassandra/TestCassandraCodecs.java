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

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Unit tests for the custom type codecs introduced during the Cassandra Java Driver 4.x migration.
 * <p>
 * {@link IntToLocalDateCodec} stores Cassandra DATE values as INT (days since epoch) to avoid the
 * "date" reserved-keyword issue, and {@link TimestampCodec} restores java.sql.Timestamp support that
 * driver 4.x dropped. Both are pure and exercised here without a live Cassandra cluster.
 */
public class TestCassandraCodecs
{
    private static final DefaultProtocolVersion VERSION = DefaultProtocolVersion.V4;

    @Test
    public void testIntToLocalDateCodecCqlType()
    {
        // DATE values are physically stored as INT, so the codec must advertise the INT CQL type.
        assertEquals(new IntToLocalDateCodec().getCqlType(), DataTypes.INT);
    }

    @Test
    public void testIntToLocalDateCodecRoundTrip()
    {
        IntToLocalDateCodec codec = new IntToLocalDateCodec();
        for (LocalDate date : new LocalDate[] {
                LocalDate.of(1970, 1, 1),
                LocalDate.of(1969, 12, 31),
                LocalDate.of(2026, 5, 30),
                LocalDate.of(5555, 6, 15),
                LocalDate.of(9999, 12, 31)}) {
            ByteBuffer encoded = codec.encode(date, VERSION);
            assertEquals(codec.decode(encoded, VERSION), date, "round trip failed for " + date);
        }
    }

    @Test
    public void testIntToLocalDateCodecEpochEncoding()
    {
        IntToLocalDateCodec codec = new IntToLocalDateCodec();
        // 1970-01-01 is day 0; the on-wire representation is a 4-byte int.
        ByteBuffer encoded = codec.encode(LocalDate.ofEpochDay(0), VERSION);
        assertEquals(encoded.remaining(), Integer.BYTES);
        assertEquals(encoded.getInt(encoded.position()), 0);
        // 1970-01-02 is day 1.
        assertEquals(codec.encode(LocalDate.ofEpochDay(1), VERSION).getInt(0), 1);
    }

    @Test
    public void testIntToLocalDateCodecNull()
    {
        IntToLocalDateCodec codec = new IntToLocalDateCodec();
        assertNull(codec.encode(null, VERSION));
        assertNull(codec.decode(null, VERSION));
    }

    @Test
    public void testTimestampCodecCqlType()
    {
        assertEquals(TimestampCodec.INSTANCE.getCqlType(), DataTypes.TIMESTAMP);
    }

    @Test
    public void testTimestampCodecRoundTrip()
    {
        Timestamp value = Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 3, 4, 5, 0));
        ByteBuffer encoded = TimestampCodec.INSTANCE.encode(value, VERSION);
        assertEquals(encoded.remaining(), Long.BYTES);
        assertEquals(TimestampCodec.INSTANCE.decode(encoded, VERSION), value);
    }

    @Test
    public void testTimestampCodecPreEpochRoundTrip()
    {
        Timestamp value = Timestamp.valueOf(LocalDateTime.of(1969, 12, 31, 23, 4, 5));
        ByteBuffer encoded = TimestampCodec.INSTANCE.encode(value, VERSION);
        assertEquals(TimestampCodec.INSTANCE.decode(encoded, VERSION), value);
    }

    @Test
    public void testTimestampCodecFormatParseRoundTrip()
    {
        Timestamp value = Timestamp.from(java.time.Instant.parse("2026-05-30T12:34:56Z"));
        String formatted = TimestampCodec.INSTANCE.format(value);
        assertEquals(TimestampCodec.INSTANCE.parse(formatted), value);
    }

    @Test
    public void testTimestampCodecNull()
    {
        assertNull(TimestampCodec.INSTANCE.encode(null, VERSION));
        assertNull(TimestampCodec.INSTANCE.decode(null, VERSION));
        assertNull(TimestampCodec.INSTANCE.decode(ByteBuffer.allocate(0), VERSION));
        assertEquals(TimestampCodec.INSTANCE.format(null), "NULL");
        assertNull(TimestampCodec.INSTANCE.parse("NULL"));
        assertNull(TimestampCodec.INSTANCE.parse(null));
        assertNull(TimestampCodec.INSTANCE.parse(""));
    }
}
