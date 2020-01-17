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
package com.facebook.presto.parquet;

import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.parquet.io.api.Binary;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.sql.Timestamp;

import static com.facebook.presto.parquet.ParquetTimestampUtils.getTimestampMillis;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils.getNanoTime;
import static org.apache.parquet.io.api.Binary.fromConstantByteBuffer;
import static org.testng.Assert.assertEquals;

public class TestParquetTimestampUtils
{
    @Test
    public void testGetTimestampMillis()
    {
        assertTimestampCorrect("2011-01-01 00:00:00.000000000");
        assertTimestampCorrect("2001-01-01 01:01:01.000000001");
        assertTimestampCorrect("2015-12-31 23:59:59.999999999");
    }

    @Test
    public void testInvalidBinaryLength()
    {
        try {
            byte[] invalidLengthBinaryTimestamp = new byte[8];
            getTimestampMillis(Binary.fromByteArray(invalidLengthBinaryTimestamp));
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertEquals(e.getMessage(), "Parquet timestamp must be 12 bytes, actual 8");
        }
    }

    private static void assertTimestampCorrect(String timestampString)
    {
        Timestamp timestamp = Timestamp.valueOf(timestampString);
        NanoTime nanoTime = getNanoTime(timestamp, false);
        ByteBuffer buffer = ByteBuffer.wrap(nanoTime.toBinary().getBytes());
        long decodedTimestampMillis = getTimestampMillis(fromConstantByteBuffer(buffer));
        assertEquals(decodedTimestampMillis, timestamp.getTime());
    }
}
