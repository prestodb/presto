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
package com.facebook.presto.orc;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.writer.SliceDictionaryColumnWriter;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.OrcWriterOptions.DEFAULT_MAX_COMPRESSION_BUFFER_SIZE;
import static com.facebook.presto.orc.OrcWriterOptions.DEFAULT_MAX_STRING_STATISTICS_LIMIT;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertFalse;

public class TestSliceDictionaryColumnWriter
{
    @Test
    public void testDirectConversion()
    {
        CompressionParameters compressionParameters = new CompressionParameters(
                CompressionKind.NONE,
                OptionalInt.empty(),
                toIntExact(DEFAULT_MAX_COMPRESSION_BUFFER_SIZE.toBytes()));
        SliceDictionaryColumnWriter writer = new SliceDictionaryColumnWriter(
                0,
                VARCHAR,
                compressionParameters,
                Optional.empty(),
                OrcEncoding.ORC,
                DEFAULT_MAX_STRING_STATISTICS_LIMIT,
                OrcEncoding.ORC.createMetadataWriter());

        // a single row group exceeds 2G after direct conversion
        byte[] value = new byte[megabytes(1)];
        ThreadLocalRandom.current().nextBytes(value);
        Block data = RunLengthEncodedBlock.create(VARCHAR, Slices.wrappedBuffer(value), 3000);
        writer.beginRowGroup();
        writer.writeBlock(data);
        writer.finishRowGroup();

        assertFalse(writer.tryConvertToDirect(megabytes(64)).isPresent());
    }

    private static int megabytes(int size)
    {
        return toIntExact(new DataSize(size, MEGABYTE).toBytes());
    }
}
