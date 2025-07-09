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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.orc.DefaultOrcWriterFlushPolicy;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.common.ErrorType.EXTERNAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.parquet.ParquetTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static org.testng.Assert.assertEquals;

public class TestOrcFileWriter
{
    private static final ErrorCode STORAGE_ERROR_CODE = new ErrorCode(123, "STORAGE_ERROR_CODE", EXTERNAL);

    @Test
    public void testPrestoExceptionPropagation()
    {
        // This test is to verify that a PrestoException thrown by the underlying data sink implementation is propagated as is
        OrcFileWriter orcFileWriter = createOrcFileWriter(true);

        try {
            // Throws PrestoException with STORAGE_ERROR error code
            orcFileWriter.commit();
        }
        catch (Exception e) {
            assertEquals(e.getClass(), PrestoException.class);
            assertEquals(e.getMessage(), "Dummy PrestoException from mocked data sink instance");
            assertEquals(((PrestoException) e).getErrorCode(), STORAGE_ERROR_CODE);
        }
    }

    @Test
    public void testIOExceptionPropagation()
    {
        // This test is to verify that a IOException thrown by the underlying data sink implementation is wrapped into a PrestoException.
        OrcFileWriter orcFileWriter = createOrcFileWriter(false);

        try {
            // Throws PrestoException with HIVE_WRITER_CLOSE_ERROR error code
            orcFileWriter.commit();
        }
        catch (Exception e) {
            assertEquals(e.getClass(), PrestoException.class);
            assertEquals(e.getMessage(), "Error committing write to Hive. Dummy IOException from mocked data sink instance");
            assertEquals(((PrestoException) e).getErrorCode(), HIVE_WRITER_CLOSE_ERROR.toErrorCode());
            assertEquals(e.getCause().getClass(), IOException.class);
        }
    }

    private OrcFileWriter createOrcFileWriter(boolean prestoExceptionThrowingOrcFileWriter)
    {
        return new OrcFileWriter(
                new MockDataSink(prestoExceptionThrowingOrcFileWriter),
                () -> null,
                ORC,
                ImmutableList.of("test1"),
                ImmutableList.of(VARCHAR),
                NONE,
                OrcWriterOptions.builder()
                        .withFlushPolicy(DefaultOrcWriterFlushPolicy.builder()
                                .withStripeMinSize(new DataSize(0, MEGABYTE))
                                .withStripeMaxSize(new DataSize(32, MEGABYTE))
                                .withStripeMaxRowCount(10)
                                .build())
                        .withRowGroupMaxRowCount(10_000)
                        .withDictionaryMaxMemory(new DataSize(32, MEGABYTE))
                        .build(),
                new int[0],
                ImmutableMap.of(),
                HIVE_STORAGE_TIME_ZONE,
                Optional.empty(),
                null,
                NOOP_WRITER_STATS,
                NO_ENCRYPTION,
                Optional.empty());
    }

    public static class MockDataSink
            implements DataSink
    {
        boolean throwPrestoException;

        public MockDataSink(boolean throwPrestoException)
        {
            this.throwPrestoException = throwPrestoException;
        }

        @Override
        public long size()
        {
            return -1L;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return -1L;
        }

        @Override
        public void write(List<DataOutput> outputData)
        {
        }

        @Override
        public void close()
                throws IOException
        {
            if (throwPrestoException) {
                throw new PrestoException(() -> STORAGE_ERROR_CODE, "Dummy PrestoException from mocked data sink instance");
            }
            throw new IOException("Dummy IOException from mocked data sink instance");
        }
    }
}
