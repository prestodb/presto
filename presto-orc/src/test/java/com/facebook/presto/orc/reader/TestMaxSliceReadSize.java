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
package com.facebook.presto.orc.reader;

import com.facebook.presto.common.GenericInternalException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.OrcSelectiveRecordReader;
import com.facebook.presto.orc.OrcTester;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.TempFile;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestMaxSliceReadSize
{
    @Test
    public void test()
            throws Exception
    {
        SqlVarbinary value = new SqlVarbinary(new byte[10 * 1024]);

        try (TempFile tempFile = new TempFile()) {
            OrcTester.writeOrcColumnsPresto(
                    tempFile.getFile(),
                    DWRF,
                    CompressionKind.NONE,
                    Optional.empty(),
                    ImmutableList.of(VARBINARY),
                    ImmutableList.of(ImmutableList.of(value)),
                    NOOP_WRITER_STATS);

            OrcSelectiveRecordReader readerNoLimits = createReader(tempFile, new DataSize(1, MEGABYTE));
            Page page = readerNoLimits.getNextPage().getLoadedPage();
            assertEquals(page.getPositionCount(), 1);

            OrcSelectiveRecordReader readerBelowThreshold = createReader(tempFile, new DataSize(1, KILOBYTE));
            GenericInternalException exception = expectThrows(GenericInternalException.class, () -> readerBelowThreshold.getNextPage().getLoadedPage());
            assertTrue(exception.getMessage().startsWith("Values in column \"test\" are too large to process for Presto. Requested to read [10240] bytes, when max allowed is [1024] bytes "));
        }
    }

    private static OrcSelectiveRecordReader createReader(TempFile tempFile, DataSize maxSliceSize)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(
                tempFile.getFile(),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                true);

        OrcReaderOptions options = OrcReaderOptions.builder()
                .withMaxMergeDistance(new DataSize(1, MEGABYTE))
                .withTinyStripeThreshold(new DataSize(1, MEGABYTE))
                .withMaxBlockSize(new DataSize(1, MEGABYTE))
                .withMaxSliceSize(maxSliceSize)
                .build();

        OrcReader orcReader = new OrcReader(
                orcDataSource,
                OrcEncoding.DWRF,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                options,
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats());

        return orcReader.createSelectiveRecordReader(
                ImmutableMap.of(0, VARBINARY),
                ImmutableList.of(0),
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                OrcPredicate.TRUE,
                0,
                orcDataSource.getSize(),
                HIVE_STORAGE_TIME_ZONE,
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                Optional.empty(),
                1);
    }
}
