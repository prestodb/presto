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
package com.facebook.presto.hive.util;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveOrcAggregatedMemoryContext;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.AbstractIterator;
import io.airlift.units.DataSize;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class TempFileReader
        extends AbstractIterator<Page>
{
    private final int columnCount;
    private final OrcBatchRecordReader reader;

    public TempFileReader(List<Type> types, OrcDataSource dataSource)
    {
        requireNonNull(types, "types is null");
        this.columnCount = types.size();

        try {
            OrcReader orcReader = new OrcReader(
                    dataSource,
                    ORC,
                    new StorageOrcFileTailSource(),
                    new StorageStripeMetadataSource(),
                    new HiveOrcAggregatedMemoryContext(),
                    new OrcReaderOptions(
                            new DataSize(1, MEGABYTE),
                            new DataSize(8, MEGABYTE),
                            new DataSize(16, MEGABYTE),
                            false),
                    false,
                    NO_ENCRYPTION,
                    DwrfKeyProvider.EMPTY,
                    new RuntimeStats());

            Map<Integer, Type> includedColumns = new HashMap<>();
            for (int i = 0; i < types.size(); i++) {
                includedColumns.put(i, types.get(i));
            }

            reader = orcReader.createBatchRecordReader(
                    includedColumns,
                    OrcPredicate.TRUE,
                    UTC,
                    new HiveOrcAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, "Failed to read temporary data");
        }
    }

    @Override
    protected Page computeNext()
    {
        try {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }

            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                return endOfData();
            }

            Block[] blocks = new Block[columnCount];
            for (int i = 0; i < columnCount; i++) {
                blocks[i] = reader.readBlock(i).getLoadedBlock();
            }
            return new Page(batchSize, blocks);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, "Failed to read temporary data");
        }
    }
}
