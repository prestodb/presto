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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.metadata.CompressionKind.LZ4;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;

public class TestOrcLz4
{
    private static final DataSize SIZE = new DataSize(1, MEGABYTE);

    @Test
    public void testReadLz4()
            throws Exception
    {
        // this file was written with Apache ORC
        // TODO: use Apache ORC library in OrcTester
        byte[] data = toByteArray(getResource("apache-lz4.orc"));

        OrcReader orcReader = new OrcReader(
                new InMemoryOrcDataSource(data),
                ORC,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                new OrcReaderOptions(
                        SIZE,
                        SIZE,
                        SIZE,
                        false),
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats());

        assertEquals(orcReader.getCompressionKind(), LZ4);
        assertEquals(orcReader.getFooter().getNumberOfRows(), 10_000);

        Map<Integer, Type> includedColumns = ImmutableMap.<Integer, Type>builder()
                .put(0, BIGINT)
                .put(1, INTEGER)
                .put(2, BIGINT)
                .build();

        OrcBatchRecordReader reader = orcReader.createBatchRecordReader(
                includedColumns,
                OrcPredicate.TRUE,
                DateTimeZone.UTC,
                new TestingHiveOrcAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE);

        int rows = 0;
        while (true) {
            int batchSize = reader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            rows += batchSize;

            Block xBlock = reader.readBlock(0);
            Block yBlock = reader.readBlock(1);
            Block zBlock = reader.readBlock(2);

            for (int position = 0; position < batchSize; position++) {
                BIGINT.getLong(xBlock, position);
                INTEGER.getLong(yBlock, position);
                BIGINT.getLong(zBlock, position);
            }
        }

        assertEquals(rows, reader.getFileRowCount());
    }

    private static class InMemoryOrcDataSource
            extends AbstractOrcDataSource
    {
        private final byte[] data;

        public InMemoryOrcDataSource(byte[] data)
        {
            super(new OrcDataSourceId("memory"), data.length, SIZE, SIZE, SIZE, false);
            this.data = data;
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            System.arraycopy(data, toIntExact(position), buffer, bufferOffset, bufferLength);
        }
    }
}
