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

import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class TempFileReader
        extends AbstractIterator<Page>
{
    private final List<Type> types;
    private final OrcRecordReader reader;

    public TempFileReader(List<Type> types, OrcDataSource dataSource)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));

        try {
            OrcReader orcReader = new OrcReader(
                    dataSource,
                    ORC,
                    new DataSize(1, MEGABYTE),
                    new DataSize(8, MEGABYTE),
                    new DataSize(8, MEGABYTE),
                    new DataSize(16, MEGABYTE));

            Map<Integer, Type> includedColumns = new HashMap<>();
            for (int i = 0; i < types.size(); i++) {
                includedColumns.put(i, types.get(i));
            }

            reader = orcReader.createRecordReader(
                    includedColumns,
                    OrcPredicate.TRUE,
                    UTC,
                    newSimpleAggregatedMemoryContext());
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

            Block[] blocks = new Block[types.size()];
            for (int i = 0; i < types.size(); i++) {
                blocks[i] = reader.readBlock(types.get(i), i).getLoadedBlock();
            }
            return new Page(batchSize, blocks);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, "Failed to read temporary data");
        }
    }
}
