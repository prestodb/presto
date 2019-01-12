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
package io.prestosql.plugin.hive.util;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcEncoding.ORC;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
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
                    newSimpleAggregatedMemoryContext(),
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
