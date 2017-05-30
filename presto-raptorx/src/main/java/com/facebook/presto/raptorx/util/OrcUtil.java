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
package com.facebook.presto.raptorx.util;

import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcDataSink;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.OutputStreamOrcDataSink;
import com.facebook.presto.raptorx.storage.CompressionType;
import com.facebook.presto.raptorx.storage.ReaderAttributes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import io.airlift.units.DataSize;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_STORAGE_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.joda.time.DateTimeZone.UTC;

public final class OrcUtil
{
    private OrcUtil() {}

    public static FileOrcDataSource fileOrcDataSource(File file, ReaderAttributes attributes)
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file, attributes.getMaxMergeDistance(), attributes.getMaxReadSize(), attributes.getStreamBufferSize(), true);
    }

    public static OrcReader createOrcReader(OrcDataSource dataSource, ReaderAttributes attributes)
            throws IOException
    {
        return new OrcReader(dataSource, ORC, attributes.getMaxMergeDistance(), attributes.getMaxReadSize(), attributes.getTinyStripeThreshold(), new DataSize(16, MEGABYTE));
    }

    public static OrcRecordReader createOrcRecordReader(OrcReader reader, Map<Integer, Type> includedColumns)
    {
        return reader.createRecordReader(includedColumns, OrcPredicate.TRUE, UTC, newSimpleAggregatedMemoryContext(), MAX_BATCH_SIZE);
    }

    public static OrcDataSink createOrcDataSink(File target)
    {
        try {
            return new OutputStreamOrcDataSink(new SyncingFileOutputStream(target));
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_STORAGE_ERROR, "Failed to open output file: " + target, e);
        }
    }

    public static OrcWriter createOrcFileWriter(
            File target,
            List<Long> columnIds,
            List<Type> storageTypes,
            CompressionType compressionType,
            Map<String, String> metadata,
            OrcWriterStats stats)
    {
        checkArgument(columnIds.size() == storageTypes.size(), "ids and types mismatch");
        checkArgument(isUnique(columnIds), "ids must be unique");

        List<String> columnNames = columnIds.stream()
                .map(Object::toString)
                .collect(toImmutableList());

        return new OrcWriter(
                createOrcDataSink(target),
                columnNames,
                storageTypes,
                ORC,
                compressionType.compressionKind(),
                new OrcWriterOptions(),
                metadata,
                UTC,
                true,
                BOTH,
                stats);
    }

    private static <T> boolean isUnique(Collection<T> items)
    {
        return items.stream().distinct().count() == items.size();
    }
}
