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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.raptor.RaptorOrcAggregatedMemoryContext;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

final class OrcTestingUtil
{
    private OrcTestingUtil() {}

    public static OrcDataSource fileOrcDataSource(File file)
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
    }

    public static OrcBatchRecordReader createReader(OrcDataSource dataSource, List<Long> columnIds, List<Type> types)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(
                dataSource,
                ORC,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                new RaptorOrcAggregatedMemoryContext(),
                createDefaultTestConfig(),
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY,
                new RuntimeStats());

        List<String> columnNames = orcReader.getColumnNames();
        assertEquals(columnNames.size(), columnIds.size());

        Map<Integer, Type> includedColumns = new HashMap<>();
        int ordinal = 0;
        for (long columnId : columnIds) {
            assertEquals(columnNames.get(ordinal), String.valueOf(columnId));
            includedColumns.put(ordinal, types.get(ordinal));
            ordinal++;
        }

        return createRecordReader(orcReader, includedColumns);
    }

    public static OrcBatchRecordReader createRecordReader(OrcReader orcReader, Map<Integer, Type> includedColumns)
            throws OrcCorruptionException
    {
        Metadata metadata = MetadataManager.createTestMetadataManager();
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        StorageTypeConverter storageTypeConverter = new StorageTypeConverter(functionAndTypeManager);
        return orcReader.createBatchRecordReader(
                storageTypeConverter.toStorageTypes(includedColumns),
                OrcPredicate.TRUE,
                DateTimeZone.UTC,
                new RaptorOrcAggregatedMemoryContext(),
                MAX_BATCH_SIZE);
    }

    public static byte[] octets(int... values)
    {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = octet(values[i]);
        }
        return bytes;
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static byte octet(int b)
    {
        checkArgument((b >= 0) && (b <= 0xFF), "octet not in range: %s", b);
        return (byte) b;
    }

    public static FileWriter createFileWriter(List<Long> columnIds, List<Type> columnTypes, File file)
            throws IOException
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        return new OrcFileWriter(columnIds, columnTypes, new OutputStreamDataSink(new FileOutputStream(file)), true, true, new OrcWriterStats(), functionAndTypeManager, ZSTD);
    }

    public static OrcReaderOptions createDefaultTestConfig()
    {
        return new OrcReaderOptions(
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                false);
    }
}
