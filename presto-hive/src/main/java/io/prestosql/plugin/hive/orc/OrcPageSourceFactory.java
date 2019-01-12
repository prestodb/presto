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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcEncoding;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.orc.TupleDomainOrcPredicate;
import io.prestosql.orc.TupleDomainOrcPredicate.ColumnReference;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveClientConfig;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.nullToEmpty;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcEncoding.ORC;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILE_MISSING_COLUMN_NAMES;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcBloomFiltersEnabled;
import static io.prestosql.plugin.hive.HiveUtil.isDeserializerClass;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcPageSourceFactory
        implements HivePageSourceFactory
{
    private static final Pattern DEFAULT_HIVE_COLUMN_NAME_PATTERN = Pattern.compile("_col\\d+");
    private final TypeManager typeManager;
    private final boolean useOrcColumnNames;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;

    @Inject
    public OrcPageSourceFactory(TypeManager typeManager, HiveClientConfig config, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats)
    {
        this(typeManager, requireNonNull(config, "hiveClientConfig is null").isUseOrcColumnNames(), hdfsEnvironment, stats);
    }

    public OrcPageSourceFactory(TypeManager typeManager, boolean useOrcColumnNames, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.useOrcColumnNames = useOrcColumnNames;
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!isDeserializerClass(schema, OrcSerde.class)) {
            return Optional.empty();
        }

        // per HIVE-13040 and ORC-162, empty files are allowed
        if (fileSize == 0) {
            return Optional.of(new FixedPageSource(ImmutableList.of()));
        }

        return Optional.of(createOrcPageSource(
                ORC,
                hdfsEnvironment,
                session.getUser(),
                configuration,
                path,
                start,
                length,
                fileSize,
                columns,
                useOrcColumnNames,
                effectivePredicate,
                hiveStorageTimeZone,
                typeManager,
                getOrcMaxMergeDistance(session),
                getOrcMaxBufferSize(session),
                getOrcStreamBufferSize(session),
                getOrcTinyStripeThreshold(session),
                getOrcMaxReadBlockSize(session),
                getOrcLazyReadSmallRanges(session),
                isOrcBloomFiltersEnabled(session),
                stats));
    }

    public static OrcPageSource createOrcPageSource(
            OrcEncoding orcEncoding,
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<HiveColumnHandle> columns,
            boolean useOrcColumnNames,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            DataSize tinyStripeThreshold,
            DataSize maxReadBlockSize,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats)
    {
        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
            FSDataInputStream inputStream = fileSystem.open(path);
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    maxMergeDistance,
                    maxBufferSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
        try {
            OrcReader reader = new OrcReader(orcDataSource, orcEncoding, maxMergeDistance, maxBufferSize, tinyStripeThreshold, maxReadBlockSize);

            List<HiveColumnHandle> physicalColumns = getPhysicalHiveColumnHandles(columns, useOrcColumnNames, reader, path);
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
            for (HiveColumnHandle column : physicalColumns) {
                if (column.getColumnType() == REGULAR) {
                    Type type = typeManager.getType(column.getTypeSignature());
                    includedColumns.put(column.getHiveColumnIndex(), type);
                    columnReferences.add(new ColumnReference<>(column, column.getHiveColumnIndex(), type));
                }
            }

            OrcPredicate predicate = new TupleDomainOrcPredicate<>(effectivePredicate, columnReferences.build(), orcBloomFiltersEnabled);

            OrcRecordReader recordReader = reader.createRecordReader(
                    includedColumns.build(),
                    predicate,
                    start,
                    length,
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE);

            return new OrcPageSource(
                    recordReader,
                    orcDataSource,
                    physicalColumns,
                    typeManager,
                    systemMemoryUsage,
                    stats);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = splitError(e, path, start, length);
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }

    private static List<HiveColumnHandle> getPhysicalHiveColumnHandles(List<HiveColumnHandle> columns, boolean useOrcColumnNames, OrcReader reader, Path path)
    {
        if (!useOrcColumnNames) {
            return columns;
        }

        verifyFileHasColumnNames(reader.getColumnNames(), path);

        Map<String, Integer> physicalNameOrdinalMap = buildPhysicalNameOrdinalMap(reader);
        int nextMissingColumnIndex = physicalNameOrdinalMap.size();

        ImmutableList.Builder<HiveColumnHandle> physicalColumns = ImmutableList.builder();
        for (HiveColumnHandle column : columns) {
            Integer physicalOrdinal = physicalNameOrdinalMap.get(column.getName());
            if (physicalOrdinal == null) {
                // if the column is missing from the file, assign it a column number larger
                // than the number of columns in the file so the reader will fill it with nulls
                physicalOrdinal = nextMissingColumnIndex;
                nextMissingColumnIndex++;
            }
            physicalColumns.add(new HiveColumnHandle(column.getName(), column.getHiveType(), column.getTypeSignature(), physicalOrdinal, column.getColumnType(), column.getComment()));
        }
        return physicalColumns.build();
    }

    private static void verifyFileHasColumnNames(List<String> physicalColumnNames, Path path)
    {
        if (!physicalColumnNames.isEmpty() && physicalColumnNames.stream().allMatch(physicalColumnName -> DEFAULT_HIVE_COLUMN_NAME_PATTERN.matcher(physicalColumnName).matches())) {
            throw new PrestoException(
                    HIVE_FILE_MISSING_COLUMN_NAMES,
                    "ORC file does not contain column names in the footer: " + path);
        }
    }

    private static Map<String, Integer> buildPhysicalNameOrdinalMap(OrcReader reader)
    {
        ImmutableMap.Builder<String, Integer> physicalNameOrdinalMap = ImmutableMap.builder();

        int ordinal = 0;
        for (String physicalColumnName : reader.getColumnNames()) {
            physicalNameOrdinalMap.put(physicalColumnName, ordinal);
            ordinal++;
        }

        return physicalNameOrdinalMap.build();
    }
}
