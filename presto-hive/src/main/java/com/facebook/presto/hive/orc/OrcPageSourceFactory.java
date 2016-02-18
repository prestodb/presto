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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
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

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_MISSING_COLUMN_NAMES;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveUtil.isDeserializerClass;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcPageSourceFactory
        implements HivePageSourceFactory
{
    private static final Pattern DEFAULT_HIVE_COLUMN_NAME_PATTERN = Pattern.compile("_col\\d+");
    private final TypeManager typeManager;
    private final boolean useOrcColumnNames;

    @Inject
    public OrcPageSourceFactory(TypeManager typeManager, HiveClientConfig config)
    {
        this(typeManager, requireNonNull(config, "hiveClientConfig is null").isUseOrcColumnNames());
    }

    public OrcPageSourceFactory(TypeManager typeManager, boolean useOrcColumnNames)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.useOrcColumnNames = useOrcColumnNames;
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!isDeserializerClass(schema, OrcSerde.class)) {
            return Optional.empty();
        }

        return Optional.of(createOrcPageSource(
                new OrcMetadataReader(),
                configuration,
                path,
                start,
                length,
                columns,
                partitionKeys,
                useOrcColumnNames,
                effectivePredicate,
                hiveStorageTimeZone,
                typeManager,
                getOrcMaxMergeDistance(session),
                getOrcMaxBufferSize(session),
                getOrcStreamBufferSize(session)));
    }

    public static OrcPageSource createOrcPageSource(MetadataReader metadataReader,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            boolean useOrcColumnNames,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize streamBufferSize)
    {
        OrcDataSource orcDataSource;
        try {
            FileSystem fileSystem = path.getFileSystem(configuration);
            long size = fileSystem.getFileStatus(path).getLen();
            FSDataInputStream inputStream = fileSystem.open(path);
            orcDataSource = new HdfsOrcDataSource(path.toString(), size, maxMergeDistance, maxBufferSize, streamBufferSize, inputStream);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        AggregatedMemoryContext systemMemoryUsage = new AggregatedMemoryContext();
        try {
            OrcReader reader = new OrcReader(orcDataSource, metadataReader, maxMergeDistance, maxBufferSize);

            List<HiveColumnHandle> physicalColumns = getPhysicalHiveColumnHandles(columns, useOrcColumnNames, reader, path);
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
            for (HiveColumnHandle column : physicalColumns) {
                if (!column.isPartitionKey()) {
                    Type type = typeManager.getType(column.getTypeSignature());
                    includedColumns.put(column.getHiveColumnIndex(), type);
                    columnReferences.add(new ColumnReference<>(column, column.getHiveColumnIndex(), type));
                }
            }

            OrcPredicate predicate = new TupleDomainOrcPredicate<>(effectivePredicate, columnReferences.build());

            OrcRecordReader recordReader = reader.createRecordReader(
                    includedColumns.build(),
                    predicate,
                    start,
                    length,
                    hiveStorageTimeZone,
                    systemMemoryUsage);

            return new OrcPageSource(
                    recordReader,
                    orcDataSource,
                    partitionKeys,
                    physicalColumns,
                    hiveStorageTimeZone,
                    typeManager,
                    systemMemoryUsage);
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
            physicalColumns.add(new HiveColumnHandle(column.getClientId(), column.getName(), column.getHiveType(), column.getTypeSignature(), physicalOrdinal, column.isPartitionKey()));
        }
        return physicalColumns.build();
    }

    private static void verifyFileHasColumnNames(List<String> physicalColumnNames, Path path)
    {
        if (physicalColumnNames.stream().allMatch(physicalColumnName -> DEFAULT_HIVE_COLUMN_NAME_PATTERN.matcher(physicalColumnName).matches())) {
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
