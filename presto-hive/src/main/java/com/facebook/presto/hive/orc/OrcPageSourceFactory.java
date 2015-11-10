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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_ORC_COLUMN_NAME_LOOKUPS_NOT_SUPPORTED;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.isOptimizedReaderEnabled;
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
        if (!isOptimizedReaderEnabled(session)) {
            return Optional.empty();
        }

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

        try {
            OrcReader reader = new OrcReader(orcDataSource, metadataReader, maxMergeDistance, maxBufferSize);

            List<HiveColumnHandle> physicalColumns = getPhysicalHiveColumnHandles(columns, useOrcColumnNames, reader);
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
                    hiveStorageTimeZone);

            return new OrcPageSource(
                    recordReader,
                    orcDataSource,
                    partitionKeys,
                    physicalColumns,
                    hiveStorageTimeZone,
                    typeManager);
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

    private static List<HiveColumnHandle> getPhysicalHiveColumnHandles(List<HiveColumnHandle> columns, boolean useOrcColumnNames, OrcReader reader)
    {
        List<HiveColumnHandle> physicalColumns = columns;
        if (useOrcColumnNames) {
            verifyOrcFileSupportsColumnNameLookups(reader.getColumnNames());
            ImmutableMap<String, Integer> physicalNameOrdinalMap = buildPhysicalNameOrdinalMap(reader);
            int nextMissingColumnIndex = physicalNameOrdinalMap.size();
            physicalColumns = new ArrayList<>();
            for (HiveColumnHandle column : columns) {
                Integer physicalOrdinal = physicalNameOrdinalMap.get(column.getName());
                if (physicalOrdinal != null) {
                    physicalColumns.add(new HiveColumnHandle(column.getClientId(), column.getName(), column.getHiveType(), column.getTypeSignature(), physicalOrdinal, column.isPartitionKey()));
                }
                else {
                    physicalColumns.add(new HiveColumnHandle(column.getClientId(), column.getName(), column.getHiveType(), column.getTypeSignature(), nextMissingColumnIndex++, column.isPartitionKey()));
                }
            }
        }
        return physicalColumns;
    }

    private static void verifyOrcFileSupportsColumnNameLookups(List<String> physicalColumnNames)
    {
        boolean columnNamesMissingFromFooter = true;
        Iterator<String> physicalColumnNameIterator = physicalColumnNames.iterator();
        while (columnNamesMissingFromFooter && physicalColumnNameIterator.hasNext()) {
            columnNamesMissingFromFooter = DEFAULT_HIVE_COLUMN_NAME_PATTERN.matcher(physicalColumnNameIterator.next()).matches();
        }
        if (columnNamesMissingFromFooter) {
            throw new PrestoException(HIVE_ORC_COLUMN_NAME_LOOKUPS_NOT_SUPPORTED,
                    "Referencing columns by name is not supported in Orc files that do not contain column names in the footer.");
        }
    }

    private static ImmutableMap<String, Integer> buildPhysicalNameOrdinalMap(OrcReader reader)
    {
        int ordinal = 0;
        ImmutableMap.Builder<String, Integer> physicalNameOrdinalMap = ImmutableMap.builder();
        List<String> physicalColumnNames = reader.getColumnNames();
        for (String physicalColumnName : physicalColumnNames) {
            physicalNameOrdinalMap.put(physicalColumnName, ordinal++);
        }
        return physicalNameOrdinalMap.build();
    }
}
