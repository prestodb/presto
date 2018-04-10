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
package com.facebook.presto.hive;

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping.extractRegularColumnHandles;
import static com.facebook.presto.hive.HiveUtil.getPrefilledColumnValue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final DateTimeZone hiveStorageTimeZone;
    private final HdfsEnvironment hdfsEnvironment;
    private final Set<HiveRecordCursorProvider> cursorProviders;
    private final TypeManager typeManager;

    private final Set<HivePageSourceFactory> pageSourceFactories;

    @Inject
    public HivePageSourceProvider(
            HiveClientConfig hiveClientConfig,
            HdfsEnvironment hdfsEnvironment,
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HivePageSourceFactory> pageSourceFactories,
            TypeManager typeManager)
    {
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        this.hiveStorageTimeZone = hiveClientConfig.getDateTimeZone();
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.cursorProviders = ImmutableSet.copyOf(requireNonNull(cursorProviders, "cursorProviders is null"));
        this.pageSourceFactories = ImmutableSet.copyOf(requireNonNull(pageSourceFactories, "pageSourceFactories is null"));
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());

        HiveSplit hiveSplit = (HiveSplit) split;
        Path path = new Path(hiveSplit.getPath());

        Optional<ConnectorPageSource> pageSource = createHivePageSource(
                cursorProviders,
                pageSourceFactories,
                hdfsEnvironment.getConfiguration(new HdfsContext(session, hiveSplit.getDatabase(), hiveSplit.getTable()), path),
                session,
                path,
                hiveSplit.getBucketNumber(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getFileSize(),
                hiveSplit.getSchema(),
                hiveSplit.getEffectivePredicate(),
                hiveColumns,
                hiveSplit.getPartitionKeys(),
                hiveStorageTimeZone,
                typeManager,
                hiveSplit.getColumnCoercions());
        if (pageSource.isPresent()) {
            return pageSource.get();
        }
        throw new RuntimeException("Could not find a file reader for split " + hiveSplit);
    }

    public static Optional<ConnectorPageSource> createHivePageSource(
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HivePageSourceFactory> pageSourceFactories,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            OptionalInt bucketNumber,
            long start,
            long length,
            long fileSize,
            Properties schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> hiveColumns,
            List<HivePartitionKey> partitionKeys,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            Map<Integer, HiveType> columnCoercions)
    {
        List<ColumnMapping> columnMappings = ColumnMapping.buildColumnMappings(partitionKeys, hiveColumns, columnCoercions, path, bucketNumber);
        List<ColumnMapping> regularColumnMappings = ColumnMapping.extractRegularColumnMappings(columnMappings);

        for (HivePageSourceFactory pageSourceFactory : pageSourceFactories) {
            Optional<? extends ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    fileSize,
                    schema,
                    extractRegularColumnHandles(regularColumnMappings, true),
                    effectivePredicate,
                    hiveStorageTimeZone);
            if (pageSource.isPresent()) {
                return Optional.of(
                        new HivePageSource(
                                columnMappings,
                                hiveStorageTimeZone,
                                typeManager,
                                pageSource.get()));
            }
        }

        for (HiveRecordCursorProvider provider : cursorProviders) {
            // GenericHiveRecordCursor will automatically do the coercion without HiveCoercionRecordCursor
            boolean doCoercion = !(provider instanceof GenericHiveRecordCursorProvider);

            Optional<RecordCursor> cursor = provider.createRecordCursor(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    fileSize,
                    schema,
                    extractRegularColumnHandles(regularColumnMappings, doCoercion),
                    effectivePredicate,
                    hiveStorageTimeZone,
                    typeManager);

            if (cursor.isPresent()) {
                RecordCursor delegate = cursor.get();

                // Need to wrap RcText and RcBinary into a wrapper, which will do the coercion for mismatch columns
                if (doCoercion) {
                    delegate = new HiveCoercionRecordCursor(regularColumnMappings, typeManager, delegate);
                }

                HiveRecordCursor hiveRecordCursor = new HiveRecordCursor(
                        columnMappings,
                        hiveStorageTimeZone,
                        typeManager,
                        delegate);
                List<Type> columnTypes = hiveColumns.stream()
                        .map(input -> typeManager.getType(input.getTypeSignature()))
                        .collect(toList());

                return Optional.of(new RecordPageSource(columnTypes, hiveRecordCursor));
            }
        }

        return Optional.empty();
    }

    public static class ColumnMapping
    {
        private final ColumnMappingKind kind;
        private final HiveColumnHandle hiveColumnHandle;
        private final Optional<String> prefilledValue;
        /**
         * ordinal of this column in the underlying page source or record cursor
         */
        private final OptionalInt index;
        private final Optional<HiveType> coercionFrom;

        public static ColumnMapping regular(HiveColumnHandle hiveColumnHandle, int index, Optional<HiveType> coerceFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == REGULAR);
            return new ColumnMapping(ColumnMappingKind.REGULAR, hiveColumnHandle, Optional.empty(), OptionalInt.of(index), coerceFrom);
        }

        public static ColumnMapping prefilled(HiveColumnHandle hiveColumnHandle, String prefilledValue, Optional<HiveType> coerceFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == PARTITION_KEY || hiveColumnHandle.getColumnType() == SYNTHESIZED);
            return new ColumnMapping(ColumnMappingKind.PREFILLED, hiveColumnHandle, Optional.of(prefilledValue), OptionalInt.empty(), coerceFrom);
        }

        private ColumnMapping(ColumnMappingKind kind, HiveColumnHandle hiveColumnHandle, Optional<String> prefilledValue, OptionalInt index, Optional<HiveType> coerceFrom)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.hiveColumnHandle = requireNonNull(hiveColumnHandle, "hiveColumnHandle is null");
            this.prefilledValue = requireNonNull(prefilledValue, "prefilledValue is null");
            this.index = requireNonNull(index, "index is null");
            this.coercionFrom = requireNonNull(coerceFrom, "coerceFrom is null");
        }

        public ColumnMappingKind getKind()
        {
            return kind;
        }

        public String getPrefilledValue()
        {
            checkState(kind == ColumnMappingKind.PREFILLED);
            return prefilledValue.get();
        }

        public HiveColumnHandle getHiveColumnHandle()
        {
            return hiveColumnHandle;
        }

        public int getIndex()
        {
            checkState(kind == ColumnMappingKind.REGULAR);
            return index.getAsInt();
        }

        public Optional<HiveType> getCoercionFrom()
        {
            return coercionFrom;
        }

        public static List<ColumnMapping> buildColumnMappings(
                List<HivePartitionKey> partitionKeys,
                List<HiveColumnHandle> columns,
                Map<Integer, HiveType> columnCoercions,
                Path path,
                OptionalInt bucketNumber)
        {
            Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::getName);
            int regularIndex = 0;
            ImmutableList.Builder<ColumnMapping> columnMappings = ImmutableList.builder();
            for (int i = 0; i < columns.size(); i++) {
                HiveColumnHandle column = columns.get(i);
                Optional<HiveType> coercionFrom = Optional.ofNullable(columnCoercions.get(column.getHiveColumnIndex()));
                if (column.getColumnType() == REGULAR) {
                    columnMappings.add(regular(column, regularIndex, coercionFrom));
                    regularIndex++;
                }
                else {
                    columnMappings.add(prefilled(
                            column,
                            getPrefilledColumnValue(column, partitionKeysByName.get(column.getName()), path, bucketNumber),
                            coercionFrom));
                }
            }
            return columnMappings.build();
        }

        public static List<ColumnMapping> extractRegularColumnMappings(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(columnMapping -> columnMapping.getKind() == ColumnMappingKind.REGULAR)
                    .collect(toList());
        }

        public static List<HiveColumnHandle> extractRegularColumnHandles(List<ColumnMapping> regularColumnMappings, boolean doCoercion)
        {
            return regularColumnMappings.stream()
                    .map(columnMapping -> {
                        HiveColumnHandle columnHandle = columnMapping.getHiveColumnHandle();
                        if (!doCoercion || !columnMapping.getCoercionFrom().isPresent()) {
                            return columnHandle;
                        }
                        return new HiveColumnHandle(
                                columnHandle.getName(),
                                columnMapping.getCoercionFrom().get(),
                                columnMapping.getCoercionFrom().get().getTypeSignature(),
                                columnHandle.getHiveColumnIndex(),
                                columnHandle.getColumnType(),
                                Optional.empty());
                    })
                    .collect(toList());
        }
    }

    public enum ColumnMappingKind
    {
        REGULAR,
        PREFILLED,
    }
}
