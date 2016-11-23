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

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping.extractRegularColumnHandles;
import static com.facebook.presto.hive.HiveUtil.getPrefilledColumnValue;
import static com.facebook.presto.hive.util.Types.checkType;
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
                .map(HiveColumnHandle::toHiveColumnHandle)
                .collect(toList());

        HiveSplit hiveSplit = checkType(split, HiveSplit.class, "split");
        Path path = new Path(hiveSplit.getPath());

        Optional<ConnectorPageSource> pageSource = createHivePageSource(
                cursorProviders,
                pageSourceFactories,
                hiveSplit.getClientId(),
                hdfsEnvironment.getConfiguration(path),
                session,
                path,
                hiveSplit.getBucketNumber(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
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
            String clientId,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            OptionalInt bucketNumber,
            long start,
            long length,
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
                    schema,
                    extractRegularColumnHandles(regularColumnMappings, true),
                    effectivePredicate,
                    hiveStorageTimeZone
            );
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
                    clientId,
                    configuration,
                    session,
                    path,
                    start,
                    length,
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
        private final HiveColumnHandle hiveColumnHandle;
        private final String prefilledValue;
        private final int index;
        private final Optional<HiveType> coercionFrom;

        private ColumnMapping(HiveColumnHandle hiveColumnHandle, String prefilledValue, int index, Optional<HiveType> coercionFrom)
        {
            requireNonNull(hiveColumnHandle, "hiveColumnHandle is null");
            if (isPrefilled(hiveColumnHandle)) {
                requireNonNull(prefilledValue, "prefilledValue is null when it is a prefilled column");
                checkArgument(index == -1, "index should be -1");
            }
            else {
                checkArgument(index >= 0, "index should be greater than or equal to 0");
            }

            this.hiveColumnHandle = hiveColumnHandle;
            this.prefilledValue = prefilledValue;
            this.index = index;
            this.coercionFrom = requireNonNull(coercionFrom, "coercionFrom is null while coercion is needed");
        }

        public boolean isPrefilled()
        {
            return isPrefilled(hiveColumnHandle);
        }

        public String getPrefilledValue()
        {
            checkState(isPrefilled(), "This is column is not prefilled");
            return prefilledValue;
        }

        public HiveColumnHandle getHiveColumnHandle()
        {
            return hiveColumnHandle;
        }

        public int getIndex()
        {
            return index;
        }

        public Optional<HiveType> getCoercionFrom()
        {
            return coercionFrom;
        }

        private static boolean isPrefilled(HiveColumnHandle hiveColumnHandle)
        {
            return hiveColumnHandle.getColumnType() != REGULAR;
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
                int currentIndex;
                String prefilledValue = null;
                if (column.getColumnType() == REGULAR) {
                    currentIndex = regularIndex;
                    regularIndex++;
                }
                else {
                    currentIndex = -1;

                    // prepare the prefilled value
                    HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());
                    prefilledValue = getPrefilledColumnValue(column, partitionKey, path, bucketNumber);
                }

                Optional<HiveType> coercionFrom = Optional.ofNullable(columnCoercions.get(column.getHiveColumnIndex()));

                columnMappings.add(new ColumnMapping(column, prefilledValue, currentIndex, coercionFrom));
            }
            return columnMappings.build();
        }

        public static List<ColumnMapping> extractRegularColumnMappings(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(columnMapping -> !columnMapping.isPrefilled())
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
                        return new HiveColumnHandle(columnHandle.getClientId(),
                                columnHandle.getName(),
                                columnMapping.getCoercionFrom().get(),
                                columnMapping.getCoercionFrom().get().getTypeSignature(),
                                columnHandle.getHiveColumnIndex(),
                                columnHandle.getColumnType());
                    })
                    .collect(toList());
        }
    }
}
