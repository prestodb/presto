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
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.util.Types.checkType;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

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
        HiveSplit hiveSplit = checkType(split, HiveSplit.class, "split");

        String clientId = hiveSplit.getClientId();

        Path path = new Path(hiveSplit.getPath());
        long start = hiveSplit.getStart();
        long length = hiveSplit.getLength();

        Configuration configuration = hdfsEnvironment.getConfiguration(path);

        TupleDomain<HiveColumnHandle> effectivePredicate = hiveSplit.getEffectivePredicate();

        Properties schema = hiveSplit.getSchema();

        List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
        List<HiveColumnHandle> hiveColumns = ImmutableList.copyOf(transform(columns, HiveColumnHandle::toHiveColumnHandle));

        for (HivePageSourceFactory pageSourceFactory : pageSourceFactories) {
            Optional<? extends ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    schema,
                    hiveColumns,
                    partitionKeys,
                    effectivePredicate,
                    hiveStorageTimeZone
            );
            if (pageSource.isPresent()) {
                return pageSource.get();
            }
        }

        HiveRecordCursor recordCursor = getHiveRecordCursor(clientId, session, configuration, path, start, length, schema, effectivePredicate, partitionKeys, hiveColumns);
        if (recordCursor != null) {
            List<Type> columnTypes = ImmutableList.copyOf(transform(hiveColumns, input -> typeManager.getType(input.getTypeSignature())));
            return new RecordPageSource(columnTypes, recordCursor);
        }

        throw new RuntimeException("Could not find a file reader for split " + hiveSplit);
    }

    protected HiveRecordCursor getHiveRecordCursor(
            String clientId,
            ConnectorSession session,
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> hiveColumns)
    {
        for (HiveRecordCursorProvider provider : cursorProviders) {
            Optional<HiveRecordCursor> cursor = provider.createHiveRecordCursor(
                    clientId,
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    schema,
                    hiveColumns,
                    partitionKeys,
                    effectivePredicate,
                    hiveStorageTimeZone,
                    typeManager);
            if (cursor.isPresent()) {
                return cursor.get();
            }
        }
        return null;
    }
}
