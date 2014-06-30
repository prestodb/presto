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

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.RecordProjectOperator;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveColumnHandle.hiveColumnHandle;
import static com.facebook.presto.hive.HiveColumnHandle.nativeTypeGetter;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public class HiveDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final Set<HiveRecordCursorProvider> cursorProviders;

    @Inject
    public HiveDataStreamProvider(HdfsEnvironment hdfsEnvironment, Set<HiveRecordCursorProvider> cursorProviders)
    {
        this.hdfsEnvironment = checkNotNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.cursorProviders = ImmutableSet.copyOf(checkNotNull(cursorProviders, "cursorProviders is null"));
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, ConnectorSplit split, List<ConnectorColumnHandle> columns)
    {
        HiveSplit hiveSplit = checkType(split, HiveSplit.class, "split");

        String clientId = hiveSplit.getClientId();
        ConnectorSession session = hiveSplit.getSession();

        Path path = new Path(hiveSplit.getPath());
        long start = hiveSplit.getStart();
        long length = hiveSplit.getLength();

        Configuration configuration = hdfsEnvironment.getConfiguration(path);

        TupleDomain<HiveColumnHandle> tupleDomain = hiveSplit.getTupleDomain();

        Properties schema = hiveSplit.getSchema();

        List<HivePartitionKey> partitionKeys = hiveSplit.getPartitionKeys();
        List<HiveColumnHandle> hiveColumns = ImmutableList.copyOf(transform(columns, hiveColumnHandle()));

        DateTimeZone timeZone = DateTimeZone.forID(session.getTimeZoneKey().getId());

        HiveRecordCursor recordCursor = getHiveRecordCursor(clientId, session, configuration, path, start, length, schema, tupleDomain, partitionKeys, hiveColumns, timeZone);
        if (recordCursor == null) {
            throw new RuntimeException("Configured cursor providers did not provide a cursor");
        }

        List<Type> columnTypes = ImmutableList.copyOf(transform(hiveColumns, nativeTypeGetter()));
        RecordProjectOperator recordProjectOperator = new RecordProjectOperator(operatorContext, columnTypes, recordCursor);
        return recordProjectOperator;
    }

    protected HiveRecordCursor getHiveRecordCursor(
            String clientId,
            ConnectorSession session,
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties schema,
            TupleDomain<HiveColumnHandle> tupleDomain,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> hiveColumns,
            DateTimeZone timeZone)
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
                    tupleDomain,
                    timeZone);
            if (cursor.isPresent()) {
                return cursor.get();
            }
        }
        return null;
    }
}
