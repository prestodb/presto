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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.HiveRecordCursor;
import com.facebook.presto.hive.HiveRecordCursorProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveSessionProperties.isParquetPredicatePushdownEnabled;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static java.util.Objects.requireNonNull;

public class ParquetRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private static final Set<String> PARQUET_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();

    private final boolean useParquetColumnNames;

    @Inject
    public ParquetRecordCursorProvider(HiveClientConfig hiveClientConfig)
    {
        this(requireNonNull(hiveClientConfig, "hiveClientConfig is null").isUseParquetColumnNames());
    }

    public ParquetRecordCursorProvider(boolean useParquetColumnNames)
    {
        this.useParquetColumnNames = useParquetColumnNames;
    }

    @Override
    public Optional<HiveRecordCursor> createHiveRecordCursor(
            String clientId,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        if (!PARQUET_SERDE_CLASS_NAMES.contains(getDeserializerClassName(schema))) {
            return Optional.empty();
        }

        return Optional.<HiveRecordCursor>of(new ParquetHiveRecordCursor(
                configuration,
                path,
                start,
                length,
                schema,
                partitionKeys,
                columns,
                useParquetColumnNames,
                hiveStorageTimeZone,
                typeManager,
                isParquetPredicatePushdownEnabled(session),
                effectivePredicate
        ));
    }
}
