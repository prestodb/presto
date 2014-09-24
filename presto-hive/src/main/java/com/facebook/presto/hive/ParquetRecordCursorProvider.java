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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveUtil.getDeserializer;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;

public class ParquetRecordCursorProvider
        implements HiveRecordCursorProvider
{
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
            TupleDomain<HiveColumnHandle> tupleDomain,
            DateTimeZone hiveStorageTimeZone)
    {
        @SuppressWarnings("deprecation")
        Deserializer deserializer = getDeserializer(schema);
        if (!(deserializer instanceof ParquetHiveSerDe)) {
            return Optional.absent();
        }

        // are all columns supported by Parquet code
        List<HiveColumnHandle> unsupportedColumns = ImmutableList.copyOf(filter(columns, not(isParquetSupportedType())));
        if (!unsupportedColumns.isEmpty()) {
            throw new IllegalArgumentException("Can not read Parquet column: " + unsupportedColumns);
        }

        return Optional.<HiveRecordCursor>of(new ParquetHiveRecordCursor(
                configuration,
                path,
                start,
                length,
                schema,
                partitionKeys,
                columns,
                DateTimeZone.forID(session.getTimeZoneKey().getId())));
    }

    private static Predicate<HiveColumnHandle> isParquetSupportedType()
    {
        return new Predicate<HiveColumnHandle>()
        {
            @Override
            public boolean apply(HiveColumnHandle columnHandle)
            {
                HiveType hiveType = columnHandle.getHiveType();
                switch (hiveType) {
                    case BOOLEAN:
                    case BYTE:
                    case SHORT:
                    case STRING:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case STRUCT:
                    case LIST:
                    case MAP:
                        return true;
                    case TIMESTAMP:
                        // not supported in Parquet
                    case DATE:
                        // not supported in Parquet
                    case BINARY:
                        // not supported in Parquet
                    default:
                        return false;
                }
            }
        };
    }
}
