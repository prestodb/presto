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
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HiveUtil.isDeserializerClass;

public class ColumnarTextHiveRecordCursorProvider
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
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        if (!isDeserializerClass(schema, ColumnarSerDe.class)) {
            return Optional.empty();
        }

        RecordReader<?, ?> recordReader = HiveUtil.createRecordReader(configuration, path, start, length, schema, columns);

        return Optional.<HiveRecordCursor>of(new ColumnarTextHiveRecordCursor<>(
                columnarTextRecordReader(recordReader),
                length,
                schema,
                partitionKeys,
                columns,
                hiveStorageTimeZone,
                typeManager));
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, BytesRefArrayWritable> columnarTextRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, BytesRefArrayWritable>) recordReader;
    }
}
