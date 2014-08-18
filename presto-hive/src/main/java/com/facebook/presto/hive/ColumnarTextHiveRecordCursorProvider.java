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

import com.google.common.base.Optional;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import java.util.List;

import static com.facebook.presto.hive.HiveUtil.getDeserializer;

public class ColumnarTextHiveRecordCursorProvider
        implements HiveRecordCursorProvider
{
    @Override
    public Optional<HiveRecordCursor> createHiveRecordCursor(HiveSplit split, RecordReader<?, ?> recordReader, List<HiveColumnHandle> columns, DateTimeZone hiveStorageTimeZone)
    {
        if (usesColumnarTextSerDe(split)) {
            return Optional.<HiveRecordCursor>of(new ColumnarTextHiveRecordCursor<>(
                    columnarTextRecordReader(recordReader),
                    split.getLength(),
                    split.getSchema(),
                    split.getPartitionKeys(),
                    columns,
                    hiveStorageTimeZone,
                    DateTimeZone.forID(split.getSession().getTimeZoneKey().getId())));
        }
        return Optional.absent();
    }

    private static boolean usesColumnarTextSerDe(HiveSplit split)
    {
        return getDeserializer(split.getSchema()) instanceof ColumnarSerDe;
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, BytesRefArrayWritable> columnarTextRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, BytesRefArrayWritable>) recordReader;
    }
}
