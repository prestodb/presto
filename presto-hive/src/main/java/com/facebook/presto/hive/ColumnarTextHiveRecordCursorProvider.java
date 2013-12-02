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

import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.mapred.RecordReader;

import java.util.List;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.getDeserializer;

public class ColumnarTextHiveRecordCursorProvider
        implements HiveRecordCursorProvider
{
    @Override
    public Optional<RecordCursor> createHiveRecordCursor(HiveSplit split, RecordReader<?, ?> recordReader, List<HiveColumnHandle> columns)
    {
        if (usesColumnarTextSerDe(split)) {
            return Optional.<RecordCursor>of(new ColumnarTextHiveRecordCursor<>(
                    columnarTextRecordReader(recordReader),
                    split.getLength(),
                    split.getSchema(),
                    split.getPartitionKeys(),
                    columns));
        }
        return Optional.absent();
    }

    private static boolean usesColumnarTextSerDe(HiveSplit split)
    {
        try {
            return getDeserializer(null, split.getSchema()) instanceof ColumnarSerDe;
        }
        catch (MetaException e) {
            throw Throwables.propagate(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, BytesRefArrayWritable> columnarTextRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, BytesRefArrayWritable>) recordReader;
    }
}
