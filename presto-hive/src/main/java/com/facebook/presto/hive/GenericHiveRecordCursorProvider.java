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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import java.util.List;

public class GenericHiveRecordCursorProvider
        implements HiveRecordCursorProvider
{
    @Override
    public Optional<RecordCursor> createHiveRecordCursor(HiveSplit split, RecordReader<?, ?> recordReader, List<HiveColumnHandle> columns)
    {
        return Optional.<RecordCursor>of(new GenericHiveRecordCursor<>(
                genericRecordReader(recordReader),
                split.getLength(),
                split.getSchema(),
                split.getPartitionKeys(),
                columns));
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, ? extends Writable> genericRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, ? extends Writable>) recordReader;
    }
}
