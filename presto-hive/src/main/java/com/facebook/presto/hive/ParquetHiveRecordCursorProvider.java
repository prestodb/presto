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
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;

import parquet.hive.serde.ParquetHiveSerDe;

import io.airlift.log.Logger;
import java.util.List;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.getDeserializer;

public class ParquetHiveRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private static final Logger log = Logger.get(ParquetHiveRecordCursorProvider.class.getSimpleName());
    @Override
    public Optional<RecordCursor> createHiveRecordCursor(HiveSplit split, RecordReader<?, ?> recordReader, List<HiveColumnHandle> columns)
    {
        try {
            Deserializer deserializer = getDeserializer(null, split.getSchema());
            if (deserializer instanceof ParquetHiveSerDe) {
                return Optional.<RecordCursor>of(
                    new ParquetHiveRecordCursor<>(
                        parquetRecordReader(recordReader),
                        split.getLength(),
                        split.getSchema(),
                        split.getPartitionKeys(),
                        columns));
            }
            else {
                return Optional.absent();
            }
        }
        catch (MetaException e) {
            throw Throwables.propagate(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static RecordReader<?, ? extends Writable> parquetRecordReader(RecordReader<?, ?> recordReader)
    {
        return (RecordReader<?, ? extends Writable>) recordReader;
    }
}
