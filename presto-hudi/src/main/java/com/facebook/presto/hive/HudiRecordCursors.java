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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hudi.HudiColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.util.stream.Collectors.toList;

public final class HudiRecordCursors
{
    private HudiRecordCursors() {}

    public static RecordCursor createRecordCursor(
            Configuration configuration,
            Path path,
            RecordReader<?, ? extends Writable> recordReader,
            long totalBytes,
            Properties hiveSchema,
            List<HudiColumnHandle> hiveColumnHandles,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        return new GenericHiveRecordCursor<>(
                configuration,
                path,
                recordReader,
                totalBytes,
                hiveSchema,
                toHiveColumnHandles(hiveColumnHandles),
                hiveStorageTimeZone,
                typeManager);
    }

    private static List<HiveColumnHandle> toHiveColumnHandles(List<HudiColumnHandle> columns)
    {
        return columns.stream()
                .map(column -> new HiveColumnHandle(
                        column.getName(),
                        column.getHiveType(),
                        column.getHiveType().getTypeSignature(),
                        column.getId(),
                        HiveColumnHandle.ColumnType.REGULAR,
                        column.getComment(),
                        Optional.empty()))
                .collect(toList());
    }
}
