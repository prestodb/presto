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
package com.facebook.presto.hive.parquet.write;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/*
  MapredParquetOutputFormat creates the Parquet schema from the column types,
  which is not always what we want. Because, in that case for decimal type
  the schema always specifies FIXED_LEN_BYTE_ARRAY as the backing type. But,
  we also want to test the cases were the backing type is INT32/INT64, which requires
  a custom Parquet schema.
*/
public class TestMapredParquetOutputFormat
        extends MapredParquetOutputFormat
{
    private final Optional<MessageType> schema;

    public TestMapredParquetOutputFormat(Optional<MessageType> schema, boolean singleLevelArray)
    {
        super(new ParquetOutputFormat<>(new TestDataWritableWriteSupport(singleLevelArray)));
        this.schema = requireNonNull(schema, "schema is null");
    }

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf,
            Path finalOutPath,
            Class<? extends Writable> valueClass,
            boolean isCompressed,
            Properties tableProperties,
            Progressable progress)
            throws IOException
    {
        if (schema.isPresent()) {
            DataWritableWriteSupport.setSchema(schema.get(), jobConf);
            return getParquerRecordWriterWrapper(realOutputFormat, jobConf, finalOutPath.toString(), progress, tableProperties);
        }
        return super.getHiveRecordWriter(jobConf, finalOutPath, valueClass, isCompressed, tableProperties, progress);
    }
}
