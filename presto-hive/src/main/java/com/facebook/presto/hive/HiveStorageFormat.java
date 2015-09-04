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

import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import static java.util.Objects.requireNonNull;

public enum HiveStorageFormat
{
    ORC(OrcSerde.class.getName(),
            OrcInputFormat.class.getName(),
            OrcOutputFormat.class.getName()),
    DWRF(com.facebook.hive.orc.OrcSerde.class.getName(),
            com.facebook.hive.orc.OrcInputFormat.class.getName(),
            com.facebook.hive.orc.OrcOutputFormat.class.getName()),
    PARQUET(ParquetHiveSerDe.class.getName(),
            MapredParquetInputFormat.class.getName(),
            MapredParquetOutputFormat.class.getName()),
    RCBINARY(LazyBinaryColumnarSerDe.class.getName(),
            RCFileInputFormat.class.getName(),
            RCFileOutputFormat.class.getName()),
    RCTEXT(ColumnarSerDe.class.getName(),
            RCFileInputFormat.class.getName(),
            RCFileOutputFormat.class.getName()),
    SEQUENCEFILE(LazySimpleSerDe.class.getName(),
            SequenceFileInputFormat.class.getName(),
            HiveSequenceFileOutputFormat.class.getName()),
    TEXTFILE(LazySimpleSerDe.class.getName(),
            TextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName());

    private final String serde;
    private final String inputFormat;
    private final String outputFormat;

    HiveStorageFormat(String serde, String inputFormat, String outputFormat)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
    }

    public String getSerDe()
    {
        return serde;
    }

    public String getInputFormat()
    {
        return inputFormat;
    }

    public String getOutputFormat()
    {
        return outputFormat;
    }
}
