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

import com.facebook.presto.hive.metastore.StorageFormat;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.hcatalog.data.JsonSerDe;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public enum HiveStorageFormat
{
    ORC(
            OrcSerde.class.getName(),
            OrcInputFormat.class.getName(),
            OrcOutputFormat.class.getName(),
            new DataSize(256, Unit.MEGABYTE)),
    DWRF(
            com.facebook.hive.orc.OrcSerde.class.getName(),
            com.facebook.hive.orc.OrcInputFormat.class.getName(),
            com.facebook.hive.orc.OrcOutputFormat.class.getName(),
            new DataSize(256, Unit.MEGABYTE)),
    PARQUET(
            ParquetHiveSerDe.class.getName(),
            MapredParquetInputFormat.class.getName(),
            MapredParquetOutputFormat.class.getName(),
            new DataSize(128, Unit.MEGABYTE)),
    AVRO(
            AvroSerDe.class.getName(),
            AvroContainerInputFormat.class.getName(),
            AvroContainerOutputFormat.class.getName(),
            new DataSize(64, Unit.MEGABYTE)),
    RCBINARY(
            LazyBinaryColumnarSerDe.class.getName(),
            RCFileInputFormat.class.getName(),
            RCFileOutputFormat.class.getName(),
            new DataSize(8, Unit.MEGABYTE)),
    RCTEXT(
            ColumnarSerDe.class.getName(),
            RCFileInputFormat.class.getName(),
            RCFileOutputFormat.class.getName(),
            new DataSize(8, Unit.MEGABYTE)),
    SEQUENCEFILE(
            LazySimpleSerDe.class.getName(),
            SequenceFileInputFormat.class.getName(),
            HiveSequenceFileOutputFormat.class.getName(),
            new DataSize(8, Unit.MEGABYTE)),
    JSON(
            JsonSerDe.class.getName(),
            TextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName(),
            new DataSize(8, Unit.MEGABYTE)),
    TEXTFILE(
            LazySimpleSerDe.class.getName(),
            TextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName(),
            new DataSize(8, Unit.MEGABYTE)),
    CSV(
            OpenCSVSerde.class.getName(),
            TextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName(),
            new DataSize(8, Unit.MEGABYTE)),
    PAGEFILE(
            "",  // SerDe is not applicable for PAGEFILE
            "PageInputFormat",
            "PageOutputFormat",
            new DataSize(0, Unit.MEGABYTE));  // memory usage is not applicable for PAGEFILE

    private final String serde;
    private final String inputFormat;
    private final String outputFormat;
    private final DataSize estimatedWriterSystemMemoryUsage;

    HiveStorageFormat(String serde, String inputFormat, String outputFormat, DataSize estimatedWriterSystemMemoryUsage)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
        this.estimatedWriterSystemMemoryUsage = requireNonNull(estimatedWriterSystemMemoryUsage, "estimatedWriterSystemMemoryUsage is null");
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

    public DataSize getEstimatedWriterSystemMemoryUsage()
    {
        return estimatedWriterSystemMemoryUsage;
    }

    private static final Map<SerdeAndInputFormat, HiveStorageFormat> HIVE_STORAGE_FORMAT_FROM_STORAGE_FORMAT = Arrays.stream(HiveStorageFormat.values())
            .collect(toImmutableMap(format -> new SerdeAndInputFormat(format.getSerDe(), format.getInputFormat()), identity()));

    private static final class SerdeAndInputFormat
    {
        private final String serDe;
        private final String inputFormat;

        public SerdeAndInputFormat(String serDe, String inputFormat)
        {
            this.serDe = serDe;
            this.inputFormat = inputFormat;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SerdeAndInputFormat that = (SerdeAndInputFormat) o;
            return serDe.equals(that.serDe) && inputFormat.equals(that.inputFormat);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(serDe, inputFormat);
        }
    }

    public static Optional<HiveStorageFormat> getHiveStorageFormat(StorageFormat storageFormat)
    {
        return Optional.ofNullable(HIVE_STORAGE_FORMAT_FROM_STORAGE_FORMAT.get(new SerdeAndInputFormat(storageFormat.getSerDeNullable(), storageFormat.getInputFormatNullable())));
    }
}
