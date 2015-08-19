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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
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
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.Collection;

import static com.facebook.presto.hive.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Class.forName;
import static java.util.Locale.ENGLISH;

public class HiveStorageFormat
{
    private static final Logger LOG = Logger.get(HiveMetadata.class);

    public static final HiveStorageFormat ORC = new HiveStorageFormat(OrcSerde.class.getName(),
            OrcInputFormat.class.getName(),
            OrcOutputFormat.class.getName());
    public static final HiveStorageFormat DWRF = new HiveStorageFormat(com.facebook.hive.orc.OrcSerde.class.getName(),
            com.facebook.hive.orc.OrcInputFormat.class.getName(),
            com.facebook.hive.orc.OrcOutputFormat.class.getName());
    public static final HiveStorageFormat PARQUET = new HiveStorageFormat(ParquetHiveSerDe.class.getName(),
            MapredParquetInputFormat.class.getName(),
            MapredParquetOutputFormat.class.getName());
    public static final HiveStorageFormat RCBINARY = new HiveStorageFormat(LazyBinaryColumnarSerDe.class.getName(),
            RCFileInputFormat.class.getName(),
            RCFileOutputFormat.class.getName());
    public static final HiveStorageFormat RCTEXT = new HiveStorageFormat(ColumnarSerDe.class.getName(),
            RCFileInputFormat.class.getName(),
            RCFileOutputFormat.class.getName());
    public static final HiveStorageFormat SEQUENCEFILE = new HiveStorageFormat(LazySimpleSerDe.class.getName(),
            SequenceFileInputFormat.class.getName(),
            HiveSequenceFileOutputFormat.class.getName());
    public static final HiveStorageFormat TEXTFILE = new HiveStorageFormat(LazySimpleSerDe.class.getName(),
            TextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName());

    private final String serDe;
    private final String inputFormat;
    private final String outputFormat;

    @JsonCreator
    public HiveStorageFormat(
            @JsonProperty("serDe") String serDe,
            @JsonProperty("inputFormat") String inputFormat,
            @JsonProperty("outputFormat") String outputFormat)
    {
        this.serDe = checkNotNull(serDe, "serDe is null");
        this.inputFormat = checkNotNull(inputFormat, "inputFormat is null");
        this.outputFormat = checkNotNull(outputFormat, "outputFormat is null");
    }

    @JsonProperty
    public String getSerDe()
    {
        return serDe;
    }

    @JsonProperty
    public String getInputFormat()
    {
        return inputFormat;
    }

    @JsonProperty
    public String getOutputFormat()
    {
        return outputFormat;
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

        HiveStorageFormat that = (HiveStorageFormat) o;

        return Objects.equal(this.serDe, that.serDe) &&
                Objects.equal(this.inputFormat, that.inputFormat) &&
                Objects.equal(this.outputFormat, that.outputFormat);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(serDe, inputFormat, outputFormat);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("serDe", serDe)
                .add("inputFormat", inputFormat)
                .add("outputFormat", outputFormat)
                .toString();
    }

    public static HiveStorageFormat valueOf(String format)
    {
        HiveStorageFormat predefinedFormat = getByName(format);
        HiveStorageFormat customFormat = getBySerdeClass(format);
        HiveStorageFormat hiveStorageFormat = predefinedFormat != null ? predefinedFormat : customFormat;
        checkArgument(hiveStorageFormat != null, "Format %s not found", format);
        return hiveStorageFormat;
    }

    public static Collection<HiveStorageFormat> predefinedFormats()
    {
        return ImmutableList.of(ORC, DWRF, PARQUET, RCBINARY, RCTEXT, SEQUENCEFILE, TEXTFILE);
    }

    private static HiveStorageFormat getByName(String format)
    {
        switch (format.toUpperCase(ENGLISH)) {
            case "ORC":
                return ORC;
            case "DWRF":
                return DWRF;
            case "PARQUET":
                return PARQUET;
            case "RCBINARY":
                return RCBINARY;
            case "RCTEXT":
                return RCTEXT;
            case "SEQUENCEFILE":
                return SEQUENCEFILE;
            case "TEXTFILE":
                return TEXTFILE;
            default:
                return null;
        }
    }

    private static HiveStorageFormat getBySerdeClass(String className)
    {
        try {
            Class<?> clazz = forName(className);
            HiveStorageHandler storageHandler = checkType(
                    clazz.newInstance(), HiveStorageHandler.class, "Handler class should implement " + HiveStorageHandler.class.getName()
            );
            return new HiveStorageFormat(
                    storageHandler.getSerDeClass().getName(),
                    storageHandler.getInputFormatClass().getName(),
                    storageHandler.getOutputFormatClass().getName()
            );
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.debug("Exception happens during loading serde class", e);
            return null;
        }
    }
}
